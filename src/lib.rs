#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

mod error;
mod node;

pub use error::{Error, Result};
pub use node::{PagePtr, Leaf, BTNode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    mem,
    path::{Path, PathBuf},
};


const PAGE_SIZE: u64 = 4096;
const MAGIC_HEADER: &str = "%bptree%";
static mut OVERRIDE_MAX_KEY_COUNT: u64 = 0;


// Computing n (the number of search keys in a node):
//
//        Let:  SIZE_K = length of a search key (in bytes)
//              SIZE_V = length of a pointer    (in bytes)
//
//
//       In 1 node, there are maximum:
//
//               n search keys = n     × SIZE_K bytes
//             n+1 pointers    = (n+1) × SIZE_V bytes
//
//      Therefore:
//
//         We must find the largest value of n such that
//
//               n × SIZE_K + (n+1) × SIZE_V  ≤  PAGE_SIZE
//
// BUT:
//      overhead (with serde) of a BTNode<K, V> is:
//          u8 -> node type (internal, leaf)                1 byte
//          2 x u64 -> 2 x size of vector (keys, entries)   16 bytes
//      overhead = 17 bytes
//
// Therefore:
//
//    We must find the largest value of n such that
//
//          n*SIZE_K + (n+1)*SIZE_V  ≤  PAGE_SIZE - 17
//
//          n*(SIZE_K + SIZE_V) + SIZE_V <= PAGE_SIZE - 17
//
//          n <= (PAGE_SIZE - SIZE_V - 17) / (SIZE_K + SIZE_V)

fn max_key_count(size_key: u64, size_value: u64) -> u64 {
    if unsafe { OVERRIDE_MAX_KEY_COUNT > 0 } {
        unsafe { OVERRIDE_MAX_KEY_COUNT }
    } else {
        (PAGE_SIZE - size_value - 17) / (size_key + size_value)
    }
}


fn split_at(max_key_count: u64) -> usize {
    // let max_key_count = max_key_count(size_key, size_value);
    ((max_key_count / 2) + (max_key_count % 2)) as usize
}


fn meta_file_path(dirname: &Path) -> PathBuf {
    let mut path = PathBuf::from(dirname);
    path.push("meta");
    path
}


fn db_path(directory: &Path) -> PathBuf {
    let mut path = PathBuf::from(directory);
    path.push("db");
    path
}


#[derive(Debug, Serialize, Deserialize)]
pub struct BTree<K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{
    magic_header: String,
    #[serde(skip)]
    pub directory: PathBuf,
    node_count: u64,
    entry_count: u64,
    root_page_nr: PagePtr,
    emtpy_pages: Vec<PagePtr>,
    key_size: u64,
    value_size: u64,
    key_type: PhantomData<K>,
    value_type: PhantomData<V>,
    max_key_count: u64,
    split_at: usize,
    #[serde(skip)]
    fh: Option<File>,
}


impl<K, V> BTree<K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{
    pub fn open<P: AsRef<Path>>(directory: P, override_max_key_count: Option<u64>) -> Result<Self> {
        fs::create_dir_all(&directory)?;
        let meta_path = meta_file_path(directory.as_ref());
        match &meta_path.exists() {
            true => Self::load_meta(&meta_path, directory.as_ref()),
            false => Ok(Self::new(directory.as_ref(), override_max_key_count)),
        }
    }

    pub fn len(&self) -> usize {
        self.entry_count as usize
    }

    pub fn keys(&mut self) -> BTreeIterator<K, V> {
        BTreeIterator::new(self).unwrap().into_iter()
    }

    pub fn values(&mut self) -> BTreeValueIterator<K, V> {
        BTreeValueIterator::new(self).unwrap().into_iter()
    }

    pub fn get(&mut self, key: K) -> Result<Option<V>> {
        if self.len() == 0 {
            return Ok(None);
        }
        let mut page_nr = self.root_page_nr;
        loop {
            match self.load_node(page_nr)? {
                BTNode::Leaf(node) => return Ok(node.get(&key)),
                BTNode::Internal(node) => page_nr = node.get(&key),
            }
        }
    }

    pub fn set(&mut self, key: K, value: V) -> Result<Option<V>> {
        if self.len() == 0 {
            self.create_first_root(key, value)?;
            return Ok(None);
        }
        let root = self.load_node(self.root_page_nr)?;
        let (split, original_value) = root.set(self, key, value)?;
        if let Some((key, page_nr)) = split {
            self.create_new_root(key, page_nr)?;
        }
        if original_value.is_none() {
            self.entry_count += 1;
        }
        Ok(original_value)
    }

    pub fn remove(&mut self, key: K) -> Result<Option<V>> {
        match self.len() > 0 {
            true => {
                let root = self.load_node(self.root_page_nr)?;
                let original_value = root.remove(self, key)?;
                if original_value.is_some() {
                    self.entry_count -= 1;
                }
                Ok(original_value)
            },
            false => Ok(None),
        }
    }

    fn create_first_root(&mut self, key: K, value: V) -> Result<()> {
        // FIXME: remove this line by eliminating function "root"
        self.node_count = 0;
        let root = self.root()?;
        root.set(self, key, value)?;
        self.entry_count += 1;
        Ok(())
    }

    fn create_new_root(&mut self, key: K, new_page_nr: u64) -> Result<()> {
        let old_root_page_nr = self.root_page_nr;
        self.root_page_nr = self.next_page_nr();
        let new_root = BTNode::new_internal(self.root_page_nr, &[key], &[old_root_page_nr, new_page_nr]);
        self.store_node(&new_root)?;
        Ok(())
    }

    fn next_page_nr(&mut self) -> u64 {
        let page_nr = self.node_count;
        self.node_count += 1;
        page_nr
    }

    fn on_page_deleted(&mut self, page_nr: PagePtr) {
        self.emtpy_pages.push(page_nr);
        self.node_count -= 1;
    }

    fn new(directory: &Path, override_max_key_count: Option<u64>) -> Self {
        let key_size = mem::size_of::<K>() as u64;
        let value_size = mem::size_of::<V>() as u64;
        let max_key_count = match override_max_key_count {
            None => max_key_count(key_size, value_size),
            Some(n) => n,
        };
        let split_at = split_at(max_key_count);
        Self {
            magic_header: String::from(MAGIC_HEADER),
            directory: PathBuf::from(directory),
            node_count: 0,
            entry_count: 0,
            root_page_nr: 0,
            emtpy_pages: vec![],
            key_size,
            value_size,
            max_key_count,
            split_at,
            key_type: PhantomData,
            value_type: PhantomData,
            fh: None,
        }
    }

    fn load_meta(path: &Path, directory: &Path) -> Result<Self> {
        let fh = File::open(path)?;
        let mut btree: Self = bincode::deserialize_from(fh)?;
        btree.directory = PathBuf::from(directory);
        Ok(btree)
    }

    fn store_meta(&self) -> Result<()> {
        let fh = File::create(meta_file_path(&self.directory))?;
        bincode::serialize_into(fh, self)?;
        Ok(())
    }

    pub fn root(&mut self) -> Result<BTNode<K, V>> {
        self.load_node(self.root_page_nr).or_else(|err| {
            // FIXME: better error checking here?
            self.root_page_nr = self.next_page_nr();
            Ok(BTNode::new_leaf(self.root_page_nr, &[], &[], None))
        })
    }

    pub fn load_node(&mut self, page_nr: u64) -> Result<BTNode<K, V>> {
        if self.fh.is_none() {
            self.fh = Some(OpenOptions::new().read(true).write(true).create(true).open(db_path(&self.directory))?);
        }
        if self.emtpy_pages.contains(&page_nr) {
            panic!("Page {:?} requested, but it has been deleted", page_nr);
        }
        let fh = self.fh.as_mut().ok_or(Error::InvalidFileHandle)?;
        let offset = PAGE_SIZE * page_nr;
        fh.seek(SeekFrom::Start(offset))?;
        let node = BTNode::deserialize_from(fh, page_nr)?;
        Ok(node)
    }

    fn store_node(&mut self, node: &BTNode<K, V>) -> Result<()> {
        if self.fh.is_none() {
            self.fh = Some(OpenOptions::new().read(true).write(true).create(true).open(db_path(&self.directory))?);
        }
        let fh = self.fh.as_mut().ok_or(Error::InvalidFileHandle)?;
        let offset = PAGE_SIZE * node.page_nr();
        fh.seek(SeekFrom::Start(offset))?;
        node.serialize_into(fh)?;
        // fh.sync_all()?;
        let pos = fh.seek(SeekFrom::Current(0))?;
        assert!(pos < offset + PAGE_SIZE, "{:?} - pos = {}, offset+PAGE_SIZE = {}", node, pos, offset + PAGE_SIZE);
        let padding = offset + PAGE_SIZE - pos;
        if padding > 0 {
            fh.write_all(&vec![0u8; padding as usize])?;
        }
        Ok(())
    }
}


// Make sure the meta data for the BTree is written to disk
impl<K, V> Drop for BTree<K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        if self.len() > 0 {
            self.store_meta().unwrap()
        }
    }
}


pub struct BTreeIterator<'a, K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{
    btree: &'a mut BTree<K, V>,
    next_node: Option<PagePtr>,
    current_iterator: std::vec::IntoIter<K>,
}


impl<'a, K, V> BTreeIterator<'a, K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{

    fn new(btree: &'a mut BTree<K, V>) -> Result<Self> {
        let current_node = match btree.load_node(0)? {
            BTNode::Internal(_) => panic!("Programming error: page 0 should not be Interal"),
            BTNode::Leaf(node) => node,
        };
        let next_node = current_node.next();
        let keys: Vec<K> = current_node.keys().collect();
        let current_iterator = keys.into_iter();
        Ok(Self { btree, next_node, current_iterator })
    }

}


impl<'a, K, V> Iterator for BTreeIterator<'a, K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_iterator.next() {
            Some(k) => Some(k),
            None => {
                match self.next_node {
                    Some(page_nr) => {
                        let node = match self.btree.load_node(page_nr).unwrap() {
                            BTNode::Internal(_) => panic!("Programming error: page 0 should not be Interal"),
                            BTNode::Leaf(node) => node,
                        };
                        self.next_node = node.next();
                        self.current_iterator = node.keys().collect::<Vec<K>>().into_iter();
                        self.current_iterator.next()
                    },
                    None => None
                }
            }
        }
    }
}


pub struct BTreeValueIterator<'a, K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{
    btree: &'a mut BTree<K, V>,
    next_node: Option<PagePtr>,
    current_iterator: std::vec::IntoIter<V>,
}


impl<'a, K, V> BTreeValueIterator<'a, K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{

    fn new(btree: &'a mut BTree<K, V>) -> Result<Self> {
        let current_node = match btree.load_node(0)? {
            BTNode::Internal(_) => panic!("Programming error: page 0 should not be Interal"),
            BTNode::Leaf(node) => node,
        };
        let next_node = current_node.next();
        let values: Vec<V> = current_node.values().collect();
        let current_iterator = values.into_iter();
        Ok(Self { btree, next_node, current_iterator })
    }

}


impl<'a, K, V> Iterator for BTreeValueIterator<'a, K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Copy + Serialize + DeserializeOwned,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_iterator.next() {
            Some(k) => Some(k),
            None => {
                match self.next_node {
                    Some(page_nr) => {
                        let node = match self.btree.load_node(page_nr).unwrap() {
                            BTNode::Internal(_) => panic!("Programming error: page 0 should not be Interal"),
                            BTNode::Leaf(node) => node,
                        };
                        self.next_node = node.next();
                        self.current_iterator = node.values().collect::<Vec<V>>().into_iter();
                        self.current_iterator.next()
                    },
                    None => None
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_len() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bt: BTree<u128, u128> = BTree::open(temp_dir.path(), None)?;
        assert_eq!(bt.len(), 0);

        Ok(())
    }

    #[test]
    fn test_root() -> Result<()> {
        unsafe { OVERRIDE_MAX_KEY_COUNT = 4; }
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut bt: BTree<u128, u128> = BTree::open(temp_dir.path(), Some(4))?;
        println!("{:?}", bt);
        let root = bt.root()?;
        assert_eq!(root.page_nr(), 0);
        assert_eq!(root.len(), 0);

        Ok(())
    }

}
