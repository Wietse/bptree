#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

mod error;
mod node;

pub use error::{Error, Result};
use node::BTNode;
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
    (PAGE_SIZE - size_value - 17) / (size_key + size_value)
}


fn split_at(size_key: u64, size_value: u64) -> usize {
    let max_key_count = max_key_count(size_key, size_value);
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
    root_page_nr: u64,
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
    pub fn open<P: AsRef<Path>>(directory: P) -> Result<Self> {
        fs::create_dir_all(&directory)?;
        let meta_path = meta_file_path(directory.as_ref());
        match &meta_path.exists() {
            true => Self::load_meta(&meta_path, directory.as_ref()),
            false => Ok(Self::new(directory.as_ref())),
        }
    }

    pub fn len(&self) -> usize {
        self.entry_count as usize
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

    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
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

    fn create_first_root(&mut self, key: K, value: V) -> Result<()> {
        self.root_page_nr = self.next_page_nr();
        let root = BTNode::new_leaf(self.root_page_nr, &[key], &[value], None);
        self.entry_count += 1;
        self.store_node(&root)?;
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

    fn new(directory: &Path) -> Self {
        let key_size = mem::size_of::<K>() as u64;
        let value_size = mem::size_of::<V>() as u64;
        let max_key_count = max_key_count(key_size, value_size);
        let split_at = split_at(key_size, value_size);
        Self {
            magic_header: String::from(MAGIC_HEADER),
            directory: PathBuf::from(directory),
            node_count: 0,
            entry_count: 0,
            root_page_nr: 0,
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

    fn root(&mut self) -> Result<BTNode<K, V>> {
        self.load_node(self.root_page_nr)
    }

    fn load_node(&mut self, page_nr: u64) -> Result<BTNode<K, V>> {
        if self.fh.is_none() {
            self.fh = Some(OpenOptions::new().read(true).write(true).create(true).open(db_path(&self.directory))?);
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
