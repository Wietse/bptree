#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

mod error;

pub use error::{Error, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    mem,
    path::{Path, PathBuf},
};

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

const PAGE_SIZE: u64 = 4096;
const MAGIC_HEADER: &str = "%bptree%";

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
struct Node<K, V> {
    #[serde(skip)]
    page_nr: u64,
    keys: Vec<K>,
    entries: Vec<V>,
}

impl<K, V> Node<K, V>
where
    K: Clone + Copy + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    fn new(page_nr: u64, keys: &[K], entries: &[V]) -> Self {
        Node {
            page_nr,
            keys: keys.to_vec(),
            entries: entries.to_vec(),
        }
    }

    fn is_full(&self, max_key_count: u64) -> bool {
        self.keys.len() >= max_key_count as usize
    }

    fn split_leaf(&mut self, page_nr: u64, split_at: usize) -> (K, Self) {
        // keys and entries have same length
        // [k0, k1, k2, k3] -> [k0, k1] | [k2, k3]  split_key == k2
        // [v0, v1, v2, v3] -> [v0, v1] | [v2, v3]
        let split_key = self.keys[split_at];
        let node = Node::new(page_nr, &self.keys[split_at..], &self.entries[split_at..]);
        self.keys.drain(split_at..);
        self.entries.drain(split_at..);
        (split_key, node)
    }

    fn insert(&mut self, i: usize, key: K, value: V) {
        self.keys.insert(i, key);
        self.entries.insert(i, value);
    }

    fn serialize_into(&self, fh: &File) -> Result<()> {
        bincode::serialize_into(fh, &self.keys)?;
        bincode::serialize_into(fh, &self.entries)?;
        Ok(())
    }

    pub fn deserialize_from(fh: &File, page_nr: u64) -> Result<Self> {
        let node = Node {
            page_nr,
            keys: bincode::deserialize_from(fh)?,
            entries: bincode::deserialize_from(fh)?,
        };
        Ok(node)
    }
}

impl<K> Node<K, u64>
where
    K: Clone + Copy + Serialize + DeserializeOwned,
{
    fn split_internal(&mut self, page_nr: u64, split_at: usize) -> (K, Self) {
        let split_key = self.keys[split_at];
        let node: Node<K, u64>;
        // take the middle key out, but leave its entry!
        // [k0, k1, k2, k3] -> [k0, k1] | [k3]  split_key == k2
        // [r0, r1, r2, r3, r4] -> [r0, r1, r2] | [r3, r4]
        node = Node::new(
            page_nr,
            &self.keys[split_at + 1..],
            &self.entries[split_at + 1..],
        );
        self.keys.drain(split_at..);
        self.entries.drain(split_at + 1..);
        (split_key, node)
    }

    fn insert_internal(&mut self, i: usize, key: K, value: u64) {
        self.keys.insert(i, key);
        self.entries.insert(i + 1, value);
    }
}

// #[derive(Debug, Serialize, Deserialize)]
#[derive(Debug)]
enum BTNode<K, V> {
    Internal(Node<K, u64>),
    Leaf(Node<K, V>),
}

impl<K, V> BTNode<K, V>
where
    K: Clone + Copy + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    pub fn new_leaf(page_nr: u64, keys: &[K], entries: &[V]) -> Self {
        BTNode::Leaf(Node::new(page_nr, keys, entries))
    }

    pub fn new_internal(page_nr: u64, keys: &[K], entries: &[u64]) -> Self {
        BTNode::Internal(Node::new(page_nr, keys, entries))
    }

    pub fn page_nr(&self) -> u64 {
        match self {
            Self::Internal(node) => node.page_nr,
            Self::Leaf(node) => node.page_nr,
        }
    }

    pub fn serialize_into(&self, fh: &File) -> Result<()> {
        match self {
            Self::Internal(node) => {
                bincode::serialize_into(fh, &0_u8)?;
                node.serialize_into(fh)?;
            }
            Self::Leaf(node) => {
                bincode::serialize_into(fh, &1_u8)?;
                node.serialize_into(fh)?;
            }
        }
        Ok(())
    }

    pub fn deserialize_from(fh: &mut File, page_nr: u64) -> Result<Self> {
        let mut buffer = [0_u8; 1];
        fh.read_exact(&mut buffer)?;
        match buffer[0] {
            0 => {
                let node = Node::<K, u64>::deserialize_from(fh, page_nr)?;
                Ok(BTNode::Internal(node))
            }
            1 => {
                let node = Node::<K, V>::deserialize_from(fh, page_nr)?;
                Ok(BTNode::Leaf(node))
            }
            _ => Err(Error::InvalidFileFormat),
        }
    }

    fn set_page_nr(&mut self, page_nr: u64) {
        match self {
            Self::Internal(ref mut node) => {
                node.page_nr = page_nr;
            }
            Self::Leaf(ref mut node) => {
                node.page_nr = page_nr;
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BTree<K, V>
where
    K: Debug + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Copy + Serialize + DeserializeOwned,
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
    K: Debug + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Copy + Serialize + DeserializeOwned,
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
                BTNode::Internal(internal_node) => {
                    match internal_node.keys.binary_search(&key) {
                        Ok(i) => {
                            // node.keys[i] == key -> right subtree
                            page_nr = internal_node.entries[i + 1];
                        }
                        Err(i) => {
                            // node.keys[i] > key -> left subtree
                            page_nr = internal_node.entries[i];
                        }
                    }
                }
                BTNode::Leaf(leaf_node) => match leaf_node.keys.binary_search(&key) {
                    Ok(i) => {
                        return Ok(Some(leaf_node.entries[i]));
                    }
                    Err(_) => {
                        return Ok(None);
                    }
                },
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        if self.len() == 0 {
            self.create_first_root(key, value)?;
            return Ok(None);
        }
        let mut page_nr = self.root_page_nr;
        let mut node_path: Vec<Node<K, u64>> = vec![];
        loop {
            match self.load_node(page_nr)? {
                BTNode::Internal(internal_node) => {
                    page_nr = match internal_node.keys.binary_search(&key) {
                        Ok(i) => internal_node.entries[i + 1], // internal_node.keys[i] == key -> right subtree
                        Err(i) => internal_node.entries[i], // internal_node.keys[i]  > key -> left subtree
                    };
                    node_path.push(internal_node);
                }
                BTNode::Leaf(mut leaf_node) => {
                    match leaf_node.keys.binary_search(&key) {
                        Ok(i) => {
                            // exact match -> overwrite and return original value
                            let original_value = mem::replace(&mut leaf_node.entries[i], value);
                            self.store_node(&BTNode::Leaf(leaf_node))?;
                            return Ok(Some(original_value));
                        }
                        Err(i) => {
                            if leaf_node.is_full(self.max_key_count) {
                                self.split_and_insert(leaf_node, node_path, i, key, value)?;
                            } else {
                                leaf_node.insert(i, key, value);
                                self.store_node(&BTNode::Leaf(leaf_node))?;
                            }
                            self.entry_count += 1;
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }

    fn split_and_insert(
        &mut self,
        mut leaf_node: Node<K, V>,
        mut node_path: Vec<Node<K, u64>>,
        i: usize,
        key: K,
        value: V,
    ) -> Result<Option<V>> {
        // i is insertion point
        let (mut split_key, mut new_leaf) =
            leaf_node.split_leaf(self.next_page_nr(), self.split_at);
        match i < self.split_at {
            true => leaf_node.insert(i, key, value),
            false => new_leaf.insert(i - self.split_at, key, value),
        }
        if leaf_node.page_nr == self.root_page_nr {
            // The root leaf node has been split
            self.create_new_root(split_key, new_leaf.page_nr)?;
        }
        let mut page_nr = new_leaf.page_nr;
        self.store_node(&BTNode::Leaf(leaf_node))?;
        self.store_node(&BTNode::Leaf(new_leaf))?;

        // Now we walk up the tree and adjust the internal nodes
        while let Some(mut internal_node) = node_path.pop() {
            match internal_node.keys.binary_search(&split_key) {
                Err(j) => {
                    if internal_node.is_full(self.max_key_count) {
                        let (internal_split_key, mut new_internal_node) =
                            internal_node.split_internal(self.next_page_nr(), self.split_at);
                        match j < self.split_at {
                            true => internal_node.insert_internal(j, split_key, page_nr),
                            false => new_internal_node.insert_internal(j - self.split_at,
                                                                       split_key,
                                                                       page_nr),
                        }
                        page_nr = new_internal_node.page_nr;
                        split_key = internal_split_key;
                        if internal_node.page_nr == self.root_page_nr {
                            // The root internal node has been split
                            self.create_new_root(split_key, new_internal_node.page_nr)?;
                        }
                        self.store_node(&BTNode::Internal(internal_node))?;
                        self.store_node(&BTNode::Internal(new_internal_node))?;
                    } else {
                        internal_node.insert_internal(j, split_key, page_nr);
                        page_nr = internal_node.page_nr;
                        self.store_node(&BTNode::Internal(internal_node))?;
                    }
                }
                _ => panic!("Programming error: the key should not be present"),
            }
        }
        Ok(None)
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

    fn create_first_root(&mut self, key: K, value: V) -> Result<()> {
        self.root_page_nr = self.next_page_nr();
        let root = BTNode::new_leaf(self.root_page_nr, &[key], &[value]);
        self.entry_count += 1;
        self.store_node(&root)?;
        Ok(())
    }

    fn create_new_root(&mut self, key: K, new_page_nr: u64) -> Result<()> {
        let old_root_page_nr = self.root_page_nr;
        self.root_page_nr = self.next_page_nr();
        let new_root =
            BTNode::new_internal(self.root_page_nr, &[key], &[old_root_page_nr, new_page_nr]);
        self.store_node(&new_root)?;
        Ok(())
    }

    fn load_node(&mut self, page_nr: u64) -> Result<BTNode<K, V>> {
        if self.fh.is_none() {
            self.fh = Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(db_path(&self.directory))?,
            );
        }
        let fh = self.fh.as_mut().ok_or(Error::InvalidFileHandle)?;
        let offset = PAGE_SIZE * page_nr;
        fh.seek(SeekFrom::Start(offset))?;
        let node = BTNode::deserialize_from(fh, page_nr)?;
        Ok(node)
    }

    fn store_node(&mut self, node: &BTNode<K, V>) -> Result<()> {
        if self.fh.is_none() {
            self.fh = Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(db_path(&self.directory))?,
            );
        }
        let fh = self.fh.as_mut().ok_or(Error::InvalidFileHandle)?;
        let offset = PAGE_SIZE * node.page_nr();
        fh.seek(SeekFrom::Start(offset))?;
        node.serialize_into(fh)?;
        // fh.sync_all()?;
        let pos = fh.seek(SeekFrom::Current(0))?;
        assert!(
            pos < offset + PAGE_SIZE,
            "{:?} - pos = {}, offset+PAGE_SIZE = {}",
            node,
            pos,
            offset + PAGE_SIZE
        );
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
    K: Debug + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Copy + Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        if self.len() > 0 {
            self.store_meta().unwrap()
        }
    }
}
