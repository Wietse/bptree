#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use crate::error::{Error, Result};
use crate::BTree;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    mem,
    path::{Path, PathBuf},
};

type PagePtr = u64;

#[derive(Debug)]
pub struct Leaf<K, V> {
    page_nr: PagePtr,
    keys: Vec<K>,
    entries: Vec<V>,
    next: Option<PagePtr>,
}

impl<K, V> Leaf<K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Clone + Copy + Serialize + DeserializeOwned,
{
    fn new(page_nr: u64, keys: &[K], entries: &[V], next: Option<PagePtr>) -> Self {
        // let padding = (size - 2 * order * (mem::size_of::<K>() + mem::size_of::<V>()) - mem::size_of::<PagePtr>()) as u64;
        Leaf {
            page_nr,
            keys: keys.to_vec(),
            entries: entries.to_vec(),
            next,
        }
    }

    fn is_full(&self, max_key_count: u64) -> bool {
        self.keys.len() >= max_key_count as usize
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match self.keys.binary_search(key) {
            Ok(i) => Some(self.entries[i]),
            Err(_) => None,
        }
    }

    pub fn set(
        mut self,
        btree: &mut BTree<K, V>,
        key: K,
        value: V,
    ) -> Result<(Option<(K, PagePtr)>, Option<V>)>
    where
        V: Debug + Clone + Copy + Serialize + DeserializeOwned,
    {
        match self.keys.binary_search(&key) {
            Ok(i) => {
                // exact match -> overwrite and return original value
                let original_value = mem::replace(&mut self.entries[i], value);
                btree.store_node(&BTNode::Leaf(self))?;
                Ok((None, Some(original_value)))
            }
            Err(i) => match self.is_full(btree.max_key_count) {
                true => {
                    let (split_key, mut new_leaf) =
                        self.split(btree.next_page_nr(), btree.split_at);
                    let split_page_nr = new_leaf.page_nr;
                    match i < btree.split_at {
                        true => self.insert(i, key, value),
                        false => new_leaf.insert(i - btree.split_at, key, value),
                    }
                    btree.store_node(&BTNode::Leaf(self))?;
                    btree.store_node(&BTNode::Leaf(new_leaf))?;
                    Ok((Some((split_key, split_page_nr)), None))
                }
                false => {
                    self.insert(i, key, value);
                    btree.store_node(&BTNode::Leaf(self))?;
                    Ok((None, None))
                }
            },
        }
    }

    // keys and entries have same length
    // [k0, k1, k2, k3] -> [k0, k1] | [k2, k3]  split_key == k2
    // [v0, v1, v2, v3] -> [v0, v1] | [v2, v3]
    fn split(&mut self, page_nr: u64, split_at: usize) -> (K, Self) {
        let split_key = self.keys[split_at];
        let node = Self::new(
            page_nr,
            &self.keys[split_at..],
            &self.entries[split_at..],
            None,
        );
        self.next = Some(page_nr);
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
        bincode::serialize_into(fh, &self.next)?;
        Ok(())
    }

    pub fn deserialize_from(fh: &File, page_nr: u64) -> Result<Self> {
        let node = Self {
            page_nr,
            keys: bincode::deserialize_from(fh)?,
            entries: bincode::deserialize_from(fh)?,
            next: bincode::deserialize_from(fh)?,
        };
        Ok(node)
    }
}

#[derive(Debug)]
pub struct Internal<K> {
    page_nr: PagePtr,
    keys: Vec<K>,
    entries: Vec<PagePtr>,
}

impl<K> Internal<K>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
{
    fn new(page_nr: u64, keys: &[K], entries: &[PagePtr]) -> Self {
        // let padding = (size - 2 * order * (mem::size_of::<K>() + mem::size_of::<PagePtr>()) - mem::size_of::<PagePtr>()) as u64;
        Internal {
            page_nr,
            keys: keys.to_vec(),
            entries: entries.to_vec(),
        }
    }

    fn is_full(&self, max_key_count: u64) -> bool {
        self.keys.len() >= max_key_count as usize
    }

    pub fn get(&self, key: &K) -> PagePtr {
        match self.keys.binary_search(key) {
            Ok(i) => self.entries[i + 1], // keys[i] == key -> right subtree
            Err(i) => self.entries[i],    // keys[i] > key -> left subtree
        }
    }

    // entries has 1 more value then keys
    // take the middle key out, but leave its entry!
    // [k0, k1, k2, k3] -> [k0, k1] | [k3]  split_key == k2
    // [r0, r1, r2, r3, r4] -> [r0, r1, r2] | [r3, r4]
    fn split(&mut self, page_nr: u64, split_at: usize) -> (K, Self) {
        let split_key = self.keys[split_at];
        let node: Internal<K>;
        node = Internal::new(
            page_nr,
            &self.keys[split_at + 1..],
            &self.entries[split_at + 1..],
        );
        self.keys.drain(split_at..);
        self.entries.drain(split_at + 1..);
        (split_key, node)
    }

    pub fn set<V>(
        mut self,
        btree: &mut BTree<K, V>,
        key: K,
        value: V,
    ) -> Result<(Option<(K, PagePtr)>, Option<V>)>
    where
        V: Debug + Default + Clone + Copy + Serialize + DeserializeOwned,
    {
        let next_level_page_nr = self.get(&key);
        let return_value = match btree.load_node(next_level_page_nr)? {
            BTNode::Internal(node) => node.set(btree, key, value)?,
            BTNode::Leaf(node) => node.set(btree, key, value)?,
        };
        match return_value {
            (None, v) => Ok((None, v)),
            (Some((key, page_nr)), v) => match self.keys.binary_search(&key) {
                Err(i) => match self.is_full(btree.max_key_count) {
                    true => {
                        let (split_key, mut new_node) =
                            self.split(btree.next_page_nr(), btree.split_at);
                        let split_page_nr = new_node.page_nr;
                        match i < btree.split_at {
                            true => self.insert(i, key, page_nr),
                            false => new_node.insert(i - btree.split_at, key, page_nr),
                        }
                        btree.store_node(&BTNode::Internal(self))?;
                        btree.store_node(&BTNode::Internal(new_node))?;
                        Ok((Some((split_key, split_page_nr)), None))
                    }
                    false => {
                        self.insert(i, key, page_nr);
                        btree.store_node(&BTNode::Internal(self))?;
                        Ok((None, None))
                    }
                },
                Ok(_) => panic!("Programming error: key should not be present!"),
            },
        }
    }

    fn insert(&mut self, i: usize, key: K, value: PagePtr) {
        self.keys.insert(i, key);
        self.entries.insert(i + 1, value);
    }

    fn serialize_into(&self, fh: &File) -> Result<()> {
        bincode::serialize_into(fh, &self.keys)?;
        bincode::serialize_into(fh, &self.entries)?;
        Ok(())
    }

    pub fn deserialize_from(fh: &File, page_nr: u64) -> Result<Self> {
        let node = Self {
            page_nr,
            keys: bincode::deserialize_from(fh)?,
            entries: bincode::deserialize_from(fh)?,
        };
        Ok(node)
    }
}

#[derive(Debug)]
pub enum BTNode<K, V> {
    Internal(Internal<K>),
    Leaf(Leaf<K, V>),
}

impl<K, V> BTNode<K, V>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
    V: Debug + Default + Clone + Copy + Serialize + DeserializeOwned,
{
    pub fn new_leaf(page_nr: u64, keys: &[K], entries: &[V], next: Option<PagePtr>) -> Self {
        BTNode::Leaf(Leaf::new(page_nr, keys, entries, next))
    }

    pub fn new_internal(page_nr: u64, keys: &[K], entries: &[u64]) -> Self {
        BTNode::Internal(Internal::new(page_nr, keys, entries))
    }

    pub fn set(
        self,
        btree: &mut BTree<K, V>,
        key: K,
        value: V,
    ) -> Result<(Option<(K, PagePtr)>, Option<V>)>
    where
        V: Debug + Clone + Copy + Serialize + DeserializeOwned,
    {
        // "self" is the root page!
        let page_nr = self.page_nr();
        match self {
            BTNode::Internal(node) => node.set(btree, key, value),
            BTNode::Leaf(node) => node.set(btree, key, value),
        }
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
                let node = Internal::<K>::deserialize_from(fh, page_nr)?;
                Ok(BTNode::Internal(node))
            }
            1 => {
                let node = Leaf::<K, V>::deserialize_from(fh, page_nr)?;
                Ok(BTNode::Leaf(node))
            }
            _ => Err(Error::InvalidFileFormat),
        }
    }
}
