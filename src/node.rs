// #![allow(dead_code)]
// #![allow(unused_variables)]
// #![allow(unused_imports)]

use crate::error::{Error, Result};
use crate::BTree;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, fs::File, io::Read, mem};


pub type PagePtr = u64;


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

    // Returns the associated value for `key` as `Some(value)` or `None` if it's not present.
    //
    fn get(&self, key: &K) -> Option<V> {
        match self.keys.binary_search(key) {
            Ok(i) => Some(self.entries[i]),
            Err(_) => None,
        }
    }

    // Inserts a `key`/`value` pair
    //
    // This method returns different kinds of information depending on the situation:
    //   - If the key is already present, the value will be overwritten and the
    //     old value will be returned as `Ok((None, Some(old_value)))`.
    //   - If the key is new, the key/value pair is inserted. Now we have 2 cases to consider:
    //     1. The node is not yet full: nothing more to do, return `Ok((None, None))`.
    //     2. The node is full: it needs to be split up, return `Ok((Some((split_key, new_page_nr)), None))`.
    //
    fn set(mut self, btree: &mut BTree<K, V>, key: K, value: V) -> Result<(Option<(K, PagePtr)>, Option<V>)>
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
                    let (split_key, mut new_leaf) = self.split(btree.next_page_nr(), btree.split_at);
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

    fn remove(
        mut self,
        btree: &mut BTree<K, V>,
        key: K,
        parent: Option<&mut Internal<K>>,
        path_info: Option<&ChildNodeInfo>,
    ) -> Result<(Option<V>, Option<PagePtr>)> {
        match self.keys.binary_search(&key) {
            Err(_) => Ok((None, None)),
            Ok(i) => {
                self.keys.remove(i);
                let original_value = Some(self.entries.remove(i));
                let mut deleted_page = None;
                if self.keys.len() < btree.split_at as usize && parent.is_some() {
                    // else this is the root node => nothing more to dead_code
                    let parent = parent.unwrap();
                    let path_info = path_info.unwrap();
                    let mut done = false;
                    if path_info.lsibling.is_some() {
                        // try to transfer a key/value pair from left sibling
                        let mut node = btree.load_node(path_info.lsibling.unwrap())?.leaf_node();
                        if node.keys.len() > btree.split_at as usize {
                            let k = node.keys.pop().unwrap();
                            let v = node.entries.pop().unwrap();
                            self.keys.insert(0, k);
                            self.entries.insert(0, v);
                            parent.keys[path_info.rparent.unwrap()] = k;
                            btree.store_node(&BTNode::Leaf(node))?;
                            done = true;
                        }
                    }
                    if !done && path_info.rsibling.is_some() {
                        // try to transfer a key/value pair from right sibling
                        let mut node = btree.load_node(path_info.rsibling.unwrap())?.leaf_node();
                        if node.keys.len() > btree.split_at as usize {
                            let k = node.keys.remove(0);
                            let v = node.entries.remove(0);
                            self.keys.push(k);
                            self.entries.push(v);
                            parent.keys[path_info.lparent.unwrap()] = node.keys[0];
                            btree.store_node(&BTNode::Leaf(node))?;
                            done = true;
                        }
                    }
                    if !done {
                        if path_info.lsibling.is_some() {
                            // merge this node into the left sibling
                            let mut node = btree.load_node(path_info.lsibling.unwrap())?.leaf_node();
                            node.keys.extend(&self.keys);
                            node.entries.extend(&self.entries);
                            node.next = self.next;
                            btree.on_page_deleted(self.page_nr);
                            deleted_page = Some(self.page_nr);
                            self = node;
                        } else {
                            // merge the right sibling into this node
                            assert!(path_info.rsibling.is_some());
                            assert_eq!(path_info.rsibling, self.next);
                            let right_node = btree.load_node(path_info.rsibling.unwrap())?.leaf_node();
                            self.keys.extend(right_node.keys);
                            self.entries.extend(right_node.entries);
                            self.next = right_node.next;
                            btree.on_page_deleted(right_node.page_nr);
                            deleted_page = Some(right_node.page_nr);
                        }
                    }
                }
                btree.store_node(&BTNode::Leaf(self))?;
                Ok((original_value, deleted_page))
            }
        }
    }

    fn new(page_nr: u64, keys: &[K], entries: &[V], next: Option<PagePtr>) -> Self {
        // let padding = (size - 2 * order * (mem::size_of::<K>() + mem::size_of::<V>()) - mem::size_of::<PagePtr>()) as u64;
        Leaf { page_nr, keys: keys.to_vec(), entries: entries.to_vec(), next }
    }

    fn is_full(&self, max_key_count: u64) -> bool {
        self.keys.len() >= max_key_count as usize
    }

    // keys and entries have same length
    // [k0, k1, k2, k3] -> [k0, k1] | [k2, k3]  split_key == k2
    // [v0, v1, v2, v3] -> [v0, v1] | [v2, v3]
    fn split(&mut self, page_nr: u64, split_at: usize) -> (K, Self) {
        let split_key = self.keys[split_at];
        let node = Self::new(page_nr, &self.keys[split_at..], &self.entries[split_at..], self.next);
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

    fn deserialize_from(fh: &File, page_nr: u64) -> Result<Self> {
        let node = Self {
            page_nr,
            keys: bincode::deserialize_from(fh)?,
            entries: bincode::deserialize_from(fh)?,
            next: bincode::deserialize_from(fh)?,
        };
        Ok(node)
    }

    pub fn keys(self) -> std::vec::IntoIter<K> {
        self.keys.into_iter()
    }

    pub fn values(self) -> std::vec::IntoIter<V> {
        self.entries.into_iter()
    }

    pub fn next(&self) -> Option<PagePtr> {
        self.next
    }
}


// impl<K, V> IntoIterator for Leaf<K, V> {
//     type Item = K;
//     type IntoIter = std::vec::IntoIter<Self::Item>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.keys.into_iter()
//     }
// }


#[derive(Debug)]
pub struct Internal<K> {
    page_nr: PagePtr,
    keys: Vec<K>,
    entries: Vec<PagePtr>,
}


#[derive(Debug)]
struct ChildNodeInfo {
    page_nr: PagePtr,
    lparent: Option<usize>, // LeftSubtree(keys[lparent]) == page_nr
    rparent: Option<usize>, // RightSubtree(keys[rparent]) == page_nr
    lsibling: Option<PagePtr>,
    rsibling: Option<PagePtr>,
}


impl<K> Internal<K>
where
    K: Debug + Default + Clone + Copy + Ord + Serialize + DeserializeOwned,
{
    fn get(&self, key: &K) -> PagePtr {
        match self.keys.binary_search(key) {
            Ok(i) => self.entries[i + 1], // keys[i] == key -> right subtree
            Err(i) => self.entries[i],    // keys[i] > key -> left subtree
        }
    }

    fn set<V>(mut self, btree: &mut BTree<K, V>, key: K, value: V) -> Result<(Option<(K, PagePtr)>, Option<V>)>
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
            (Some((key, page_nr)), _) => match self.keys.binary_search(&key) {
                Err(i) => match self.is_full(btree.max_key_count) {
                    true => {
                        let (split_key, mut new_node) = self.split(btree.next_page_nr(), btree.split_at);
                        let split_page_nr = new_node.page_nr;
                        match i < btree.split_at {
                            true => self.insert(i, key, page_nr),
                            // minus 1 because we're taking the split_key out!
                            false => new_node.insert(i - btree.split_at - 1, key, page_nr),
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

    fn get_child_node_info(&self, key: &K) -> ChildNodeInfo {
        match self.keys.binary_search(key) {
            Ok(i) => {
                // exact match -> right subtree
                ChildNodeInfo {
                    page_nr: self.entries[i + 1],
                    lparent: if i < self.keys.len() - 1 { Some(i + 1) } else { None },
                    rparent: Some(i),
                    lsibling: Some(self.entries[i]),
                    rsibling: if i < self.entries.len() - 2 { Some(self.entries[i + 2]) } else { None },
                }
            }
            Err(i) => {
                // not found: keys(i) > key -> left subtree
                ChildNodeInfo {
                    page_nr: self.entries[i],
                    lparent: Some(i),
                    rparent: if i > 0 { Some(i - 1) } else { None },
                    lsibling: if i > 0 { Some(self.entries[i - 1]) } else { None },
                    rsibling: if i < self.entries.len() - 1 { Some(self.entries[i + 1]) } else { None },
                }
            }
        }
    }

    fn remove<V>(
        mut self,
        btree: &mut BTree<K, V>,
        key: K,
        parent: Option<&mut Internal<K>>,
        path_info: Option<&ChildNodeInfo>,
    ) -> Result<(Option<V>, Option<PagePtr>)>
    where
        V: Debug + Default + Clone + Copy + Serialize + DeserializeOwned,
    {
        let child_info = self.get_child_node_info(&key);
        let (original_value, deleted_page) = match btree.load_node(child_info.page_nr)? {
            BTNode::Internal(node) => node.remove(btree, key, Some(&mut self), Some(&child_info))?,
            BTNode::Leaf(node) => node.remove(btree, key, Some(&mut self), Some(&child_info))?,
        };

        let result = match deleted_page {
            None => Ok((original_value, None)),
            Some(page_nr) => {
                let deleted_page = self.remove_page(btree, page_nr, parent, path_info)?;
                Ok((original_value, deleted_page))
            }
        };
        btree.store_node(&BTNode::Internal(self))?;
        result
    }

    fn remove_page<V>(
        &mut self,
        btree: &mut BTree<K, V>,
        page_nr: PagePtr,
        parent: Option<&mut Internal<K>>,
        path_info: Option<&ChildNodeInfo>,
    ) -> Result<Option<PagePtr>>
    where
        V: Debug + Default + Clone + Copy + Serialize + DeserializeOwned,
    {
        match self.entries.binary_search(&page_nr) {
            Err(_) => panic!("Programming error: deleted page should be present!"),
            Ok(i) => {
                self.keys.remove(i - 1);
                self.entries.remove(i);

                let deleted_page = match parent {
                    None => {
                        // This is the root node!
                        if self.keys.len() == 0 {
                            // We're at the root and it's last key has just been removed
                            // The tree collapses into 1 leaf node.
                            let new_root_page_nr = self.entries[0];
                            btree.root_page_nr = new_root_page_nr;
                            btree.on_page_deleted(self.page_nr);
                            Some(self.page_nr)
                        } else {
                            None
                        }
                    }

                    Some(parent) => {
                        let mut deleted_page = None;
                        if self.keys.len() < btree.split_at as usize {
                            let path_info = path_info.unwrap();
                            let mut done = false;
                            if path_info.lsibling.is_some() {
                                // try to transfer a key/value pair from left sibling
                                let mut node = btree.load_node(path_info.lsibling.unwrap())?.internal_node();
                                if node.keys.len() > btree.split_at as usize {
                                    let k = node.keys.pop().unwrap();
                                    let v = node.entries.pop().unwrap();
                                    self.keys.insert(0, k);
                                    self.entries.insert(0, v);
                                    parent.keys[path_info.rparent.unwrap()] = k;
                                    btree.store_node(&BTNode::Internal(node))?;
                                    done = true;
                                }
                            }

                            if !done && path_info.rsibling.is_some() {
                                // try to transfer a key/value pair from right sibling
                                let mut node = btree.load_node(path_info.rsibling.unwrap())?.internal_node();
                                if node.keys.len() > btree.split_at {
                                    let k = node.keys.remove(0);
                                    let v = node.entries.remove(0);
                                    self.keys.push(k);
                                    self.entries.push(v);
                                    parent.keys[path_info.lparent.unwrap()] = node.keys[0];
                                    btree.store_node(&BTNode::Internal(node))?;
                                    done = true;
                                }
                            }

                            if !done {
                                if path_info.lsibling.is_some() {
                                    // merge this node into the left sibling
                                    let mut node = btree.load_node(path_info.lsibling.unwrap())?.internal_node();
                                    node.keys.push(parent.keys[path_info.rparent.unwrap()]);
                                    node.keys.extend(&self.keys);
                                    node.entries.extend(&self.entries);
                                    btree.on_page_deleted(self.page_nr);
                                    deleted_page = Some(self.page_nr);
                                    *self = node;
                                } else if path_info.rsibling.is_some() {
                                    // merge the right sibling into this node
                                    // we only get here if "self" if the first leaf of the BTree
                                    let node = btree.load_node(path_info.rsibling.unwrap())?.internal_node();
                                    self.keys.push(parent.keys[path_info.lparent.unwrap()]);
                                    self.keys.extend(node.keys);
                                    self.entries.extend(node.entries);
                                    btree.on_page_deleted(node.page_nr);
                                    deleted_page = Some(node.page_nr);
                                }
                            }
                        }
                        deleted_page
                    }
                };
                Ok(deleted_page)
            }
        }
    }

    fn new(page_nr: u64, keys: &[K], entries: &[PagePtr]) -> Self {
        // let padding = (size - 2 * order * (mem::size_of::<K>() + mem::size_of::<PagePtr>()) - mem::size_of::<PagePtr>()) as u64;
        Internal { page_nr, keys: keys.to_vec(), entries: entries.to_vec() }
    }

    fn is_full(&self, max_key_count: u64) -> bool {
        self.keys.len() >= max_key_count as usize
    }

    // entries has 1 more value then keys
    // take the middle key out, but leave its entry!
    // [k0, k1, k2, k3] -> [k0, k1] | [k3]  split_key == k2
    // [r0, r1, r2, r3, r4] -> [r0, r1, r2] | [r3, r4]
    fn split(&mut self, page_nr: u64, split_at: usize) -> (K, Self) {
        let split_key = self.keys[split_at];
        let node: Internal<K>;
        node = Internal::new(page_nr, &self.keys[split_at + 1..], &self.entries[split_at + 1..]);
        self.keys.drain(split_at..);
        self.entries.drain(split_at + 1..);
        (split_key, node)
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

    fn deserialize_from(fh: &File, page_nr: u64) -> Result<Self> {
        let node = Self { page_nr, keys: bincode::deserialize_from(fh)?, entries: bincode::deserialize_from(fh)? };
        Ok(node)
    }

    pub fn keys(self) -> std::vec::IntoIter<K> {
        self.keys.into_iter()
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

    pub fn get(self, btree: &mut BTree<K, V>, key: K) -> Result<Option<V>> {
        // "self" is the root page!
        match self {
            BTNode::Leaf(node) => return Ok(node.get(&key)),
            BTNode::Internal(node) => {
                let mut page_nr = node.get(&key);
                loop {
                    match btree.load_node(page_nr)? {
                        BTNode::Leaf(node) => return Ok(node.get(&key)),
                        BTNode::Internal(node) => page_nr = node.get(&key),
                    }
                }
            },
        }
    }

    pub fn set(self, btree: &mut BTree<K, V>, key: K, value: V) -> Result<(Option<(K, PagePtr)>, Option<V>)> {
        // "self" is the root page!
        match self {
            BTNode::Internal(node) => node.set(btree, key, value),
            BTNode::Leaf(node) => node.set(btree, key, value),
        }
    }

    pub fn remove(self, btree: &mut BTree<K, V>, key: K) -> Result<Option<V>> {
        // "self" is the root page!
        let (original_value, _) = match self {
            BTNode::Internal(node) => node.remove(btree, key, None, None)?,
            BTNode::Leaf(node) => node.remove(btree, key, None, None)?,
        };
        Ok(original_value)
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

    pub fn len(&self) -> usize {
        match self {
            Self::Internal(node) => node.keys.len(),
            Self::Leaf(node) => node.keys.len(),
        }
    }

    pub fn keys(self) -> std::vec::IntoIter<K> {
        match self {
            Self::Internal(node) => node.keys(),
            Self::Leaf(node) => node.keys(),
        }
    }

    // Only for debugging
    pub fn dump(&self, btree: &mut BTree<K, V>) -> Result<()> {
        // This is the root node
        match self {
            Self::Internal(node) => {
                let mut all_nodes = vec![node.page_nr];
                let mut start = 0;
                let mut end = 1;
                let mut count = 0;
                loop {
                    for i in start..end {
                        let node = btree.load_node(all_nodes[i])?.internal_node();
                        for page_nr in node.entries {
                            match btree.load_node(page_nr)? {
                                Self::Internal(_) => {
                                    count += 1;
                                    all_nodes.push(page_nr);
                                }
                                Self::Leaf(_) => {
                                    break;
                                }
                            }
                        }
                        if count == 0 {
                            break;
                        }
                    }
                    if count > 0 {
                        start = end;
                        end += count;
                        count = 0;
                    } else {
                        break;
                    }
                }
                for page_nr in all_nodes {
                    println!("{:?}", btree.load_node(page_nr)?);
                }
                self.dump_leafs(btree)?;
            }
            Self::Leaf(node) => println!("{:?}", node),
        }
        Ok(())
    }

    // Only for debugging
    pub fn dump_leafs(&self, btree: &mut BTree<K, V>) -> Result<()> {
        let mut page_nr = Some(0);
        while page_nr.is_some() {
            let node = btree.load_node(page_nr.unwrap())?.leaf_node();
            println!("{:?}", node);
            page_nr = node.next;
        }
        Ok(())
    }

    fn leaf_node(self) -> Leaf<K, V> {
        match self {
            BTNode::Internal(_) => panic!("Expected a Leaf, got an Internal"),
            BTNode::Leaf(node) => node,
        }
    }

    fn internal_node(self) -> Internal<K> {
        match self {
            BTNode::Leaf(_) => panic!("Expected an Internal, got a Leaf"),
            BTNode::Internal(node) => node,
        }
    }
}


#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use tempfile::TempDir;

    fn dump_btree(bt: &mut BTree<u128, u128>) -> Result<()> {
        println!("==== BTree");
        println!("{:?}", bt);
        bt.root()?.dump(bt)?;
        println!("====");
        Ok(())
    }

    #[test]
    fn test_set() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut bt: BTree<u128, u128> = BTree::open(temp_dir.path(), Some(4))?;

        // println!("{:?}", bt);

        let root = bt.root()?;
        assert_eq!(root.len(), 0);


        // Fill the first leaf
        for i in 1..=4 {
            bt.set(i, i * 10)?;
        }
        assert_eq!(bt.root()?.len(), 4);

        // First leaf will be split
        bt.set(5, 50)?;
        assert_eq!(bt.root()?.len(), 1);

        // Second leaf will be split
        bt.set(6, 60)?;
        bt.set(7, 70)?;
        assert_eq!(bt.root()?.len(), 2);

        // Fill the root
        for i in 8..=11 {
            bt.set(i, i * 10)?;
        }
        assert_eq!(bt.root()?.len(), 4);

        // Root will be split
        bt.set(12, 120)?;
        bt.set(13, 130)?;
        assert_eq!(bt.root()?.len(), 1);

        Ok(())
    }

    #[test]
    fn test_remove_mkc_4() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut bt: BTree<u128, u128> = BTree::open(temp_dir.path(), Some(4))?;

        // Fill the BTree
        // 7 leaf nodes
        // 2 level 1 internal nodes
        // 1 root node
        //
        //                           [70]
        //                     ...../    \.....
        //                    /                \
        //           [30,  50]                 [90,    110,    130]
        //          /    /   \                /     /      /       \
        //         /    /     \              /     /      /         \
        // [10, 20] [30, 40] [50, 60] [70, 80] [90, 100] [110, 120] [130, 140, 150]
        //
        for i in 1..=15 {
            bt.set(i * 10, i * 100)?;
        }
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 10);
        assert_eq!(bt.len(), 15);

        // Remove 120 (from leaf(6) [110, 120])
        // 130 is transferred from right sibling leaf(9)
        // key 130 is replaced by 140 in internal(7)
        //
        //                           [70]
        //                     ...../    \.....
        //                    /                \
        //           [30,  50]                 [90,    110,    140]
        //          /    /   \                /     /      /       \
        //         /    /     \              /     /      /         \
        // [10, 20] [30, 40] [50, 60] [70, 80] [90, 100] [110, 130] [140, 150]
        //
        assert_eq!(bt.remove(120)?, Some(1200));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 10);
        assert_eq!(bt.len(), 14);

        // Remove 100 (from leaf(5) [90, 100])
        // leaf(5) is merged into leaf(4)
        // leaf(5) is deleted
        // key 90 is removed from internal(7)
        //
        //                           [70]
        //                     ...../    \.........
        //                    /                    \
        //           [30,  50]                     [110,  140]
        //          /    /   \                    /     /    \
        //         /    /     \                  /     /      \
        // [10, 20] [30, 40] [50, 60] [70, 80, 90] [110, 130] [140, 150]
        //
        assert_eq!(bt.remove(100)?, Some(1000));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 9);
        assert_eq!(bt.len(), 13);

        // Remove 110 (from leaf(6) [110, 130])
        // 90 is transferred from left sibling leaf(4)
        // key 110 is replaced by 90 in internal(7)
        //
        //                           [70]
        //                     ...../    \......
        //                    /                 \
        //           [30,  50]                  [90,  140]
        //          /    /   \                 /    /    \
        //         /    /     \               /    /      \
        // [10, 20] [30, 40] [50, 60] [70, 80] [90, 130] [140, 150]
        //
        assert_eq!(bt.remove(110)?, Some(1100));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 9);
        assert_eq!(bt.len(), 12);

        // Remove 30 (from leaf(1) [30, 40])
        // leaf(1) is merged into leaf(0)
        // leaf(1) is deleted
        // key 30 is removed from internal(2)
        // Now also internal nodes are merged:
        // internal(7) is merged into internal(2)
        // This bubbles up to the root:
        // the root is replaced by internal(2)
        // key 70 gets inserted into internal(2)
        //
        //               [50,    70,     90,    140]
        //              /      /      /       /    \
        //             /      /      /       /      \
        // [10, 20, 40] [50, 60] [70, 80] [90, 130] [140, 150]
        //
        assert_eq!(bt.remove(30)?, Some(300));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 6);
        assert_eq!(bt.len(), 11);

        // Remove 20, 40, 50, 60, 80, 90, 130 and 140 so that the root collapses into 1 leaf(0)
        for i in [20_u128, 40, 50, 60, 80, 90, 130, 140].iter() {
            assert_eq!(bt.remove(*i)?, Some(i * 10));
        }
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 1);
        assert_eq!(bt.len(), 3);

        Ok(())
    }

    #[test]
    fn test_remove_mkc_5() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut bt: BTree<u128, u128> = BTree::open(temp_dir.path(), Some(5))?;

        // Fill the BTree
        // 7 leaf nodes
        // 2 level 1 internal nodes
        // 1 root node
        for i in 1..=22 {
            bt.set(i * 10, i * 100)?;
        }
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 10);
        assert_eq!(bt.len(), 22);

        // transfer from right sibling
        assert_eq!(bt.remove(180)?, Some(1800));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 10);
        assert_eq!(bt.len(), 21);

        // left merge
        assert_eq!(bt.remove(100)?, Some(1000));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 7);
        assert_eq!(bt.len(), 20);

        // transfer from left sibling
        assert_eq!(bt.remove(110)?, Some(1100));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 7);
        assert_eq!(bt.len(), 19);

        assert_eq!(bt.remove(30)?, Some(300));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 6);
        assert_eq!(bt.len(), 18);

        for i in [10, 20_u128, 40, 50, 60, 80, 90, 120, 130, 140, 170, 200, 220].iter() {
            assert_eq!(bt.remove(*i)?, Some(i * 10));
        }
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 1);
        assert_eq!(bt.len(), 5);

        Ok(())
    }

    #[test]
    fn test_remove_mkc_3() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut bt: BTree<u128, u128> = BTree::open(temp_dir.path(), Some(3))?;

        // Fill the BTree
        // 1 root node
        // 2 level 1 internal nodes
        // 5 level 2 internal nodes
        // 14 leaf nodes
        for i in 1..=29 {
            bt.set(i, i * 10)?;
        }
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 22);
        assert_eq!(bt.len(), 29);

        // transfer from right sibling
        assert_eq!(bt.remove(28)?, Some(280));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 22);
        assert_eq!(bt.len(), 28);

        // left merge
        assert_eq!(bt.remove(6)?, Some(60));
        dump_btree(&mut bt)?;
        assert_eq!(bt.get(7)?, Some(70));
        assert_eq!(bt.node_count, 18);
        assert_eq!(bt.len(), 27);

        // transfer from left sibling
        assert_eq!(bt.remove(7)?, Some(70));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 18);
        assert_eq!(bt.len(), 26);

        assert_eq!(bt.remove(5)?, Some(50));
        assert_eq!(bt.remove(8)?, Some(80));
        assert_eq!(bt.remove(27)?, Some(270));
        assert_eq!(bt.remove(29)?, Some(290));
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 15);
        assert_eq!(bt.len(), 22);

        for i in [1, 3, 9, 11, 13, 15, 17, 19, 21, 23, 25].iter() {
            assert_eq!(bt.remove(*i)?, Some(i * 10));
            assert_eq!(bt.get(*i + 1)?, Some((i + 1) * 10));
        }
        dump_btree(&mut bt)?;
        assert_eq!(bt.node_count, 6);
        assert_eq!(bt.len(), 11);

        Ok(())
    }
}
