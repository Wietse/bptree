mod store;

use std::{io, fs};
use bytes::{Buf, BytesMut, BufMut};
use num_integer::Integer;
use store::{PAGE_SIZE, MemPage, Store};


type KeyType = u64;
type ValueType = u64;


const BT_ORDER: u16 = 84;
const BT_MAX_KEY_COUNT: u16 = 2 * BT_ORDER - 1;


pub struct Node {
    page_nr: usize,
    is_leaf: bool,
    keys: Vec<u64>,
    entries: Vec<u64>,
}


impl Node {

    fn new(page_nr: usize, is_leaf: bool, keys: &[u64], entries: &[u64]) -> Node {
        let mut node = Node {
            page_nr,
            is_leaf,
            keys: Vec::new(),
            entries: Vec::new(),
        };
        node.keys.extend_from_slice(keys);
        node.entries.extend_from_slice(entries);
        node
    }

    fn is_full(&self) -> bool {
        self.keys.len() >= BT_MAX_KEY_COUNT as usize
    }

    fn split(&mut self, page_nr: usize) -> (KeyType, Self) {
        let m = BT_MAX_KEY_COUNT.div_ceil(&2) as usize;
        let split_key = self.keys[m];
        let node: Node;
        if self.is_leaf {
            // keys and entries have same length
            // [k0, k1, k2, k3] -> [k0, k1] | [k2, k3]  split_key == k2
            // [v0, v1, v2, v3] -> [v0, v1] | [v2, v3]
            node = Node::new(page_nr, self.is_leaf, &self.keys[m..], &self.entries[m..]);
            self.keys.drain(..m);
            self.entries.drain(..m);
        } else {
            // take the middle key out, but leave its entry!
            // [k0, k1, k2, k3] -> [k0, k1] | [k3]  split_key == k2
            // [r0, r1, r2, r3, r4] -> [r0, r1, r2] | [r3, r4]
            node = Node::new(page_nr, self.is_leaf, &self.keys[m+1..], &self.entries[m+1..]);
            self.keys.drain(..m);
            self.entries.drain(..m+1);
        }
        (split_key, node)
    }

    fn serialize(&self) -> MemPage {
        let mut buf = BytesMut::with_capacity(PAGE_SIZE as usize);
        buf.put_u8(self.is_leaf as u8);
        buf.put_u64(self.keys.len() as u64);
        for key in &self.keys {
            buf.put_u64(*key);
        }
        buf.put_u64(self.entries.len() as u64);
        for entry in &self.entries {
            buf.put_u64(*entry);
        }
        for _ in 0..buf.remaining_mut() {
            buf.put_u8(0);
        }
        MemPage { page_nr: 0, data: buf.freeze() }
    }

    fn deserialize(page: &mut MemPage) -> io::Result<Self> {
        let mut node = Self {
            page_nr: page.page_nr,
            is_leaf: page.data.get_u8() == 1,
            keys: Vec::new(),
            entries: Vec::new(),
        };
        let key_len = page.data.get_u64() as usize;
        node.keys.reserve(key_len);
        for _ in 0..key_len {
            node.keys.push(page.data.get_u64());
        }
        let entries_len = page.data.get_u64() as usize;
        node.entries.reserve(entries_len);
        for _ in 0..entries_len {
            node.entries.push(page.data.get_u64());
        }
        Ok(node)
    }

}


struct BTree {
    dir_path: String,
    store: Store,
}


impl BTree {

    pub fn open(dir_path: &str) -> io::Result<Self> {
        fs::create_dir_all(dir_path)?;
        let fname = format!("{}/btree", dir_path);
        let mut bt = BTree { dir_path: dir_path.to_string(), store: Store::new(&fname) };
        bt.store.open()?;
        Ok(bt)
    }

    pub fn close(&mut self) {
        self.store.close()
    }

    fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    fn root_page_nr(&self) -> usize {
        self.store.root_page_nr()
    }

    fn load_node(&mut self, page_nr: usize) -> io::Result<Node> {
        Node::deserialize(&mut self.store.read_page(page_nr)?)
    }

    fn store_node(&mut self, node: &Node) -> io::Result<()> {
        self.store.write_page(&node.serialize())
    }

    fn insert_recursive(&mut self, page_nr: usize, key: KeyType, value: ValueType) -> Result<Option<(KeyType, Node)>, ValueType> {
        let mut node = self.load_node(page_nr).unwrap();
        let position = node.keys.binary_search(&key);
        if node.is_leaf {
            match position {
                Err(i) => {
                    node.keys.insert(i, key);
                    node.entries.insert(i, value);
                },
                Ok(i) => {
                    let result = Err(node.entries[i]);
                    node.entries[i] = value;
                    return result;
                }
            }
        } else {
            let i = position.unwrap();
            if let Ok(Some((split_key, split_node))) = self.insert_recursive(node.entries[i] as usize, key, value) {
                self.store_node(&split_node).unwrap();
                let i = node.keys.binary_search(&split_key).unwrap();
                node.keys.insert(i, split_key);
                node.entries.insert(i+1, split_node.page_nr as u64);
            }
        }
        let result;
        if node.is_full() {
            result = Some(node.split(self.store.next_page().unwrap()));
        } else {
            result = None;
        }
        self.store_node(&node).unwrap();
        Ok(result)
    }

    pub fn insert(&mut self, key: KeyType, value: ValueType) -> Result<(), ValueType> {
        if self.is_empty() {
            let root = Node::new(self.root_page_nr(), true, &[key], &[value]);
            self.store_node(&root).unwrap();
            self.store.next_page().unwrap();
            return Ok(());
        }
        match self.insert_recursive(self.root_page_nr(), key, value) {
            Ok(Some((split_key, split_node))) => {
                self.store_node(&split_node).unwrap();
                let new_root = Node::new(
                    self.store.next_page().unwrap(),
                    false,
                    &[split_key],
                    &[self.root_page_nr() as u64, split_node.page_nr as u64]
                    );
                self.store_node(&new_root).unwrap();
                self.store.set_root_page_nr(new_root.page_nr);
                Ok(())
            },
            Ok(None) => Ok(()),
            Err(v) => Err(v)
        }
    }

    pub fn get(&mut self, key: KeyType) -> Option<ValueType> {
        if self.is_empty() {
            return None;
        }
        let mut node = self.load_node(self.root_page_nr()).unwrap();
        while !node.is_leaf {
            match node.keys.binary_search(&key) {
                Ok(i) | Err(i) => {
                    node = self.load_node(node.entries[i] as usize).unwrap();
                }
            }
        }
        match node.keys.binary_search(&key) {
            Ok(i) => Some(node.entries[i]),
            Err(_) => None
        }
    }

}
