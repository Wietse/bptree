use std::{
    io::{self, Write, Seek, SeekFrom},
    fs::{self, OpenOptions, File},
    path::Path,
};
use serde::{Serialize, Deserialize};
use bincode::{deserialize, serialize, deserialize_from, serialize_into};
use num_integer::Integer;


type KeyType = u64;
type ValueType = u64;


const BT_ORDER: u16 = 84;
const BT_MAX_KEY_COUNT: u16 = 2 * BT_ORDER - 1;
pub const PAGE_SIZE: usize = 4096;
const MAGIC_HEADER: &str = "%bptree%";


#[derive(Debug)]
pub struct Pages {
    fh: Option<File>,
}


impl Pages {

    fn new() -> Self {
        Pages { fh: None }
    }

    fn is_open(&self) -> bool {
        match self.fh {
            Some(_) => true,
            None => false,
        }
    }

    fn open(&mut self, file_path: &str) -> io::Result<bool> {
        if !self.is_open() {
            let is_new_file = !Path::new(file_path).exists();
            let fh = OpenOptions::new().read(true).write(true).create(is_new_file).open(file_path)?;
            self.fh = Some(fh);
            return Ok(is_new_file);
        }
        Ok(false)
    }

    fn close(&mut self) -> io::Result<()> {
        let fh = self.fh.as_mut().unwrap();
        fh.sync_all()?;
        self.fh = None;
        Ok(())
    }

    fn write_header(&mut self, header: &Header) -> Result<(), std::boxed::Box<bincode::ErrorKind>> {
        println!("write_header: {:?}", header);
        let mut fh = self.fh.as_mut().unwrap();
        fh.seek(SeekFrom::Start(0))?;
        let result = serialize_into(&mut fh, header);
        let pos = fh.seek(SeekFrom::Current(0)).unwrap() as usize;
        assert!(pos < PAGE_SIZE, "Header wrote {} bytes", pos);
        let padding = PAGE_SIZE - pos;
        if padding > 0 {
            fh.write(&vec![0u8; padding]).unwrap();
        }
        return result;
    }

    fn read_header(&mut self) -> Result<Header, std::boxed::Box<bincode::ErrorKind>> {
        let fh = self.fh.as_mut().unwrap();
        fh.seek(SeekFrom::Start(0))?;
        let header: Header = deserialize_from(fh)?;
        println!("read_header: {:?}", header);
        Ok(header)
    }

    fn write_node(&mut self, node: &Node) -> Result<(), std::boxed::Box<bincode::ErrorKind>> {
        let mut fh = self.fh.as_mut().unwrap();
        let offset = PAGE_SIZE * node.page_nr;
        fh.seek(SeekFrom::Start(offset as u64))?;
        let result = serialize_into(&mut fh, node);
        let pos = fh.seek(SeekFrom::Current(0)).unwrap() as usize;
        assert!(pos < offset + PAGE_SIZE, "pos = {}, offset+PAGE_SIZE = {}", pos, offset+PAGE_SIZE);
        let padding = offset + PAGE_SIZE - pos;
        if padding > 0 {
            fh.write(&vec![0u8; padding]).unwrap();
        }
        return result;
    }

    fn read_node(&mut self, page_nr: usize) -> Result<Node, std::boxed::Box<bincode::ErrorKind>> {
        let fh = self.fh.as_mut().unwrap();
        let offset = (PAGE_SIZE * page_nr) as u64;
        fh.seek(SeekFrom::Start(offset))?;
        let node: Node = deserialize_from(fh)?;
        Ok(node)
    }

}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Header {
    magic_header: String,
    page_count: usize,
    root_page_nr: usize,
    leaf_count: usize,
}


impl Header {
    pub fn new() -> Header {
        Header {
            magic_header: MAGIC_HEADER.to_string(),
            page_count: 0,
            root_page_nr: 0,
            leaf_count: 0
        }
    }
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Node {
    page_nr: usize,
    is_leaf: bool,
    keys: Vec<KeyType>,
    entries: Vec<ValueType>,
}


impl Node {

    fn new(page_nr: usize, is_leaf: bool, keys: &[KeyType], entries: &[ValueType]) -> Node {
        assert!(page_nr > 0);
        let mut node = Node {
            page_nr,
            is_leaf,
            keys: Vec::new(),
            entries: Vec::new(),
        };
        node.keys.extend_from_slice(keys);
        node.entries.extend_from_slice(entries);
        return node;
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

}


#[derive(Debug)]
pub struct BTree {
    dir_path: String,
    header: Header,
    store: Pages,
}


impl BTree {

    pub fn open(dir_path: &str) -> io::Result<Self> {
        fs::create_dir_all(dir_path)?;
        let fname = format!("{}/btree", dir_path);
        let mut bt = BTree {
            dir_path: dir_path.to_string(),
            header: Header::new(),
            store: Pages::new(),
        };
        let is_new_file = bt.store.open(&fname)?;
        match is_new_file {
            false => bt.load_header(),
            true => bt.init_header(),
        }
        Ok(bt)
    }

    fn init_header(&mut self) {
        self.next_page().unwrap();
    }

    fn load_header(&mut self) {
        self.header = self.store.read_header().unwrap()
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.store.write_header(&self.header).unwrap();
        self.store.close()
    }

    pub fn is_empty(&self) -> bool {
        self.header.leaf_count == 0
    }

    pub fn len(&self) -> usize {
        self.header.leaf_count
    }

    pub fn next_page(&mut self) -> io::Result<usize> {
        let next_page_nr = self.header.page_count;
        self.header.page_count += 1;
        self.store.write_header(&self.header).unwrap();
        Ok(next_page_nr)
    }

    pub fn insert(&mut self, key: KeyType, value: ValueType) -> Result<(), ValueType> {
        if self.is_empty() {
            self.header.root_page_nr = 1;
            let root = Node::new(self.header.root_page_nr, true, &[key], &[value]);
            self.header.leaf_count += 1;
            self.store.write_node(&root).unwrap();
            self.next_page().unwrap();
            return Ok(());
        }
        match self.insert_recursive(self.header.root_page_nr, key, value) {
            Ok(Some((split_key, split_node))) => {
                self.store.write_node(&split_node).unwrap();
                let new_root = Node::new(
                    self.next_page().unwrap(),
                    false,
                    &[split_key],
                    &[self.header.root_page_nr as u64, split_node.page_nr as u64]
                    );
                self.store.write_node(&new_root).unwrap();
                self.header.root_page_nr = new_root.page_nr;
                self.store.write_header(&self.header).unwrap();
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
        let mut node = self.store.read_node(self.header.root_page_nr).unwrap();
        while !node.is_leaf {
            match node.keys.binary_search(&key) {
                Ok(i) | Err(i) => {
                    node = self.store.read_node(node.entries[i] as usize).unwrap();
                }
            }
        }
        match node.keys.binary_search(&key) {
            Ok(i) => Some(node.entries[i]),
            Err(_) => None
        }
    }

    fn insert_recursive(&mut self, page_nr: usize, key: KeyType, value: ValueType) -> Result<Option<(KeyType, Node)>, ValueType> {
        let mut node = self.store.read_node(page_nr).unwrap();
        let position = node.keys.binary_search(&key);
        if node.is_leaf {
            match position {
                Err(i) => {
                    // The key was not found -> new entry.
                    node.keys.insert(i, key);
                    node.entries.insert(i, value);
                    self.header.leaf_count += 1;
                },
                Ok(i) => {
                    // The key was found -> we assign the new value, but return an error with the
                    // old value.
                    let result = Err(node.entries[i]);
                    node.entries[i] = value;
                    return result;
                }
            }
        } else {
            let i = match position { Ok(i) | Err(i) => i };
            // let idx;
            // match position {
            //     Ok(i) | Err(i) => idx = i,
            // }
            if let Ok(Some((split_key, split_node))) = self.insert_recursive(node.entries[i] as usize, key, value) {
                self.store.write_node(&split_node).unwrap();
                let i = match node.keys.binary_search(&split_key) { Ok(i) | Err(i) => i };
                // let i = node.keys.binary_search(&split_key).unwrap();
                node.keys.insert(i, split_key);
                node.entries.insert(i+1, split_node.page_nr as u64);
            }
        }
        let result;
        if node.is_full() {
            result = Some(node.split(self.next_page().unwrap()));
        } else {
            result = None;
        }
        self.store.write_node(&node).unwrap();
        println!("insert_recursive: {:?}", self.header);
        Ok(result)
    }

}
