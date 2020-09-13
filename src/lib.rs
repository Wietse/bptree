mod store;


struct Entry {
    value: u64
}


impl Entry {

    fn new(value: u64) -> Entry {
        Entry { value }
    }
}


impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}


impl Eq for Entry {}


struct Node {
    page_nr: u32,
    is_leaf: bool,
    keys: Vec<u64>,
    entries: Vec<Entry>,
}


impl Node {

    fn new(page_nr: u32, is_leaf: bool, keys: Option<Vec<u64>>, entries: Option<Vec<Entry>>) -> Node {
        let keys_ = match keys {
            Some(x) => x,
            None => Vec::new(),
        };
        let entries_ = match entries {
            Some(x) => x,
            None => Vec::new(),
        };
        Node { page_nr, is_leaf, keys: keys_, entries: entries_ }
    }

}








#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
