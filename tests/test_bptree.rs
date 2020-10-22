use std::fs;
use std::path::Path;
use bptree::BTree;


#[test]
fn test1() {
    let fname = "tmp/test1";
    if Path::new(fname).is_dir() {
        fs::remove_dir_all(fname).unwrap();
    }
    let mut bt = BTree::open(fname).unwrap();
    assert_eq!(true, bt.is_empty());
    bt.insert(1, 10).unwrap();
    assert_eq!(1, bt.len());
    assert_eq!(10, bt.get(1).unwrap());
    bt.close().unwrap();
}


fn range_test(n: u64) {
    let fname = format!("tmp/test{}", n);
    if Path::new(&fname).is_dir() {
        fs::remove_dir_all(&fname).unwrap();
    }
    let mut bt = BTree::open(&fname).unwrap();
    for i in 0..n {
        bt.insert(i, i*10).unwrap();
    }
    assert_eq!(n, bt.len() as u64);
    for i in 0..n {
        match bt.get(i) {
            Some(v) => assert_eq!(i*10, v, "Unexpected value {} for key {}", v, i),
            None => assert!(false, "Key {} NOT FOUND", i),
        }
    }
    bt.close().unwrap();

    bt = BTree::open(&fname).unwrap();
    assert_eq!(n, bt.len() as u64);
    for i in 0..n {
        match bt.get(i) {
            Some(v) => assert_eq!(i*10, v, "Unexpected value {} for key {}", v, i),
            None => assert!(false, "Key {} NOT FOUND", i),
        }
    }
    bt.close().unwrap();
}


#[test]
fn test2() {
    range_test(10);
}


#[test]
fn test3() {
    range_test(1000);
}
