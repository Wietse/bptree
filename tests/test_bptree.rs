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


#[test]
fn test2() {
    let fname = "tmp/test2";
    if Path::new(fname).is_dir() {
        fs::remove_dir_all(fname).unwrap();
    }
    let mut bt = BTree::open(fname).unwrap();
    for i in 0..100 {
        bt.insert(i, i*10).unwrap();
    }
    assert_eq!(100, bt.len());
    assert_eq!(50, bt.get(5).unwrap());
    bt.close().unwrap();

    bt = BTree::open(fname).unwrap();
    assert_eq!(100, bt.len());
    assert_eq!(700, bt.get(70).unwrap());
    assert_eq!(320, bt.get(32).unwrap());
    bt.close().unwrap();
}


#[test]
fn test3() {
    let fname = "tmp/test3";
    if Path::new(fname).is_dir() {
        fs::remove_dir_all(fname).unwrap();
    }
    let mut bt = BTree::open(fname).unwrap();
    for i in 0..200 {
        bt.insert(i, i*10).unwrap();
    }
    assert_eq!(200, bt.len());
    assert_eq!(50, bt.get(5).unwrap());
    bt.close().unwrap();

    bt = BTree::open(fname).unwrap();
    assert_eq!(100, bt.len());
    assert_eq!(700, bt.get(70).unwrap());
    assert_eq!(320, bt.get(32).unwrap());
    bt.close().unwrap();
}
