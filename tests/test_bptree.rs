#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

// use assert_cmd::prelude::*;
use bptree::{BTree, Result};
// use predicates::ord::eq;
// use predicates::str::{contains, is_empty, PredicateStrExt};
// use std::process::Command;
use tempfile::TempDir;
// use walkdir::WalkDir;


// Should get previously stored value.
#[test]
fn get_stored_value() -> Result<()> {
    // let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    // let mut store = BTree::open(temp_dir.path())?;
    let mut store = BTree::open("temp")?;
    println!("{:?}", store);

    store.insert(1, 1000)?;
    store.insert(2, 2000)?;

    assert_eq!(store.get(1)?, Some(1000));
    assert_eq!(store.get(2)?, Some(2000));

    // Open from disk again and check persistent data.
    drop(store);
    // let mut store = BTree::open(temp_dir.path())?;
    let mut store = BTree::open("temp")?;
    assert_eq!(store.get(1)?, Some(1000));
    assert_eq!(store.get(2)?, Some(2000));

    Ok(())
}

// Should get previously stored value.
#[test]
fn get_stored_value_from_multiple_pages() -> Result<()> {
    // let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    // let mut store = BTree::open(temp_dir.path())?;
    let mut store = BTree::open("temp2")?;
    println!("{:?}", store);

    let n = 10000;

    for i in 1..n {
        store.insert(i, i*10)?;
    }
    assert_eq!(n-1, store.len() as u64);
    for i in 1..n {
        match store.get(i)? {
            Some(v) => assert_eq!(i*10, v, "Unexpected value {} for key {}", v, i),
            None => assert!(false, "Key {} NOT FOUND", i),
        }
    }

    drop(store);

    store = BTree::open("temp2")?;
    assert_eq!(n-1, store.len() as u64);
    for i in 1..n {
        match store.get(i)? {
            Some(v) => assert_eq!(i*10, v, "Unexpected value {} for key {}", v, i),
            None => assert!(false, "Key {} NOT FOUND", i),
        }
    }

    Ok(())
}
