#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

// use assert_cmd::prelude::*;
use bptree::{BTNode, BTree, Result};
// use predicates::ord::eq;
// use predicates::str::{contains, is_empty, PredicateStrExt};
// use std::process::Command;
use tempfile::TempDir;
// use walkdir::WalkDir;


fn dump_btree(bt: &mut BTree<u128, u128>) -> Result<()> {
    println!("==== BTree");
    bt.dump()?;
    println!("====");
    Ok(())
}


// Should get previously stored value.
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path(), None)?;

    btree.set(1, 1000)?;
    btree.set(2, 2000)?;

    assert_eq!(btree.get(1)?, Some(1000));
    assert_eq!(btree.get(2)?, Some(2000));

    // Open from disk again and check persistent data.
    drop(btree);
    let mut btree = BTree::open(temp_dir.path(), None)?;
    assert_eq!(btree.get(1)?, Some(1000));
    assert_eq!(btree.get(2)?, Some(2000));

    Ok(())
}


// Should get previously stored value.
#[test]
fn get_stored_value_from_multiple_pages() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path(), None)?;

    let n = 1025_u128;

    for i in 1..n {
        btree.set(i, i * 10)?;
    }
    assert_eq!(n - 1, btree.len() as u128);
    for i in 1..n {
        assert_eq!(btree.get(i)?, Some(i * 10));
    }

    // Open from disk again and check persistent data.
    drop(btree);
    let mut btree = BTree::open(temp_dir.path(), None)?;
    assert_eq!(n - 1, btree.len() as u128);
    for i in 1..n {
        assert_eq!(btree.get(i)?, Some(i * 10));
    }

    Ok(())
}


#[test]
fn check_next_page_pointer() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path(), None)?;

    let n = 1025_u128;

    for i in 1..n {
        btree.set(i, i * 10)?;
    }
    assert_eq!(n - 1, btree.len() as u128);
    assert!(btree.keys().zip(1..n).all(|(i, j)| i == j));
    assert!(btree.values().zip(1..n).all(|(i, j)| i == j * 10));

    Ok(())
}


#[test]
fn remove_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path(), None)?;

    btree.set(1, 1000)?;
    btree.set(2, 2000)?;

    assert_eq!(btree.get(1)?, Some(1000));
    assert_eq!(btree.get(2)?, Some(2000));

    assert_eq!(btree.len(), 2);
    assert_eq!(btree.remove(1)?, Some(1000));
    assert_eq!(btree.len(), 1);

    // Open from disk again and check persistent data.
    drop(btree);
    let mut btree = BTree::open(temp_dir.path(), None)?;
    assert_eq!(btree.get(1)?, None);
    assert_eq!(btree.get(2)?, Some(2000));

    Ok(())
}


#[test]
fn remove_stored_value_from_multiple_pages() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path(), None)?;

    let n = 1025_u128;

    for i in 1..n {
        btree.set(i, i * 10)?;
    }
    assert_eq!(n - 1, btree.len() as u128);
    for i in 1..n {
        assert_eq!(btree.get(i)?, Some(i * 10));
    }

    let start = n / 4;
    let end = start * 3;
    let mut count = 0;
    for i in start..=end {
        match btree.remove(i)? {
            Some(v) => { assert_eq!(v, i*10); },
            None => {
                dump_btree(&mut btree)?;
                panic!("Expected value {:?}, got None", Some(i*10));
            }
        }
        // assert_eq!(btree.remove(i)?, Some(i * 10));
        count += 1;
    }
    assert_eq!((n - 1) - count, btree.len() as u128, "{:?}", btree);

    // Open from disk again and check persistent data.
    drop(btree);
    let mut btree = BTree::open(temp_dir.path(), None)?;
    assert_eq!((n - 1) - count, btree.len() as u128, "{:?}", btree);
    for i in 1..start {
        assert_eq!(btree.get(i)?, Some(i * 10));
    }
    for i in start..=end {
        assert_eq!(btree.get(i)?, None);
    }
    for i in (end+1)..n {
        assert_eq!(btree.get(i)?, Some(i * 10));
    }

    Ok(())
}
