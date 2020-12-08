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


// Should get previously stored value.
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path())?;
    println!("{:?}", btree);

    btree.insert(1, 1000)?;
    btree.insert(2, 2000)?;

    assert_eq!(btree.get(1)?, Some(1000));
    assert_eq!(btree.get(2)?, Some(2000));

    // Open from disk again and check persistent data.
    drop(btree);
    let mut btree = BTree::open(temp_dir.path())?;
    assert_eq!(btree.get(1)?, Some(1000));
    assert_eq!(btree.get(2)?, Some(2000));

    Ok(())
}


// Should get previously stored value.
#[test]
fn get_stored_value_from_multiple_pages() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path())?;
    println!("{:?}", btree);

    let n = 1025_u128;

    for i in 1..n {
        btree.insert(i, i * 10)?;
    }
    assert_eq!(n - 1, btree.len() as u128);
    println!("{:?}", btree);
    println!("{:?}", btree.root()?);
    for i in 1..n {
        assert_eq!(btree.get(i)?, Some(i * 10));
    }

    // Open from disk again and check persistent data.
    drop(btree);
    let mut btree = BTree::open(temp_dir.path())?;
    assert_eq!(n - 1, btree.len() as u128);
    for i in 1..n {
        assert_eq!(btree.get(i)?, Some(i * 10));
    }

    Ok(())
}


#[test]
fn check_next_page_pointer() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut btree = BTree::open(temp_dir.path())?;
    println!("{:?}", btree);

    let n = 1025_u128;

    for i in 1..n {
        btree.insert(i, i * 10)?;
    }
    assert_eq!(n - 1, btree.len() as u128);
    assert!(btree.keys().zip(1..n).all(|(i, j)| i == j));
    assert!(btree.values().zip(1..n).all(|(i, j)| i == j * 10));

    Ok(())
}
