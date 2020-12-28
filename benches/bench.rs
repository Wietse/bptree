use bptree::{BTree, Result};
use tempfile::TempDir;
use csv;
use criterion::{criterion_group, criterion_main, Criterion};
use serde::Deserialize;
use std::{
    path::{Path,PathBuf},
    fs::File,
};


#[derive(Debug, Deserialize)]
struct Record {
    key: u128,
    value: u128,
}


fn create_btree() -> Result<(TempDir, BTree<u128, u128>)> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let btree: BTree<u128, u128> = BTree::open(temp_dir.path(), None)?;
    Ok((temp_dir, btree))
}


fn fill_btree(btree: &mut BTree<u128, u128>, fname: &Path) -> Result<()> {
    // Build the CSV reader and iterate over each record.
    let fh = File::open(fname)?;
    let mut rdr = csv::Reader::from_reader(fh);
    for result in rdr.deserialize() {
        let record: Record = result.unwrap();
        btree.set(record.key, record.value)?;
    }
    Ok(())
}


fn get_from_btree(btree: &mut BTree<u128, u128>, pairs: &Vec<Record>) -> Result<()> {
    for p in pairs {
        assert_eq!(btree.get(p.key)?, Some(p.value));
    }
    Ok(())
}


fn remove_from_btree(btree: &mut BTree<u128, u128>, pairs: &Vec<Record>) -> Result<()> {
    for p in pairs {
        assert_eq!(btree.remove(p.key)?, Some(p.value));
    }
    Ok(())
}


fn get_pair_sample(n: usize, fname: &Path) -> Result<Vec<Record>> {
    // Build the CSV reader and iterate over each record.
    let fh = File::open(fname)?;
    let mut rdr = csv::Reader::from_reader(fh);
    let mut i = 0;
    let mut result = Vec::<Record>::new();
    for r in rdr.deserialize() {
        if i >= n { break; }
        let record: Record = r.unwrap();
        result.push(record);
        i += 1;
    }
    Ok(result)
}


fn criterion_benchmark(c: &mut Criterion) {
    let (_temp_dir, mut btree) = create_btree().unwrap();
    let mut group = c.benchmark_group("sample-size-20");
    let path = PathBuf::from("benches/dataset1000.csv");
    // Configure Criterion.rs to increase sample size
    group.sample_size(10);
    group.bench_function("fill btree 1000", |b| b.iter(|| fill_btree(&mut btree, &path).unwrap()));

    assert_eq!(btree.len(), 1000);
    assert_eq!(btree.get(278670620785117865706424567175710324253_u128).unwrap(), Some(97642519388707873205867907648170780481_u128));

    let pairs = get_pair_sample(10, &path).unwrap();
    group.bench_function("get 10 values from btree 1000", |b| b.iter(|| get_from_btree(&mut btree, &pairs).unwrap()));
    // group.bench_function("remove 10 keys from btree 1000", |b| b.iter(|| remove_from_btree(&mut btree, &pairs).unwrap()));
    group.finish();
}


criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
