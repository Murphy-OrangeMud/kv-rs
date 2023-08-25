use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{
    KvServer, KvStore, KvsEngine, RayonThreadPool, SharedQueueThreadPool, SledStore, ThreadPool,
};
use rand::prelude::*;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(mut store, _temp_dir)| {
                for i in 1..(1 << 12) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (SledStore::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(mut db, _temp_dir)| {
                for i in 1..(1 << 12) {
                    db.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for i in &vec![8, 12, 16, 20] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        });
    }
    for i in &vec![8, 12, 16, 20] {
        group.bench_with_input(format!("sled_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut db = SledStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                db.set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                db.get(format!("key{}", rng.gen_range(1, 1 << i))).unwrap();
            })
        });
    }
    group.finish();
}

fn read_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_bench");
    for i in &vec![4, 8, 12, 16, 24, 32] {
        group.bench_with_input(format!("shared_queue_kvs_{i}"), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            let pool = SharedQueueThreadPool::new(*i).unwrap();
            for key_i in 1..16 {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([1; 16]);
            b.iter(|| {
                let n_store = store.clone();
                let n = rng.gen_range(1, 32);
                pool.spawn(move || {
                    n_store.get(format!("key{}", n)).unwrap();
                });
            })
        });
    }

    for i in &vec![4, 8, 12, 16, 24, 32] {
        group.bench_with_input(format!("rayon_kvs_{i}"), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            let pool = RayonThreadPool::new(*i).unwrap();
            for key_i in 1..16 {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([1; 16]);
            b.iter(|| {
                let n_store = store.clone();
                let n = rng.gen_range(1, 32);
                pool.spawn(move || {
                    n_store.get(format!("key{}", n)).unwrap();
                });
            })
        });
    }

    for i in &vec![4, 8, 12, 16, 24, 32] {
        group.bench_with_input(format!("rayon_sled_{i}"), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = SledStore::open(temp_dir.path()).unwrap();
            let pool = RayonThreadPool::new(*i).unwrap();
            for key_i in 1..16 {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([1; 16]);
            b.iter(|| {
                let n_store = store.clone();
                let n = rng.gen_range(1, 32);
                pool.spawn(move || {
                    n_store.get(format!("key{}", n)).unwrap();
                });
            })
        });
    }
}

fn write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_bench");
    for i in &vec![4, 8, 12, 16, 24, 32] {
        group.bench_with_input(format!("shared_queue_kvs_{i}"), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            let pool = SharedQueueThreadPool::new(*i).unwrap();
            let mut rng = SmallRng::from_seed([1; 16]);
            b.iter(|| {
                let n_store = store.clone();
                let n = rng.gen_range(1, 32);
                pool.spawn(move || {
                    n_store
                        .set(format!("key{}", n), format!("value{}", n))
                        .unwrap();
                });
            })
        });
    }

    for i in &vec![4, 8, 12, 16, 24, 32] {
        group.bench_with_input(format!("rayon_kvs_{i}"), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            let pool = RayonThreadPool::new(*i).unwrap();
            let mut rng = SmallRng::from_seed([1; 16]);
            b.iter(|| {
                let n_store = store.clone();
                let n = rng.gen_range(1, 32);
                pool.spawn(move || {
                    n_store
                        .set(format!("key{}", n), format!("value{}", n))
                        .unwrap();
                });
            })
        });
    }

    for i in &vec![4, 8, 12, 16, 24, 32] {
        group.bench_with_input(format!("rayon_sled_{i}"), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = SledStore::open(temp_dir.path()).unwrap();
            let pool = RayonThreadPool::new(*i).unwrap();
            let mut rng = SmallRng::from_seed([1; 16]);
            b.iter(|| {
                let n_store = store.clone();
                let n = rng.gen_range(1, 32);
                pool.spawn(move || {
                    n_store
                        .set(format!("key{}", n), format!("value{}", n))
                        .unwrap();
                });
            })
        });
    }
}

criterion_group!(benches, set_bench, get_bench, read_bench, write_bench);
criterion_main!(benches);
