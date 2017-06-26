extern crate env_logger;
extern crate sanakirja_facade;
use sanakirja_facade::*;

#[test]
fn insert_commit_read() {
    let env = Env::memory_backed(4096 * 20).unwrap();
    let txn = env.writer().unwrap();
    {
        let mut db = txn.create_db();
        db.insert(1u64, 77u64).unwrap();
        txn.set_root_db(0, db);
    }
    txn.commit().unwrap();

    let txn = env.reader().unwrap();
    let db: Db<u64, u64> = txn.root_db(0);
    println!("{:?}", db.get(&1));
    println!("{:?}", db.get(&2));
}

#[test]
fn insert_snapshot_read() {
    let env = Env::memory_backed(4096 * 20).unwrap();

    let write_txn = env.writer().unwrap();
    {
        let mut db = write_txn.create_db();
        db.insert(1u64, 77u64).unwrap();
        write_txn.set_root_db(0, db);
    }

    let read_txn = write_txn.snapshot();
    let read_db: Db<u64, u64> = read_txn.root_db(0);
    println!("{:?}", read_db.get(&1));
    println!("{:?}", read_db.get(&2));
}

#[test]
fn open_db_write_snapshot_read() {
    env_logger::init().unwrap();

    let env = sanakirja_facade::Env::memory_backed(4096 * 10).unwrap();
    {
        let w = env.writer().unwrap();
        {
            let db = w.create_db::<u64, u64>();
            println!("created db {:?}", db);
            w.set_root_db(0, db);
        }
        w.commit().unwrap();
    }
    let writer = env.writer().unwrap();
    let mut write_db = writer.root_db::<u64, u64>(0);
    write_db.insert(1, 23).unwrap();
    writer.set_root_db(0, write_db);

    let reader = writer.snapshot();
    let read_db = reader.root_db::<u64, u64>(0);
    assert!(read_db.get(&1) == Some(23));
}

