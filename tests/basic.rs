extern crate env_logger;
extern crate sanakirja_facade;
use sanakirja_facade::*;

#[test]
fn insert_commit_read() {
    let env = Env::open_anonymous(4096 * 20).unwrap();
    let txn = env.writer().unwrap();
    {
        let mut db = txn.create_root_db(0).unwrap();
        db.insert(&1u64, &77u64).unwrap();

        //let mut db: WriteDb<u64, LargeBuf> = txn.create_db();
        //db.insert(&2u64, &b"abcdefg"[..]).unwrap();
    }
    txn.commit().unwrap();

    let txn = env.reader().unwrap();
    let db: Db<u64, u64> = txn.root_db(0).unwrap();
    println!("{:?}", db.get(&1));
    println!("{:?}", db.get(&2));
}

#[test]
fn insert_snapshot_read() {
    let env = Env::open_anonymous(4096 * 20).unwrap();

    let write_txn = env.writer().unwrap();
    {
        let mut db = write_txn.create_root_db(0).unwrap();
        db.insert(&1u64, &77u64).unwrap();
    }

    let read_txn = write_txn.snapshot();
    let read_db: Db<u64, u64> = read_txn.root_db(0).unwrap();
    println!("{:?}", read_db.get(&1));
    println!("{:?}", read_db.get(&2));
}

#[test]
fn open_db_commit_write_snapshot_read() {
    let env = Env::open_anonymous(4096 * 10).unwrap();
    {
        let w = env.writer().unwrap();
        w.create_root_db::<u64, u64>(0).unwrap();
        w.commit().unwrap();
    }
    let writer = env.writer().unwrap();
    {
        let mut write_db = writer.root_db::<u64, u64>(0).unwrap();
        write_db.insert(&1, &23).unwrap();
    }

    let reader = writer.snapshot();
    let read_db = reader.root_db::<u64, u64>(0).unwrap();
    assert!(read_db.get(&1) == Some(23));
}

#[test]
fn borrow_root_twice() {
    let env = Env::open_anonymous(4096 * 10).unwrap();
    let w = env.writer().unwrap();
    {
        // These can co-exist, since they're at different indices.
        let mut db = w.create_root_db::<u64, u64>(0).unwrap();
        let mut db1 = w.create_root_db::<u64, u64>(1).unwrap();
        db.insert(&1, &2).unwrap();
        db1.insert(&5, &6).unwrap();
    }
    {
        let mut db = w.root_db::<u64, u64>(0).unwrap();
        db.insert(&3, &4).unwrap();
    }
    {
        let mut db = w.root_db::<u64, u64>(0).unwrap();
        db.insert(&7, &8).unwrap();
    }
}

#[test]
#[should_panic]
fn borrow_root_twice_simultaneously() {
    let env = Env::open_anonymous(4096 * 10).unwrap();
    let w = env.writer().unwrap();
    let mut db = w.create_root_db::<u64, u64>(0).unwrap();
    db.insert(&1, &2).unwrap();
    w.root_db::<u64, u64>(0);
}

#[test]
#[should_panic]
fn create_overwriting_borrowed_root() {
    let env = Env::open_anonymous(4096 * 10).unwrap();
    let w = env.writer().unwrap();
    {
        w.create_root_db::<u64, u64>(0).unwrap();
    }
    let db = w.root_db::<u64, u64>(0);
    let _ = w.create_root_db::<u64, u64>(0);
    let _ = db; // just to shut up the unused variable warning
}

