extern crate sanakirja_facade;
use sanakirja_facade::*;

#[test]
fn insert_commit_read() {
    let mut env = Env::memory_backed(4096 * 20).unwrap();
    {
        let txn = env.begin_mut_txn().unwrap();
        {
            let mut db = txn.create_db();
            db.insert(1u64, 77u64).unwrap();
            txn.set_root_db(0, db);
        }
        txn.commit().unwrap();
    }

    let txn = env.begin_txn().unwrap();
    let db: Db<u64, u64> = txn.root_db(0);
    println!("{:?}", db.get(&1));
    println!("{:?}", db.get(&2));
}

