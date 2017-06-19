// Copyright 2015 Pierre-Étienne Meunier and Florent Becker. See the
// COPYRIGHT file at the top-level directory of this distribution and
// at http://pijul.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;
use skiplist::SkipListPage;
use std;
use std::collections::HashSet;
use rand::{StdRng, SeedableRng};

#[test]
fn test_put__() {
    extern crate tempdir;
    extern crate rand;
    use rand::Rng;

    extern crate env_logger;
    env_logger::init().unwrap_or(());
    use value::UnsafeValue;
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 4096000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let n = 50;
    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);


    let mut v = Vec::new();

    for i in 0..n {
        println!("======== i = {:?}", i);
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();


        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        println!("put done");
        // debug(&txn, &[&db], format!("/tmp/test_{}", i));

        v.push((k0, v0))
    }

    let (ref a, ref b) = v[n / 2];
    {
        let a = UnsafeValue::from_slice(a.as_bytes());
        let bb = txn.get(&db, a, None).unwrap();
        assert!(unsafe { bb.as_slice() == b.as_bytes() });
    }
    {
        // Check that there are really n different entries.
        let v: Vec<_> = txn.iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n);
        // Check that they're all different (this actually implies the
        // previous check, but debugging is easier with both).
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n);
    }
    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    txn.set_root(0, db);
    txn.commit().unwrap();
    assert_eq!(occupied_pages(&env), h.len() as isize)
}



#[test]
fn test_reviter() {
    extern crate tempdir;
    extern crate rand;
    use rand::Rng;

    extern crate env_logger;
    env_logger::init().unwrap_or(());
    use value::UnsafeValue;
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 4096000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let n = 50;
    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);


    let mut v = Vec::new();

    for i in 0..n {
        println!("======== i = {:?}", i);
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();


        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        println!("put done");
        debug(&txn, &[&db], format!("/tmp/test_{}", i));

        v.push((k0, v0))
    }

    let (k, _) = v[1+v.len()/2].clone();
    println!("k: {:?}", k);
    for i in txn.rev_iter(&db, Some((UnsafeValue::from_slice(k.as_bytes()), None))) {
        println!("{:?}", i)
    }
    {
        // Check that there are really n different entries.
        let v: Vec<_> = txn.rev_iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n);
        // Check that they're all different (this actually implies the
        // previous check, but debugging is easier with both).
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n);
    }
    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    txn.set_root(0, db);
    txn.commit().unwrap();
    assert_eq!(occupied_pages(&env), h.len() as isize)
}





#[test]
fn test_del_fork() {
    extern crate tempdir;
    extern crate rand;
    use rand::Rng;
    use value::UnsafeValue;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 4096000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let n = 10;
    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);


    let mut v = Vec::new();

    for i in 0..n {
        debug!("======== i = {:?}", i);
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();


        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        debug(&txn, &[&db], format!("/tmp/test_{}", i));

        v.push((k0, v0))
    }

    let (ref a, ref b) = v[n / 2];
    {
        let bb = txn.get(&db, UnsafeValue::from_slice(a.as_bytes()), None).unwrap();
        assert!(unsafe { bb.as_slice() == b.as_bytes() });
    }
    {
        // Check that there are really n different entries.
        let v: Vec<_> = txn.iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n);
        // Check that they're all different (this actually implies the
        // previous check, but debugging is easier with both).
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n);
    }

    debug!("============================ Putting blabla/blibli");
    let mut db2 = txn.fork(&mut rng, &db).unwrap();
    {
        let rc_root = Db::<u64, u64>(txn.root_(RC_ROOT), std::marker::PhantomData);
        debug(&txn, &[&rc_root], "/tmp/fork");
    }
    txn.put(&mut rng,
             &mut db2,
             UnsafeValue::from_slice(b"blabla"),
             UnsafeValue::from_slice(b"blibli"))
        .unwrap();
    debug(&txn, &[&db, &db2], "/tmp/test");

    {
        let rc_root = Db::<u64, u64>(txn.root_(RC_ROOT), std::marker::PhantomData);
        debug(&txn, &[&rc_root], "/tmp/rc");
    }

    let idx = v.iter().position(|&(ref x, _)| &x.as_bytes()[0..2] == b"6o").unwrap();

    debug!("============================ DEL");
    txn.del(&mut rng,
             &mut db,
             UnsafeValue::from_slice(v[idx].0.as_bytes()),
             None)
        .unwrap();
    debug!("============================ /DEL");
    debug!("{:?} {:?}", db.0, db2.0);

    {
        let bb = txn.get(&db2, UnsafeValue::from_slice(v[idx].0.as_bytes()), None).unwrap();
        assert!(unsafe { bb.as_slice() == v[idx].1.as_bytes() });

        assert!(txn.get(&db, UnsafeValue::from_slice(v[idx].0.as_bytes()), None).is_none());
    }

    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    let mut h2 = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h2, db2.0);

    let mut h3 = HashSet::new();
    {
        let rc_root = txn.root_(RC_ROOT);
        is_tree::<u64, u64>(&txn, &mut h3, rc_root);
    }
    h.extend(h2.iter());
    h.extend(h3.iter());
    debug!("{:?}", h);

    txn.set_root(0, db);
    txn.set_root(1, db2);
    txn.commit().unwrap();
    assert_eq!(occupied_pages(&env), h.len() as isize)
}


#[test]
fn test_put_fork() {
    extern crate tempdir;
    extern crate rand;
    use rand::Rng;
    use value::UnsafeValue;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 4096000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let n = 20;
    let m = 10;
    let o = 10;
    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);


    let mut v = Vec::new();

    for i in 0..n / 2 {
        debug!("======== i = {:?}", i);
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();


        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        debug(&txn, &[&db], format!("/tmp/test_{}", i));

        v.push((k0, v0))
    }

    txn.set_root(0, db);
    txn.commit().unwrap();
    let mut txn = env.mut_txn_begin().unwrap();
    let mut db: Db<UnsafeValue, UnsafeValue> = txn.root(0).unwrap();

    for i in n / 2..n {
        debug!("======== i = {:?}", i);
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();


        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        debug(&txn, &[&db], format!("/tmp/test_{}", i));

        v.push((k0, v0))
    }

    let mut db2 = txn.fork(&mut rng, &db).unwrap();
    let mut db3 = txn.fork(&mut rng, &db).unwrap();

    let mut w = Vec::new();
    for i in 0..m {
        debug!(">>>>>>>> i = {:?}", i);
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();

        debug!("PUT 2: {:?}", k0);
        txn.put(&mut rng,
                 &mut db2,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        debug(&txn, &[&db, &db2], format!("/tmp/test2_{}", i));

        w.push((k0, v0));
    }

    let mut x = Vec::new();
    for i in 0..o {
        debug!(">>>>>>>> i = {:?}", i);
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();

        debug!("PUT 3: {:?}", k0);
        txn.put(&mut rng,
                 &mut db3,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        debug(&txn, &[&db, &db2, &db3], format!("/tmp/test3_{}", i));

        x.push((k0, v0))
    }


    for &(ref a, ref b) in v.iter() {
        debug!("a = {:?}", a);
        let bb = txn.get(&db, UnsafeValue::from_slice(a.as_bytes()), None).unwrap();
        assert_eq!(std::str::from_utf8(unsafe { bb.as_slice() }).unwrap(), b.as_str());
    }
    for &(ref a, ref b) in w.iter() {
        debug!("a = {:?}", a);
        let bb = txn.get(&db2, UnsafeValue::from_slice(a.as_bytes()), None).unwrap();
        assert_eq!(std::str::from_utf8(unsafe { bb.as_slice() }).unwrap(), b.as_str());
    }
    for &(ref a, ref b) in x.iter() {
        debug!("a = {:?}", a);
        let bb = txn.get(&db3, UnsafeValue::from_slice(a.as_bytes()), None).unwrap();
        assert_eq!(std::str::from_utf8(unsafe { bb.as_slice() }).unwrap(), b.as_str());
    }

    {
        // Check that there are really n different entries in each map.
        let v: Vec<_> = txn.iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n);
        // Check that they're all different (this actually implies the
        // previous check, but debugging is easier with both).
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n);
    }
    {
        // Check that there are really n different entries in each map.
        let v: Vec<_> = txn.iter(&db2, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n + m);
        // Check that they're all different (this actually implies the
        // previous check, but debugging is easier with both).
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n + m);
    }
    {
        // Check that there are really n different entries in each map.
        let v: Vec<_> = txn.iter(&db3, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n + o);
        // Check that they're all different (this actually implies the
        // previous check, but debugging is easier with both).
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n + o);
    }

    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    let mut h2 = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h2, db2.0);

    let mut h3 = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h3, db3.0);

    let mut h_rc = HashSet::new();
    {
        let rc_root = txn.root_(RC_ROOT);
        is_tree::<u64, u64>(&txn, &mut h_rc, rc_root);
    }
    h.extend(h2.iter());
    h.extend(h3.iter());
    h.extend(h_rc.iter());
    debug!("{:?}", h);
    debug!("h_rc = {:?}", h_rc);

    txn.set_root(0, db);
    txn.set_root(1, db2);
    txn.set_root(2, db3);
    txn.commit().unwrap();
    assert_eq!(occupied_pages(&env), h.len() as isize)
}


#[cfg(test)]
fn test_del_leaf(n: usize) {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use rand::{StdRng, Rng, SeedableRng};
    use value::*;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 4096000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let mut v = Vec::new();
    for i in 0..n {
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();


        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        debug(&txn, &[&db], format!("/tmp/test_{}", i));
        v.push((k0, v0));
    }

    for i in 0..1 {
        debug!("delete: {:?}", v[i]);
        txn.del(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(v[i].0.as_bytes()),
                 Some(UnsafeValue::from_slice(v[i].1.as_bytes())))
            .unwrap();
        debug(&txn, &[&db], format!("/tmp/del_{}", i));
    }
    {
        // Check that there are really n entries.
        let v: Vec<_> = txn.iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n - 1);
        // Check that they're all different.
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n - 1);
    }

    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    txn.commit().unwrap();
    assert_eq!(occupied_pages(&env), h.len() as isize)
}


#[cfg(test)]
fn test_del_leaf_fork(n: usize) {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use rand::{StdRng, Rng, SeedableRng};
    use value::*;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 4096000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let mut v = Vec::new();
    for i in 0..n {
        let k0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(250)
            .collect();


        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 UnsafeValue::from_slice(v0.as_bytes()))
            .unwrap();
        debug(&txn, &[&db], format!("/tmp/test_{}", i));
        v.push((k0, v0));
    }

    let mut db2 = txn.fork(&mut rng, &db).unwrap();
    {
        let i = 0;
        let j = 1;
        debug!("delete: {:?}", v[i]);
        debug!("delete: {:?}", v[j]);
        txn.del(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(v[i].0.as_bytes()),
                 Some(UnsafeValue::from_slice(v[i].1.as_bytes())))
            .unwrap();

        debug(&txn, &[&db, &db2], format!("/tmp/test_"));

        txn.del(&mut rng,
                 &mut db2,
                 UnsafeValue::from_slice(v[j].0.as_bytes()),
                 Some(UnsafeValue::from_slice(v[j].1.as_bytes())))
            .unwrap();

        debug(&txn, &[&db, &db2], format!("/tmp/test"));
    }
    {
        // Check that there are really n entries.
        let v: Vec<_> = txn.iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n - 1);
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n - 1);


        let v2: Vec<_> = txn.iter(&db2, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v2.len(), n - 1);
        // Check that they're all different.
        let h2: HashSet<_> = v2.iter().collect();
        assert_eq!(h2.len(), n - 1);
    }

    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    let mut h2 = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h2, db2.0);

    let mut h3 = HashSet::new();
    {
        let rc_root = txn.root_(RC_ROOT);
        is_tree::<u64, u64>(&txn, &mut h3, rc_root);
    }
    h.extend(h2.iter());
    h.extend(h3.iter());

    txn.set_root(0, db);
    txn.set_root(1, db2);
    txn.commit().unwrap();
    debug!("h = {:?}", h);
    assert_eq!(occupied_pages(&env), h.len() as isize)
}


#[test]
fn test_del_leaf_merge() {
    test_del_leaf(8)
}

#[test]
fn test_del_leaf_rebalance() {
    test_del_leaf(20)
}

#[test]
fn test_del_leaf_root() {
    test_del_leaf(4)
}


#[test]
fn test_del_leaf_fork_merge() {
    test_del_leaf_fork(8)
}

#[test]
fn test_del_leaf_fork_rebalance() {
    test_del_leaf_fork(20)
}

#[test]
fn test_del_leaf_fork_root() {
    test_del_leaf_fork(4)
}



#[cfg(test)]
fn test_del_all(n: usize, val_size: usize) {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use rand::{StdRng, Rng, SeedableRng};
    use value::*;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 40960000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let mut v = Vec::new();
    let display = n < 100;
    for i in 0..n {
        let k0: String = rng.gen_ascii_chars()
            .take(500)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(val_size)
            .collect();

        let value = UnsafeValue::alloc_if_needed(&mut txn, v0.as_bytes()).unwrap();
        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 value)
            .unwrap();
        if display {
            debug(&txn, &[&db], format!("/tmp/test_{}", i));
        }
        v.push((k0, v0));
    }

    let db2 = txn.fork(&mut rng, &db).unwrap();
    let mut i = 0;
    if display {
        debug(&txn, &[&db], format!("/tmp/init"));
    }
    for &(ref k0, ref v0) in v.iter() {
        debug!(">>>>>>>>>> Deleting {:?} {:?} <<<<<<<<<<<", i, k0);
        txn.del(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 Some(UnsafeValue::from_slice(v0.as_bytes())))
            .unwrap();
        if display {
            debug(&txn, &[&db], format!("/tmp/del_{}", i));
            debug(&txn, &[&db2, &db], format!("/tmp/del2_{}", i));
            debug(&txn, &[&db2], format!("/tmp/del1_{}", i));
        }
        if display {
            let rc_root: Db<u64, u64> = Db(txn.root_(RC_ROOT), std::marker::PhantomData);
            debug(&txn, &[&rc_root], format!("/tmp/del_rc_{}", i));
            let mut h2 = HashSet::new();
            is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h2, db2.0);
        }

        assert!(txn.get(&db, UnsafeValue::from_slice(k0.as_bytes()), None).is_none());
        i += 1
    }

    if false {
        // Check that there are really n entries.
        let v: Vec<_> = txn.iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n - 1);
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n - 1);


        let v2: Vec<_> = txn.iter(&db2, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v2.len(), n - 1);
        // Check that they're all different.
        let h2: HashSet<_> = v2.iter().collect();
        assert_eq!(h2.len(), n - 1);
    }

    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    if display {
        debug(&txn, &[&db2], format!("/tmp/del"));
    }
    let mut h2 = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h2, db2.0);

    let mut h3 = HashSet::new();
    {
        let rc_root = txn.root_(RC_ROOT);
        is_tree::<u64, u64>(&txn, &mut h3, rc_root);
    }
    h.extend(h2.iter());
    h.extend(h3.iter());

    txn.set_root(0, db);
    txn.set_root(1, db2);
    txn.commit().unwrap();
    debug!("h = {:?}", h);
    {
        let stat = env.statistics();
        for i in 1..stat.total_pages {
            let p = (i * 4096) as u64;
            if !(h.contains(&p) || stat.free_pages.contains(&p) ||
                 stat.bookkeeping_pages.contains(&p)) {

                panic!("Lost track of page {:?}", p);

            }
        }
    }
    assert_eq!(occupied_pages(&env), h.len() as isize)
}

#[test]
fn test_del_all_10_() {
    test_del_all(10, 300)
}

#[test]
fn test_del_all_20_() {
    test_del_all(20, 300)
}

#[test]
fn test_del_all_50_() {
    test_del_all(50, 300)
}

#[test]
fn test_del_all_100_() {
    test_del_all(100, 300)
}

#[test]
fn test_del_all_200_() {
    test_del_all(200, 300)
}

#[test]
fn test_del_all_500_() {
    test_del_all(500, 300)
}

#[test]
fn test_del_all_750_() {
    test_del_all(750, 300)
}

#[test]
fn test_del_all_3000_() {
    test_del_all(3000, 300)
}


#[test]
fn test_del_all_large_10_() {
    test_del_all(10, 4000)
}

#[test]
fn test_del_all_large_30_() {
    test_del_all(30, 4000)
}

#[test]
fn test_del_all_large_3000_() {
    test_del_all(3000, 4000)
}





#[cfg(test)]
fn test_del_nofork_all(n: usize, val_size: usize) {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use rand::{StdRng, Rng, SeedableRng};

    extern crate env_logger;
    env_logger::init().unwrap_or(());
    use value::*;
    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 40960000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let mut v = Vec::new();
    for i in 0..n {
        let k0: String = rng.gen_ascii_chars()
            .take(500)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(val_size)
            .collect();

        let value = UnsafeValue::alloc_if_needed(&mut txn, v0.as_bytes()).unwrap();
        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 value)
            .unwrap();
        debug(&txn, &[&db], format!("/tmp/test_{}", i));
        v.push((k0, v0));
    }
    let display = n < 100;
    let mut i = 0;
    for &(ref k0, ref v0) in v.iter() {
        debug!(">>>>>>>>>> Deleting {:?} {:?} <<<<<<<<<<<", i, k0);
        txn.del(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 Some(UnsafeValue::from_slice(v0.as_bytes())))
            .unwrap();
        if display {
            debug(&txn, &[&db], format!("/tmp/del_{}", i));
        }
        assert!(txn.get(&db, UnsafeValue::from_slice(k0.as_bytes()), None).is_none());
        i += 1
    }

    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);
    if display {
        debug(&txn, &[&db], format!("/tmp/del"));
    }

    let mut h3 = HashSet::new();
    {
        let rc_root = txn.root_(RC_ROOT);
        is_tree::<u64, u64>(&txn, &mut h3, rc_root);
    }
    h.extend(h3.iter());

    txn.set_root(0, db);
    txn.commit().unwrap();
    debug!("h = {:?}", h);
    {
        let stat = env.statistics();
        for i in 1..stat.total_pages {
            let p = (i * 4096) as u64;
            if !(h.contains(&p) || stat.free_pages.contains(&p) ||
                 stat.bookkeeping_pages.contains(&p)) {

                panic!("Lost track of page {:?}", p);

            }
        }
    }
    assert_eq!(occupied_pages(&env), h.len() as isize)
}

#[test]
fn test_del_really_large_10_() {
    test_del_nofork_all(10, 6000)
}

#[test]
fn test_del_really_large_3000_() {
    test_del_nofork_all(3000, 6000)
}



#[cfg(test)]
fn test_del_none(n: usize, val_size: usize) {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use rand::{StdRng, Rng, SeedableRng};
    use value::*;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    let seed: &[_] = &[1, 2, 3, 5];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 40960000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut db: Db<UnsafeValue, UnsafeValue> = txn.create_db().unwrap();

    let mut v = Vec::new();
    let display = n <= 100;
    for i in 0..n {
        let k0: String = rng.gen_ascii_chars()
            .take(500)
            .collect();
        let v0: String = rng.gen_ascii_chars()
            .take(val_size)
            .collect();

        let value = UnsafeValue::alloc_if_needed(&mut txn, v0.as_bytes()).unwrap();
        txn.put(&mut rng,
                 &mut db,
                 UnsafeValue::from_slice(k0.as_bytes()),
                 value)
            .unwrap();
        if display {
            debug(&txn, &[&db], format!("/tmp/test_{}", i));
        }
        v.push((k0, v0));
    }

    let mut i = 0;
    if display {
        debug(&txn, &[&db], format!("/tmp/init"));
    }
    for &(ref k0, _) in v.iter() {
        debug!(">>>>>>>>>> Deleting {:?} {:?} <<<<<<<<<<<", i, k0);
        txn.del(&mut rng,
                &mut db,
                UnsafeValue::from_slice(k0.as_bytes()),
                None)
            .unwrap();
        if display {
            debug(&txn, &[&db], format!("/tmp/del_{}", i));
        }

        assert!(txn.get(&db, UnsafeValue::from_slice(k0.as_bytes()), None).is_none());
        i += 1
    }

    if false {
        // Check that there are really n entries.
        let v: Vec<_> = txn.iter(&db, None)
            .map(|x| std::str::from_utf8(unsafe { x.0.as_slice() }).unwrap().to_string())
            .collect();
        assert_eq!(v.len(), n - 1);
        let h: HashSet<_> = v.iter().collect();
        assert_eq!(h.len(), n - 1);

    }

    let mut h = HashSet::new();
    is_tree::<UnsafeValue, UnsafeValue>(&txn, &mut h, db.0);

    let mut h3 = HashSet::new();
    {
        let rc_root = txn.root_(RC_ROOT);
        is_tree::<u64, u64>(&txn, &mut h3, rc_root);
    }
    h.extend(h3.iter());

    txn.set_root(0, db);
    txn.commit().unwrap();
    debug!("h = {:?}", h);
    {
        let stat = env.statistics();
        for i in 1..stat.total_pages {
            let p = (i * 4096) as u64;
            if !(h.contains(&p) || stat.free_pages.contains(&p) ||
                 stat.bookkeeping_pages.contains(&p)) {

                panic!("Lost track of page {:?}", p);

            }
        }
    }
    assert_eq!(occupied_pages(&env), h.len() as isize)
}

#[test]
fn test_del_none_10_() {
    test_del_none(70, 60)
}


#[cfg(test)]
use transaction::LoadPage;

#[cfg(test)]
/// Tests whether the given database is a tree. Takes an initially empty HashSet.
fn is_tree<K: Representable, V: Representable>(txn: &MutTxn<()>, h: &mut HashSet<u64>, page: u64) {
    // debug!("is_tree, page = {:?}", page);
    if page != 0 {
        if h.contains(&page) {
            panic!("h.contains({:?})", page);
        }
        h.insert(page);
        let page = txn.load_page(page);
        for (_, v, r) in page.iter_all::<K, V>() {
            if let Some((k, v)) = v {
                for offset in k.page_offsets() {
                    h.insert(offset);
                }
                for offset in v.page_offsets() {
                    h.insert(offset);
                }
            }
            // debug!("{:?} -> {:?}", page, r);
            is_tree::<K, V>(txn, h, r)
        }
    }
}

#[cfg(test)]
/// Tests whether the given database is a tree. Takes an initially empty HashSet.
fn occupied_pages(env: &Env) -> isize {
    let stat = env.statistics();
    debug!("{:?}", stat);
    stat.total_pages as isize
        - 1 //page 0
        - stat.bookkeeping_pages.len() as isize - stat.free_pages.len() as isize
}
