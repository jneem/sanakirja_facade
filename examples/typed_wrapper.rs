extern crate env_logger;
extern crate sanakirja_facade;

use sanakirja_facade::*;

struct MyTypedEnv {
    env: Env
}

struct MyTypedWriteTxn<'env> {
    txn: WriteTxn<'env>,
}

struct MyTypedReadTxn<'env> {
    txn: ReadTxn<'env>
}

impl MyTypedEnv {
    fn open_anonymous(max_length: usize) -> Result<MyTypedEnv> {
        let mut ret = MyTypedEnv {
            env: Env::open_anonymous(max_length)?
        };
        {
            let writer = ret.writer()?;
            writer.txn.create_root_db::<u64, u64>(0)?;
            writer.txn.create_root_db::<u64, Db<u64, u64>>(1)?;
            writer.txn.commit()?;
        }
        Ok(ret)
    }

    fn reader<'env>(&'env self) -> Result<MyTypedReadTxn<'env>> {
        Ok(MyTypedReadTxn {
            txn: self.env.reader()?,
        })
    }

    fn writer<'env>(&'env mut self) -> Result<MyTypedWriteTxn<'env>> {
        Ok(MyTypedWriteTxn {
            txn: self.env.writer()?
        })
    }
}

impl<'env> MyTypedWriteTxn<'env> {
    fn create_db<'txn, K: Stored<'env>, V: Stored<'env>>(&'txn self) -> Result<WriteDb<'txn, 'env, K, V>> {
        self.txn.create_db()
    }

    fn number_to_number<'txn>(&'txn self) -> RootWriteDb<'txn, 'env, u64, u64> {
        self.txn.root_db(0).unwrap()
    }

    fn number_to_number_to_number<'txn>(&'txn self) -> RootWriteDb<'txn, 'env, u64, Db<'env, u64, u64>> {
        self.txn.root_db(1).unwrap()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }
}

impl<'env> MyTypedReadTxn<'env> {
    fn number_to_number(&self) -> Db<'env, u64, u64> { self.txn.root_db(0).unwrap() }
    fn number_to_number_to_number(&self) -> Db<'env, u64, Db<'env, u64, u64>> { self.txn.root_db(1).unwrap() }
}

fn main() {
    env_logger::init().unwrap();

    let mut env = MyTypedEnv::open_anonymous(4096 * 50).unwrap();
    {
        let writer = env.writer().unwrap();
        writer.number_to_number().insert(&57, &68).unwrap();
        {
            let mut sub_db = writer.create_db().unwrap();
            sub_db.insert(&12, &34).unwrap();
            writer.number_to_number_to_number().insert(&77, &sub_db).unwrap();
        }
        writer.commit().unwrap();
    }
    let reader = env.reader().unwrap();
    assert_eq!(reader.number_to_number().get(&57), Some(68));
    assert_eq!(reader.number_to_number_to_number().get(&77).unwrap().get(&12), Some(34));
}
