extern crate sanakirja_facade;
#[macro_use] extern crate sanakirja_facade_derive;

use sanakirja_facade::*;

#[derive(TypedEnv)]
pub struct BasicSchema<'env> {
    pub first_db: Db<'env, u64, u64>,
    pub nested_db: Db<'env, u64, Db<'env, u64, u64>>,
}

