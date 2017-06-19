# Sanakirja

An implementation of B trees, on top of a fully transactional layer (absolutely all operations are atomic). Inspired by [LMDB](https://symas.com/offerings/lightning-memory-mapped-database).

Sanakirja has optional reference counting, which makes it possible to fork tables in complexity O(log n), where n is the total size of the database.

Being written in [Rust](//rust-lang.org), Sanakirja also use types to encode the size of fixed-size objects, reducing on-disk size and cache misses.


## Contributing

We welcome contributions. Currently, the main areas where we need help is to replace our custom serialization framework with something more generic such as [Serde](//serde.rs).

By contributing, you agree to license all your contributions under the Apache 2.0 and MIT licenses.

Moreover, the main platform for contributing is [the Nest](//nest.pijul.com/pijul_org/sanakirja), which is still at an experimental stage. Therefore, even though we do our best to avoid it, our repository might be reset, causing the patches of all contributors to be merged.
