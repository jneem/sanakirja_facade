#![recursion_limit="1024"]

extern crate proc_macro;
extern crate syn;
#[macro_use] extern crate quote;

use proc_macro::TokenStream;

#[proc_macro_derive(TypedEnv)]
pub fn typed_env(input: TokenStream) -> TokenStream {
    let source = input.to_string();
    let ast = syn::parse_derive_input(&source).unwrap();
    let expanded = expand_typed_env(&ast);
    expanded.parse().unwrap()
}

fn expand_typed_env(ast: &syn::DeriveInput) -> quote::Tokens {
    let name = if ast.ident.as_ref().ends_with("Schema") {
        let len = ast.ident.as_ref().len() - "Schema".len();
        ast.ident.as_ref()[..len].to_owned()
    } else {
        ast.ident.as_ref().to_owned()
    };
    let env_lifetime = if ast.generics.lifetimes.len() != 1 {
        panic!("#[derive(TypedEnv)] expects a struct with a single lifetime parameter");
    } else {
        ast.generics.lifetimes[0].lifetime.ident.clone()
    };
    let body = match ast.body {
        syn::Body::Struct(ref data) => data,
        syn::Body::Enum(_) => panic!("#[derive(TypedEnv)] can only be used with structs"),
    };
    let num_fields = body.fields().len();
    for field in body.fields() {
        if field.ident.is_none() {
            panic!("#[derive(TypedEnv)] expects named files");
        }
    }

    // For each of the root dbs, make a method to access its reader.
    let reader_fns = body.fields().iter().enumerate().map(|(idx, field)| {
        let field_name = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        let vis = &field.vis;
        quote! {
            #vis fn #field_name(&self) -> #ty {
                self.txn.root_db(#idx).unwrap()
            }
        }
    }).collect::<Vec<_>>();

    // The most natural lifetime for us to use for the transation is 'txn, but we also need to make
    // sure it's distinct from env_lifetime.
    let txn_lifetime = if env_lifetime.as_ref() == "'txn" {
        syn::Lifetime::new("'txn_")
    } else {
        syn::Lifetime::new("'txn")
    };

    // For each of the root dbs, make a method to access its writer.
    let writer_fns = body.fields().iter().enumerate().map(|(idx, field)| {
        let field_name = field.ident.as_ref().unwrap();
        let ty = db_to_root_write_db(field.ty.clone(), txn_lifetime.clone());
        let vis = &field.vis;
        quote! {
            #vis fn #field_name<#txn_lifetime>(&#txn_lifetime self) -> #ty {
                self.txn.root_db(#idx).unwrap()
            }
        }
    }).collect::<Vec<_>>();

    // In case we need to initialize a new environment, generate statements to create each of the
    // root dbs.
    let create_dbs = body.fields().iter().enumerate().map(|(idx, field)| {
        let generics = match field.ty {
            syn::Ty::Path(_, ref path) => &path.segments.last().unwrap().parameters,
            _ => panic!("expected db type to be a path"),
        };
        quote! {
            txn.create_root_db::#generics(#idx)?;
        }
    }).collect::<Vec<_>>();

    let vis = &ast.vis;
    let reader_ident = syn::Ident::new(name.clone() + "Reader");
    let writer_ident = syn::Ident::new(name.clone() + "Writer");
    let env_ident = syn::Ident::new(name.clone() + "Env");

    quote! {
        #vis struct #reader_ident<'env> {
            txn: ::sanakirja_facade::ReadTxn<'env>
        }

        impl<#env_lifetime> #reader_ident<#env_lifetime> {
            #(#reader_fns)*
        }

        #vis struct #writer_ident<'env> {
            txn: ::sanakirja_facade::WriteTxn<'env>
        }

        impl<#env_lifetime> #writer_ident<#env_lifetime> {
            #vis fn commit(self) -> ::sanakirja_facade::Result<()> {
                self.txn.commit()
            }

            #vis fn snapshot<#txn_lifetime>(&#txn_lifetime self) -> #reader_ident<#txn_lifetime> {
                #reader_ident {
                    txn: self.txn.snapshot()
                }
            }

            #vis fn create_db<#txn_lifetime, K, V>(&#txn_lifetime self)
            -> ::sanakirja_facade::Result<::sanakirja_facade::WriteDb<#txn_lifetime, #env_lifetime, K, V>>
            where
            K: ::sanakirja_facade::Stored<#env_lifetime>,
            V: ::sanakirja_facade::Stored<#env_lifetime>,
            {
                self.txn.create_db()
            }

            #(#writer_fns)*
        }

        #vis struct #env_ident {
            env: ::sanakirja_facade::Env
        }

        impl #env_ident {
            #vis fn open<P: AsRef<::std::path::Path>>(path: P, max_length: usize)
            -> ::sanakirja_facade::Result<#env_ident> {
                let mut ret = #env_ident {
                    env: ::sanakirja_facade::Env::open(path, max_length)?
                };

                let mut all_present = true;
                let mut all_absent = true;
                {
                    let txn = ret.env.reader()?;
                    for idx in 0..#num_fields {
                        let present = txn.root_db::<u64, u64>(idx).is_some();
                        all_present &= present;
                        all_absent &= !present;
                    }
                }

                if all_absent {
                    ret.create_dbs()?;
                } else if !all_present {
                    return Err(::sanakirja_facade::sanakirja::Error::Inconsistency);
                }

                Ok(ret)
            }

            fn create_dbs<#env_lifetime>(&#env_lifetime mut self) -> ::sanakirja_facade::Result<()> {
                let txn = self.env.writer()?;
                #(#create_dbs)*
                txn.commit()
            }

            #vis fn open_anonymous(max_length: usize)
            -> ::sanakirja_facade::Result<#env_ident> {
                let mut ret = #env_ident {
                    env: ::sanakirja_facade::Env::open_anonymous(max_length)?
                };
                ret.create_dbs()?;
                Ok(ret)
            }

            #vis fn reader<'env>(&'env self) -> ::sanakirja_facade::Result<#reader_ident<'env>> {
                Ok(#reader_ident {
                    txn: self.env.reader()?
                })
            }

            #vis fn writer<'env>(&'env self) -> ::sanakirja_facade::Result<#writer_ident<'env>> {
                Ok(#writer_ident {
                    txn: self.env.writer()?
                })
            }
        }
    }
}

// Convert the type Db<'x, K, V> into RootWriteDb<'lifetime_name, 'x, K, V>.
fn db_to_root_write_db(mut db: syn::Ty, lt: syn::Lifetime) -> syn::Ty {
    let cloned_db = db.clone();
    match db {
        syn::Ty::Path(_, ref mut path) => {
            let last = path.segments.last_mut().unwrap();
            if last.ident.as_ref() != "Db" {
                panic!("expected a Db type, got {:?}", cloned_db);
            }
            last.ident = syn::Ident::new("RootWriteDb");

            // RootWriteDb takes one more lifetime parameter than Db does, so we need to insert one.
            match &mut last.parameters {
                &mut syn::PathParameters::AngleBracketed(ref mut params) => {
                    params.lifetimes.insert(0, lt);
                },
                _ => {
                    panic!("expected angle bracket parameters");
                }
            }
        },
        _ => {
            panic!("expected a Db type, got {:?}", db);
        },
    };

    db
}

