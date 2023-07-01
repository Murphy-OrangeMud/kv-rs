use clap::{Arg, Command};
use std::{process::exit, env::current_dir};

use kvs::Result;

fn main() -> Result<()> {
    
    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(
            Command::new("set")
                    .about("Set the value of a key, both types are string")
                    .arg(Arg::new("KEY").help("A key").required(true))
                    .arg(Arg::new("VALUE").help("A value").required(true))
        )
        .subcommand(
            Command::new("get")
                    .about("Get the value of a specified key")
                    .arg(Arg::new("KEY").help("A key").required(true))
        )
        .subcommand(
            Command::new("rm")
                    .about("Remove the key-value pair")
                    .arg(Arg::new("KEY").help("The key of the key-value pair to be removed").required(true))
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", _matches)) => {
            let mut store = kvs::KvStore::open(current_dir()?)?;
            store.set(_matches.get_one::<String>("KEY").expect("required").to_string(), 
                    _matches.get_one::<String>("VALUE").expect("required").to_string())?;
            //println!("Set successfully");
        }
        Some(("get", _matches)) => {
            let mut store = kvs::KvStore::open(current_dir()?)?;
            match store.get(_matches.get_one::<String>("KEY").expect("required").to_string())? {
                None => {
                    println!("Key not found");
                },
                Some(value) => {
                    println!("{value}");
                }
            }
        }
        Some(("rm", _matches)) => {
            let mut store = kvs::KvStore::open(current_dir()?)?;
            match store.remove(_matches.get_one::<String>("KEY").expect("required").to_string()) {
                Ok(_) => {},
                Err(_) => {
                    println!("Key not found");
                    exit(1);
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}