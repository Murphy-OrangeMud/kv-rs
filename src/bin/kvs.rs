use clap::{Arg, Command as cCommand};
use kvs::{KvStore, KvsEngine, Result};
use std::{env::current_dir, process::exit};
use stderrlog::{self, LogLevelNum, Timestamp};

fn main() -> Result<()> {
    stderrlog::new()
        .module(module_path!())
        .timestamp(Timestamp::Second)
        .verbosity(LogLevelNum::Debug)
        .init()
        .unwrap();

    let matches = cCommand::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(
            cCommand::new("set")
                .about("Set the value of a key, both types are string")
                .arg(Arg::new("KEY").help("A key").required(true))
                .arg(Arg::new("VALUE").help("A value").required(true)),
        )
        .subcommand(
            cCommand::new("get")
                .about("Get the value of a specified key")
                .arg(Arg::new("KEY").help("A key").required(true)),
        )
        .subcommand(
            cCommand::new("rm").about("Remove the key-value pair").arg(
                Arg::new("KEY")
                    .help("The key of the key-value pair to be removed")
                    .required(true),
            ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", _matches)) => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(
                _matches
                    .get_one::<String>("KEY")
                    .expect("required")
                    .to_string(),
                _matches
                    .get_one::<String>("VALUE")
                    .expect("required")
                    .to_string(),
            )?;
            //println!("Set successfully");
        }
        Some(("get", _matches)) => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.get(
                _matches
                    .get_one::<String>("KEY")
                    .expect("required")
                    .to_string(),
            )? {
                None => {
                    println!("Key not found");
                }
                Some(value) => {
                    println!("{value}");
                }
            }
        }
        Some(("rm", _matches)) => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(
                _matches
                    .get_one::<String>("KEY")
                    .expect("required")
                    .to_string(),
            ) {
                Ok(_) => {}
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

use assert_cmd::prelude::*;
use predicates::ord::eq;
use predicates::str::{contains, is_empty, PredicateStrExt};
use std::process::Command;
use tempfile::TempDir;
use walkdir::WalkDir;

// `kvs` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin("kvs").unwrap().assert().failure();
}

// `kvs -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs get <KEY>` should print "Key not found" for a non-existent key and exit with zero.
#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());
}

// `kvs rm <KEY>` should print "Key not found" for an empty database and exit with non-zero code.
#[test]
fn cli_rm_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stdout(eq("Key not found").trim());
}

// `kvs set <KEY> <VALUE>` should print nothing and exit with zero.
#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value1").trim());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value2").trim());

    Ok(())
}

// `kvs rm <KEY>` should print nothing and exit with zero.
#[test]
fn cli_rm_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());

    Ok(())
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}
