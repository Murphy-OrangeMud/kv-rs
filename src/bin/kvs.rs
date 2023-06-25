use clap::{Arg, Command};
use std::process::exit;

fn main() {
    
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
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("get", _matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("rm", _matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(),
    }
}