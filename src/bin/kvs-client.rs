use clap::{arg, Arg, Command};
use std::{env::current_dir, process::exit};

use kvs::{Command as kCommand, Record, Result};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};

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
                .args([
                    arg!(-a --addr <IPADDR> "Accepts an IP address to be connected to, 
                            either v4 or v6, and a port number, with the format IP:PORT. 
                            If --addr is not specified then listen on 127.0.0.1:4000"),
                ]),
        )
        .subcommand(
            Command::new("get")
                .about("Get the value of a specified key")
                .arg(Arg::new("KEY").help("A key").required(true))
                .args([
                    arg!(-a --addr <IPADDR> "Accepts an IP address to be connected to, 
                            either v4 or v6, and a port number, with the format IP:PORT. 
                            If --addr is not specified then listen on 127.0.0.1:4000"),
                ]),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove the key-value pair")
                .arg(
                    Arg::new("KEY")
                        .help("The key of the key-value pair to be removed")
                        .required(true),
                )
                .args([
                    arg!(-a --addr <IPADDR> "Accepts an IP address to be connected to, 
                            either v4 or v6, and a port number, with the format IP:PORT. 
                            If --addr is not specified then listen on 127.0.0.1:4000"),
                ]),
        )
        .args(
            [
                arg!(-a --addr <IPADDR> "Accepts an IP address to be connected to, 
                either v4 or v6, and a port number, with the format IP:PORT. 
                If --addr is not specified then listen on 127.0.0.1:4000"),
            ], //Arg::new("addr").value_name("IP-ADDRESS")
               //.help("Accepts an IP address to be connected to,
               //      either v4 or v6, and a port number, with the format IP:PORT.
               //      If --addr is not specified then listen on 127.0.0.1:4000")
        )
        .get_matches();

    let default_ip = "127.0.0.1:4000".to_string();
    let mut ip = matches.get_one::<String>("addr").unwrap_or(&default_ip);

    match matches.subcommand() {
        Some(("set", _matches)) => {
            ip = _matches.get_one::<String>("addr").unwrap_or(&ip);
            let mut socket = TcpStream::connect(ip)?;
            let record = Record {
                cmd: kCommand::Set,
                key: _matches
                    .get_one::<String>("KEY")
                    .expect("required")
                    .to_string(),
                value: _matches
                    .get_one::<String>("VALUE")
                    .expect("required")
                    .to_string(),
            };
            let buffer = serde_json::to_string(&record)?;
            socket.write(&(buffer.len() as u32 + 4).to_be_bytes())?;
            socket.write(buffer.as_bytes())?;
            socket.flush()?;
            let mut value = String::new();
            socket.try_clone()?.read_to_string(&mut value)?;
            if value.starts_with("ERROR") {
                //println!("{value}");
                exit(1);
            } else {
                exit(0);
            }
        }
        Some(("get", _matches)) => {
            ip = _matches.get_one::<String>("addr").unwrap_or(&ip);
            let mut socket = TcpStream::connect(ip)?;
            let record = Record {
                cmd: kCommand::Get,
                key: _matches
                    .get_one::<String>("KEY")
                    .expect("required")
                    .to_string(),
                value: "".to_string(),
            };
            let buffer = serde_json::to_string(&record)?;
            socket.write(&(buffer.len() as u32 + 4).to_be_bytes())?;
            socket.write(buffer.as_bytes())?;
            socket.flush()?;
            let mut value = String::new();
            socket.try_clone()?.read_to_string(&mut value)?;
            if value.starts_with("ERROR") {
                println!("Key not found");
                exit(0);
            } else {
                println!("{value}");
                exit(0);
            }
        }
        Some(("rm", _matches)) => {
            ip = _matches.get_one::<String>("addr").unwrap_or(&ip);
            let mut socket = TcpStream::connect(ip)?;
            let record = Record {
                cmd: kCommand::Remove,
                key: _matches
                    .get_one::<String>("KEY")
                    .expect("required")
                    .to_string(),
                value: "".to_string(),
            };
            let buffer = serde_json::to_string(&record)?;
            socket.write(&(buffer.len() as u32 + 4).to_be_bytes())?;
            socket.write(buffer.as_bytes())?;
            socket.flush()?;
            let mut value = String::new();
            socket.try_clone()?.read_to_string(&mut value)?;
            if value.starts_with("ERROR") {
                eprintln!("Key not found");
                //println!("{value}");
                exit(1);
            } else {
                exit(0);
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
