use std::fs::{self, FileTimes, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{PathBuf, absolute};
use std::time::{Duration, SystemTime};
use std::{env, process, thread};
use std::error::Error;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use tcp_file_copy::{FileCopyOperation, SIGNATURE, StreamProgress};

fn handle_client(mut stream: TcpStream, streams_in_progress: Arc<RwLock<HashMap<[u8; 16], StreamProgress>>>, root_path:PathBuf) -> Result<(), Box<dyn Error>> {
    // A buffer to hold the incoming data
    let mut buffer = Vec::new();

    match stream.read_to_end(&mut buffer) {
        Ok(n) if n > 20 => {
            //expected chunk header: signature: 4 bytes + uuid 16 bytes,
            let signature:[u8; 4] = buffer[0..4].try_into().expect("buffer size is not 4 bytes");
            if signature != SIGNATURE {
                Err("Unexpected signature at start of chunk.")?;
            }
            //if new item then a string of all met details, otherwise if is_read then amount of bytes or !is_read then bytes to stream through
            // let mut stream_exists = false;
            // {

            // }
            let uuid_bytes:[u8; 16] = buffer[4..20].try_into().expect("buffer size is not 16 bytes");
            //let streams_in_progress_reader = streams_in_progress.read().expect("could not read streams_in_progress");
            if streams_in_progress.read().expect("could not read streams_in_progress").contains_key(&uuid_bytes) {
                let streams_in_progress_reader = streams_in_progress.read().expect("could not read streams_in_progress");
                let stream_progress = streams_in_progress_reader.get(&uuid_bytes).unwrap();
                // println!("{:#?}", stream_progress);
                let mtime = SystemTime::UNIX_EPOCH.checked_add(Duration::new(stream_progress.mtime, 0)).expect("could not get systemtime for mtime");
                //write bytes to end of file
                let full_path = absolute(root_path.join(&stream_progress.serverside_path))?;
                // println!("full_path: {:?}", full_path);
                fs::create_dir_all(full_path.parent().unwrap())?;
                let file_bytes = &buffer[20..];
                // println!("file_bytes: {:?}", file_bytes);
                {
                    let mut file = OpenOptions::new().write(true).append(true).create(true).open(&full_path)?;
                    file.write_all(file_bytes)?;
                    let times = FileTimes::new()
                        .set_modified(mtime);
                    file.set_times(times)?;
                }
                let file_metadata = full_path.metadata()?;
                let msg = format!("in progress stream, filesize: {} / {} {:.1}%", file_metadata.len(), stream_progress.total_size, file_metadata.len() as f64 / stream_progress.total_size as f64 * 100.0);
                println!("{msg}");
                stream.write_all(msg.as_bytes())?;
            } else {
                let stream_bytes = &buffer[20..];
                let stream_progress:StreamProgress = wincode::deserialize(stream_bytes).expect("Could not deserialize bytes to StreamProgress");
                println!("{:#?}", stream_progress);
                let mut file_current_len: u64 = 0;
                let full_path = absolute(root_path.join(&stream_progress.serverside_path))?;
                println!("full_path: {:?}", full_path);
                if full_path.exists() {
                    if stream_progress.operation == FileCopyOperation::WriteReplace {
                        fs::remove_file(&full_path).expect("Could not remove file"); 
                    } else {
                        let serverside_path_metadata = full_path.metadata().expect("error getting serverside_path metadata");
                        file_current_len = serverside_path_metadata.len();
                    }
                }
                streams_in_progress.write().expect("could not write streams_in_progress").insert(uuid_bytes, stream_progress.clone());
                //send back starting byte. To know where to continue from.
                stream.write_all(format!("file_current_len={}", file_current_len).as_bytes())?;
            }
        }
        Ok(n) if n > 0 => {
            Err(format!("Server received: {} bytes, should contain header", n))?;
        }
        Ok(_) => {
            Err("Connection closed by client.")?;
        }
        Err(e) => {
            Err(format!("Failed to read from stream: {}", e))?;
        }
    }
    Ok(())
}

fn run_server(host:&str, port:&str, root_path:PathBuf) -> Result<(), std::io::Error> {
    let address = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&address)?;
    let streams_in_progress: Arc<RwLock<HashMap<[u8; 16], StreamProgress>>> = Arc::new(RwLock::new(HashMap::new()));

    println!("TCP Server running on {}", address);
    println!("Listening for connections...");

    // Accept connections and process them sequentially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                //let peer_addr = stream.peer_addr().unwrap_or("Unknown".parse().unwrap());
                match stream.peer_addr() {
                    Ok(peer_addr) => {
                        println!("\nNew connection established from {}", peer_addr);
                        // Handle the client in a new thread to allow for concurrent connections
                        let streams_in_progress_clone = Arc::clone(&streams_in_progress);
                        let root_path_clone = root_path.clone();
                        let handle = thread::spawn(move || {
                            handle_client(stream, streams_in_progress_clone, root_path_clone).expect("Error from handle_client");
                        });
                        if let Err(e) = handle.join() {
                            eprintln!("Error in handle_client: {:?}", e);
                        }
                    }
                    Err(e) => {
                        panic!("!!! ERROR 1 {}", e)
                    }
                }
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}

fn print_usage() {
    eprintln!("\nTCP App Usage:");
    eprintln!("  Server: cargo run -- server HOST PORT root_path");
    // cargo run server 127.0.0.1 52709 "/home/ray/temp"
    eprintln!("  Client: cargo run -- send_file HOST PORT src_path dest_path");
    eprintln!("\nExample:");
    eprintln!("  1. Terminal 1: cargo run -- server");
    eprintln!("  2. Terminal 2: cargo run -- client \"Hello, World!\"");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // println!("{args:?}");
    
    // Check command line arguments to determine mode
    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }
    if args[1]==String::from("server") {
        if args.len() < 5 {
            print_usage();
            process::exit(1);
        }
        let host = args[2].clone();
        let port = args[3].clone();
        let root_path = PathBuf::from(&args[4]);
        if let Err(err) = run_server(&host, &port, root_path) {
            eprint!("Server error: {}", err);
            process::exit(1);
        }
    } else if args[1]==String::from("send_file") {
    } else {
        print_usage();
        process::exit(1);
    }
}