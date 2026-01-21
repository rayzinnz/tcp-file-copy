use crc_fast::{checksum_file, CrcAlgorithm::Crc64Nvme};
use helper_lib::{setup_logger, datetime::{systemtime_to_unixtimestamp, unixtimestamp_to_systemtime}};
use log::*;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{Read, Write, Seek};
use std::net::{TcpListener, TcpStream};
use std::path::{PathBuf, absolute};
use std::time::{SystemTime};
use std::{env, process, thread};
use std::error::Error;
use tcp_file_copy::{DownloadClientInitalise, DownloadClientTransfer, DownloadServerInitalise, DownloadServerTransfer, FileCopyStep, SIGNATURE, UploadClientEnd, UploadClientInitalise, UploadClientTransfer, UploadServerEnd, UploadServerInitalise, UploadServerTransfer, download_file_from_server, upload_file_to_server};

fn get_full_path(root_path:Option<PathBuf>, serverside_path:String) -> PathBuf {
    match root_path {
        Some(root_path) =>  absolute(root_path.join(&serverside_path)).expect("Could not add serverside_path to root_path"),
        None => PathBuf::from(&serverside_path)
    }
}

fn handle_client(mut stream: TcpStream, root_path:Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    // A buffer to hold the incoming data
    let mut buffer = Vec::new();

    match stream.read_to_end(&mut buffer) {
        Ok(n) if n >= 6 => {
            //expected chunk header: signature: 4 bytes + step 1 byte,
            let signature:[u8; 4] = buffer[0..4].try_into().expect("buffer size is not 4 bytes");
            if signature != SIGNATURE {
                Err("Unexpected signature at start of chunk.")?;
            }
            let is_upload = buffer[4];
            let step = buffer[5];
            // println!("is_upload: {}, step: {}", is_upload, step);
            let step = FileCopyStep::from_u8(step).expect(&format!("unexpected step value: {}", step));
            if is_upload == 0 {
                //is download operation
                // println!("step: {:?}", step);
                if step == FileCopyStep::Initialise {
                    let stream_bytes = &buffer[6..];
                    let download_client_initialise:DownloadClientInitalise = wincode::deserialize(stream_bytes).expect("Could not deserialize bytes to DownloadClientInitalise");
                    debug!("{:#?}", download_client_initialise);
                    let full_path: PathBuf = get_full_path(root_path, download_client_initialise.serverside_path);
                    let download_server_initialise: DownloadServerInitalise;
                    if !full_path.exists() {
                        download_server_initialise = DownloadServerInitalise {
                            error_msg: Some(format!("File does not exist on server: {}", full_path.to_string_lossy())),
                            filelen: 0,
                            mtime: 0,
                            crc: 0,
                        }
                    } else {
                        let mut errmsg: Option<String> = None;
                        let mut filelen: u64 = 0;
                        let mut mtime: u64 = 0;
                        let mut crc: u64 = 0;
                        match full_path.metadata() {
                            Ok(serverside_path_metadata) => {
                                filelen = serverside_path_metadata.len();
                                mtime = systemtime_to_unixtimestamp(serverside_path_metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH));
                            }
                            Err(e) => {
                                errmsg = Some(format!("Error getting serverside_path metadata for {}: {}", full_path.to_string_lossy(), e));
                            }
                        }
                        if errmsg.is_none() {
                            match checksum_file(Crc64Nvme, &full_path.to_string_lossy(), None) {
                                Ok(file_crc) => {
                                    crc = file_crc;
                                }
                                Err(e) => {
                                    errmsg = Some(format!("Error getting crc for {}: {}", full_path.to_string_lossy(), e));
                                }
                            }
                        }
                        download_server_initialise = DownloadServerInitalise {
                            error_msg: errmsg,
                            filelen: filelen,
                            mtime: mtime,
                            crc: crc,
                        }
                    }
                    let serialized = wincode::serialize(&download_server_initialise)?;
                    // println!("{:?}", serialized);
                    stream.write_all(&serialized)?;
                } else if step == FileCopyStep::Transfer {
                    let stream_bytes = &buffer[6..];
                    let download_client_transfer:DownloadClientTransfer = wincode::deserialize(stream_bytes).expect("Could not deserialize bytes to DownloadClientTransfer");
                    let full_path: PathBuf = get_full_path(root_path, download_client_transfer.serverside_path);
                    let mut errmsg: Option<String> = None;
                    let mut bytes: Vec<u8> = Vec::new();
                    'fileop: {
                        let mut file = File::open(full_path)?;
                        if let Err(e) = file.seek(std::io::SeekFrom::Start(download_client_transfer.from_byte)) {
                            errmsg = Some(format!("Error seeking file: {}", e));
                            break 'fileop;
                        }
                        let mut buffer = vec![0u8; download_client_transfer.chunk_size];
                        match file.read(&mut buffer) {
                            Ok(nbytes) => {
                                if nbytes==0 {
                                    errmsg = Some(format!("0 bytes read"));
                                    break 'fileop;
                                }
                                bytes = buffer[..nbytes].to_vec();
                            }
                            Err(e) => {
                                errmsg = Some(format!("Error reading file: {}", e));
                                break 'fileop;
                            }
                        }
                    }
                    let download_server_transfer = DownloadServerTransfer {
                        error_msg: errmsg,
                        //bytes: bytes
                    };
                    let serialized = wincode::serialize(&download_server_transfer)?;
                    let header_len: u64 = serialized.len() as u64;
                    let package = [header_len.to_le_bytes().to_vec(), serialized, bytes].concat();
                    stream.write_all(&package)?;
                }
            } else {
                //is upload operation
                if step == FileCopyStep::Initialise {
                    let stream_bytes = &buffer[6..];
                    let upload_client_initialise:UploadClientInitalise = wincode::deserialize(stream_bytes).expect("Could not deserialize bytes to UploadClientInitalise");
                    debug!("{:#?}", upload_client_initialise);
                    let full_path: PathBuf = get_full_path(root_path, upload_client_initialise.serverside_path);
                    let mut errmsg: Option<String> = None;
                    let mut filelen: u64 = 0;
                    if !upload_client_initialise.is_continue && full_path.exists() {
                        if let Err(e) = fs::remove_file(&full_path) {
                            errmsg = Some(format!("Error deleting existing file on server: {}", e));
                        }
                    }
                    if errmsg.is_none() && full_path.exists() {
                        match full_path.metadata() {
                            Ok(dest_metadata) => {
                                filelen = dest_metadata.len();
                            }
                            Err(e) => {
                                errmsg = Some(format!("Error getting metadata of file on server: {}", e));
                            }
                        }
                    }
                    let upload_server_initialise: UploadServerInitalise = UploadServerInitalise {
                        error_msg: errmsg,
                        filelen: filelen
                    };
                    let serialized = wincode::serialize(&upload_server_initialise)?;
                    stream.write_all(&serialized)?;
                } else if step == FileCopyStep::Transfer {
                    let header_len:[u8; 8] = buffer[6..6+8].try_into().expect("Could not convert header_len bytes to fixed length");
                    let header_len = u64::from_le_bytes(header_len);
                    let byte_starting_pos = 14+header_len as usize;
                    let header_bytes = &buffer[14..byte_starting_pos];
                    let stream_bytes = &buffer[byte_starting_pos..];
                    let upload_client_transfer:UploadClientTransfer = wincode::deserialize(header_bytes).expect("Could not deserialize bytes to UploadClientTransfer");
                    let full_path: PathBuf = get_full_path(root_path, upload_client_transfer.serverside_path);
                    // println!("full_path: {:?}", full_path);
                    //write bytes to end of file
                    let mut errmsg: Option<String> = None;
                    if let Err(e) = fs::create_dir_all(full_path.parent().unwrap()) {
                        errmsg = Some(format!("Error creating dirs on server: {}", e));
                    };
                    let mut bytes: Vec<u8> = Vec::new();
                    if errmsg.is_none() {
                        bytes = stream_bytes.into();
                    }
                    if errmsg.is_none() {
                        {
                            match OpenOptions::new().write(true).append(true).create(true).open(&full_path) {
                                Ok(mut file) => {
                                    if let Err(e) = file.write_all(&bytes) {
                                        errmsg = Some(format!("Error writing data to file on server: {}", e));
                                    };
                                }
                                Err(e) => {
                                    errmsg = Some(format!("Error opening file for writing on server: {}", e));
                                }
                            }
                        }
                    }
                    let upload_server_transfer = UploadServerTransfer {
                        error_msg: errmsg
                    };
                    let serialized = wincode::serialize(&upload_server_transfer)?;
                    stream.write_all(&serialized)?;
                } else if step == FileCopyStep::End {
                    let stream_bytes = &buffer[6..];
                    let upload_client_end:UploadClientEnd = wincode::deserialize(stream_bytes).expect("Could not deserialize bytes to UploadClientEnd");
                    let full_path: PathBuf = get_full_path(root_path, upload_client_end.serverside_path);
                    let mut errmsg: Option<String> = None;
                    if !full_path.exists() {
                        errmsg = Some(format!("File {} does not exists on server.", full_path.to_string_lossy()));
                    }
                    if errmsg.is_none() {
                        match checksum_file(Crc64Nvme, &full_path.to_string_lossy(), None) {
                            Ok(file_crc) => {
                                if file_crc != upload_client_end.crc {
                                    errmsg = Some(format!("CRC does not match for file {}", full_path.to_string_lossy()));
                                };
                            }
                            Err(e) => {
                                errmsg = Some(format!("Error getting crc for {}: {}", full_path.to_string_lossy(), e));
                            }
                        }
                    }
                    if errmsg.is_none() {
                        let mtime = unixtimestamp_to_systemtime(upload_client_end.mtime);
                        {
                            match File::open(full_path) {
                                Ok(file) => {
                                    let times = FileTimes::new()
                                        .set_modified(mtime);
                                    if let Err(e) = file.set_times(times) {
                                        errmsg = Some(format!("Could not file.set_times on server: {}", e));
                                    };
                                }
                                Err(e) => {
                                    errmsg = Some(format!("Could not open file to set mtime on server: {}", e));
                                }
                            }
                        }
                    }
                    let upload_server_end = UploadServerEnd {
                        error_msg: errmsg
                    };
                    let serialized = wincode::serialize(&upload_server_end)?;
                    stream.write_all(&serialized)?;
                }
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

fn run_server(host:&str, port:&str, root_path:Option<PathBuf>) -> Result<(), std::io::Error> {
    let address = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&address)?;
    // let streams_in_progress: Arc<RwLock<HashMap<[u8; 16], StreamProgress>>> = Arc::new(RwLock::new(HashMap::new()));

    println!("TCP Server running on {}", address);
    println!("Listening for connections...");

    // Accept connections and process them sequentially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                //let peer_addr = stream.peer_addr().unwrap_or("Unknown".parse().unwrap());
                match stream.peer_addr() {
                    Ok(_peer_addr) => {
                        // println!("\nNew connection established from {}", peer_addr);
                        // Handle the client in a new thread to allow for concurrent connections
                        // let streams_in_progress_clone = Arc::clone(&streams_in_progress);
                        let root_path_clone = root_path.clone();
                        let handle = thread::spawn(move || {
                            handle_client(stream, root_path_clone).expect("Error from handle_client");
                        });
                        if let Err(e) = handle.join() {
                            error!("Error in handle_client: {:?}", e);
                        }
                    }
                    Err(e) => {
                        panic!("!!! ERROR 1 {}", e)
                    }
                }
            }
            Err(e) => {
                error!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}

fn print_usage() {
    eprintln!("\nTCP App Usage:");
    eprintln!("  Server: cargo run -- server HOST PORT --path root_path");
    // cargo run server 127.0.0.1 52709 --path "/home/ray/temp"
    // cargo run server XXPA201LAP00072.local 52709 --path "C:\Users\hrag\temp"
    eprintln!("  Client: cargo run -- upload HOST PORT src_path_local dest_path_server");
    // cargo run upload 127.0.0.1 52709 "./tests/Bremshley Treadmill Service Manual.pdf" "./large"
    // cargo run upload 127.0.0.1 52709 "/home/ray/Downloads/vulkansdk-linux-x86_64-1.4.328.1.tar.xz" "./large"
    // cargo run upload XXPA201LAP00072.local 52709 "./tests/Bremshley Treadmill Service Manual.pdf" "./large"
    eprintln!("  Client: cargo run -- download HOST PORT src_path_server dest_path_local");
    // cargo run download 127.0.0.1 52709 "./large/Bremshley Treadmill Service Manual.pdf" "/home/ray/temp/rec"
    // cargo run download XXPA201LAP00072.local 52709 "./large/Bremshley Treadmill Service Manual.pdf" "C:\Users\hrag\temp\rec"
    eprintln!("\nExample:");
    eprintln!("  1. Terminal 1: cargo run -- server");
    eprintln!("  2. Terminal 2: cargo run -- client \"Hello, World!\"");
}

fn main() {
    setup_logger(LevelFilter::Trace);

    let args: Vec<String> = env::args().collect();
    // println!("{args:?}");
    
    // Check command line arguments to determine mode
    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }
    let mut is_continue = true;
    if args.contains(&"--overwrite".to_string()) {
        is_continue = false;
    }
    if args[1]==String::from("server") {
        if args.len() < 4 {
            print_usage();
            process::exit(1);
        }
        let host = args[2].clone();
        let port = args[3].clone();
        //the rest come in pairs
        let mut root_path: Option<PathBuf> = None;
        for iarg in (4..args.len()).step_by(2) {
            if args[iarg] == "--path" {
                root_path = Some(PathBuf::from(&args[iarg+1]));
            }
        }
        if let Err(err) = run_server(&host, &port, root_path) {
            eprint!("Server error: {}", err);
            process::exit(1);
        }
    } else if args[1]==String::from("upload") {
        if args.len() < 6 {
            print_usage();
            process::exit(1);
        }
        let host = args[2].clone();
        let port: u16 = args[3].clone().parse().expect("error parsing port to u16");
        let src = PathBuf::from(&args[4]);
        let dest = PathBuf::from(&args[5]);
        upload_file_to_server(&host, port, src, dest, is_continue, None).expect("Error in upload_file_to_server")
    } else if args[1]==String::from("download") {
        if args.len() < 6 {
            print_usage();
            process::exit(1);
        }
        let host = args[2].clone();
        let port: u16 = args[3].clone().parse().expect("error parsing port to u16");
        let src = PathBuf::from(&args[4]);
        let dest = PathBuf::from(&args[5]);
        download_file_from_server(&host, port, src, dest, is_continue, None).expect("Error in download_file_from_server")
    } else {
        print_usage();
        process::exit(1);
    }
}