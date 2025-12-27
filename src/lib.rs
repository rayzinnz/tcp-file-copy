use wincode::{SchemaWrite, SchemaRead};
use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::net::{TcpStream};
use std::path::{Path, PathBuf};
use std::time::{UNIX_EPOCH};
use uuid::Uuid;
#[cfg(target_os = "linux")]
use std::os::unix::fs::{FileExt, MetadataExt};

pub const SIGNATURE: [u8; 4] = [0x54, 0x46, 0x43, 0x31]; //tfc1

#[derive(Clone, Debug, PartialEq, SchemaWrite, SchemaRead)]
pub enum FileCopyOperation {
	WriteReplace,
	WriteContinue,
	Read,
}

#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct StreamProgress {
    pub serverside_path: String,
    pub operation: FileCopyOperation,
    pub total_size: u64,
	pub mtime: u64,
    pub chunk_size: usize,
	pub crc: u64,
}

///copy binary data from a host to a client
pub fn send_file_to_host(host:&str, port:u16, src:&Path, mut dest:PathBuf, is_continue:bool, chunk_size:Option<usize>) -> Result<(), Box<dyn Error>> {
    if !src.exists() || !src.is_file() {
		Err(format!("Source path does not exist on client: {}", src.to_string_lossy()))?
	}
	let src_metadata = src.metadata()?;
	println!("send_file_to_host start");

    let address = format!("{}:{}", host, port);
    
	let chunk_size: usize = chunk_size.unwrap_or(1_048_576); //1MB
	let uuid = Uuid::new_v4();
    let uuid_bytes = uuid.as_bytes();

	let total_size = src_metadata.len();
	let mtime = src_metadata.modified().expect("could not get mtime");
	let mtime = mtime.duration_since(UNIX_EPOCH).expect("could not convert systemtime to unix epoch").as_secs();
	// let num_chunks = total_size.div_ceil(chunk_size as u64) as usize;

	let operation: FileCopyOperation;
	if is_continue {
		operation = FileCopyOperation::WriteContinue
	} else {
		operation = FileCopyOperation::WriteReplace
	}
	//dest add filename if dest is a dir
	// if dest.is_dir() {
	dest.push(src.file_name().expect("no filename in src"));
	// }
	let stream_progress = StreamProgress {
		serverside_path: dest.to_string_lossy().to_string(),
		operation: operation,
		total_size: total_size,
		mtime: mtime,
		chunk_size: chunk_size,
		crc: 0,
	};

	let binary_data = wincode::serialize(&stream_progress)?;
	// let stream_progress:StreamProgress = wincode::deserialize(&binary_data).expect("Could not deserialize bytes to StreamProgress");
	// println!("{:#?}", stream_progress);

    //inital package.
    let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), binary_data].concat();

    let mut file_current_len: u64 = 0;
	{
		println!("Attempting to connect to server at {}...", address);
		let mut stream = TcpStream::connect(&address)?;
		// Send the message
		// println!("client start send bytes");
		stream.write_all(&package)?;
		// println!("client end send bytes");
		stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
		// println!("shutdown");
		// get response
		// stream.set_read_timeout(Some(Duration::from_millis(1000)))?;
		let mut buffer_from_server = Vec::new();
		let _n = stream.read_to_end(&mut buffer_from_server)?;
		// println!("Client received echo: {} bytes", n);
		let received_text = String::from_utf8_lossy(&buffer_from_server);
		println!("Recieved 01: \"{}\"", received_text);
		let file_current_len_str = received_text.split_once("=").expect("file_current_len could not split on =").1;
		file_current_len = file_current_len_str.parse().expect("file_current_len could not parse out u64");
	}

	//now we send file bytes, if any left to send.
	if stream_progress.operation == FileCopyOperation::WriteContinue {
		if file_current_len == src_metadata.len() {
			println!("File of same size already exists in destination.");
			return Ok(());
		}
	}
	{
		let mut file = File::open(src)?;
		file.seek(std::io::SeekFrom::Start(file_current_len))?;
		let mut buffer = vec![0u8; chunk_size];
		loop {
			let fpos = file.stream_position().expect("could not get stream position");
			let nbytes = file.read(&mut buffer)?;
			if nbytes==0 {
				break;
			}
			let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), buffer[..nbytes].to_vec()].concat();
			let mut stream = TcpStream::connect(&address)?;
			stream.write_all(&package)?;
			stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
			let mut buffer_from_server = Vec::new();
			let _n = stream.read_to_end(&mut buffer_from_server)?;
			let received_text = String::from_utf8_lossy(&buffer_from_server);
			println!("Recieved 02: \"{}\"", received_text);
		}
	}

	//send file modified datetime

	Ok(())
}

// fn run_client(host: &str, port: &str, bytes_to_send: Vec<u8>) -> Result<(), std::io::Error> {
//     let address = format!("{}:{}", host, port);
//     println!("Attempting to connect to server at {}...", address);
    
//     // Connect to the server
//     let mut stream = match TcpStream::connect(&address) {
//         Ok(s) => {
//             println!("Successfully connected to {}", address);
//             s
//         }
//         Err(e) => {
//             eprintln!("Failed to connect: {}. Ensure the server is running.", e);
//             return Err(e);
//         }
//     };

//     // Send the message
//     match stream.write_all(&bytes_to_send) {
//         Ok(_) => println!("Client sent: {} bytes", bytes_to_send.len()),
//         Err(e) => {
//             eprintln!("Failed to send data: {}", e);
//             return Err(e);
//         }
//     }
    
//     // Set a timeout for reading the response
//     // stream.set_read_timeout(Some(Duration::from_millis(1000)))?;

//     // Receive the echoed response
//     // let mut buffer = [0; 512];
//     // match stream.read(&mut buffer) {
//     //     Ok(n) if n > 0 => {
//     //         let received_text = String::from_utf8_lossy(&buffer[..n]);
//     //         println!("Client received echo: {} bytes", n);
//     //         println!("Echoed data: \"{}\"", received_text);
//     //     }
//     //     Ok(_) => {
//     //         println!("Server closed the connection without sending an echo.");
//     //     }
//     //     Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
//     //          // Timeout is expected if the server doesn't respond quickly, or if the server logic isn't set up for immediate echo.
//     //          // Given our handle_client echoes immediately, this likely means a network or setup issue.
//     //          println!("Timed out waiting for server echo. Error: {}", e);
//     //     }
//     //     Err(e) => {
//     //         eprintln!("Failed to read response: {}", e);
//     //         return Err(e);
//     //     }
//     // }

//     Ok(())
// }


// cargo test -- --nocapture
#[cfg(test)]
mod tests {
	use super::*;

    // #[test]
    // fn test_send_file_to_host() {
	// 	let result = send_file_to_host("127.0.0.1", 52709, Path::new("./tests/text_utf8bom.txt"), PathBuf::from("."), true, None).unwrap();
	// 	assert_eq!(result, ());
    // }

    #[test]
    fn test_send_file_to_host_large() {
		let result = send_file_to_host("127.0.0.1", 52709, Path::new("./tests/Bremshley Treadmill Service Manual.pdf"), PathBuf::from("./large"), true, None).unwrap();
		assert_eq!(result, ());
    }

}

