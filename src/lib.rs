use crc_fast::{checksum_file, CrcAlgorithm::Crc64Nvme};
use log::*;
use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::net::{TcpStream};
use std::path::{PathBuf};
use std::time::{UNIX_EPOCH};
use uuid::Uuid;
use wincode::{SchemaWrite, SchemaRead};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FileCopyStep {
    Initialise = 0,
    Transfer = 10,
    End = 200,
}
impl FileCopyStep {
    pub fn from_u8(value: u8) -> Option<FileCopyStep> {
        match value {
            0 => Some(FileCopyStep::Initialise),
            10 => Some(FileCopyStep::Transfer),
            200 => Some(FileCopyStep::End),
            _ => None,
        }
    }
    
    pub fn to_u8(&self) -> u8 {
        *self as u8
    }
}


///copy binary data from a host to a client
pub fn send_file_to_host(host:&str, port:u16, src:PathBuf, mut dest:PathBuf, is_continue:bool, chunk_size:Option<usize>) -> Result<(), Box<dyn Error>> {
    if !src.exists() || !src.is_file() {
		Err(format!("Source path does not exist on client: {}", src.to_string_lossy()))?
	}
	let src_metadata = src.metadata()?;
	info!("send_file_to_host start");

    let address = format!("{}:{}", host, port);
    
	let chunk_size: usize = chunk_size.unwrap_or(1_048_576); //1MB
	let uuid = Uuid::new_v4();
    let uuid_bytes = uuid.as_bytes();

	let total_size = src_metadata.len();
	let mtime = src_metadata.modified().expect("could not get mtime");
	let mtime = mtime.duration_since(UNIX_EPOCH).expect("could not convert systemtime to unix epoch").as_secs();
	// let num_chunks = total_size.div_ceil(chunk_size as u64) as usize;
	let file_crc = checksum_file(Crc64Nvme, &src.to_string_lossy(), None).unwrap();

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
		crc: file_crc,
	};

	let binary_data = wincode::serialize(&stream_progress)?;
	// let stream_progress:StreamProgress = wincode::deserialize(&binary_data).expect("Could not deserialize bytes to StreamProgress");
	// println!("{:#?}", stream_progress);

    //inital package.
	let step:FileCopyStep = FileCopyStep::Initialise;
    let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), vec![step.to_u8()], binary_data].concat();

    let file_current_len: u64;
	{
		info!("Attempting to connect to server at {}...", address);
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
		info!("Recieved: \"{}\"", received_text);
		let file_current_len_str = received_text.split_once("=").expect("file_current_len could not split on =").1;
		file_current_len = file_current_len_str.parse().expect("file_current_len could not parse out u64");
	}

	//now we send file bytes, if any left to send.
	if stream_progress.operation == FileCopyOperation::WriteContinue {
		if file_current_len == src_metadata.len() {
			warn!("File of same size already exists in destination.");
			return Ok(());
		}
	}
	{
		let mut file = File::open(src)?;
		file.seek(std::io::SeekFrom::Start(file_current_len))?;
		let mut buffer = vec![0u8; chunk_size];
		loop {
			// let fpos = file.stream_position().expect("could not get stream position");
			let nbytes = file.read(&mut buffer)?;
			if nbytes==0 {
				break;
			}
			let step:FileCopyStep = FileCopyStep::Transfer;
			let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), vec![step.to_u8()], buffer[..nbytes].to_vec()].concat();
			let mut stream = TcpStream::connect(&address)?;
			stream.write_all(&package)?;
			stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
			let mut buffer_from_server = Vec::new();
			let _n = stream.read_to_end(&mut buffer_from_server)?;
			let received_text = String::from_utf8_lossy(&buffer_from_server);
			info!("Sending: \"{}\"", received_text);
		}
	}

	//send completed signal
	let step:FileCopyStep = FileCopyStep::End;
	let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), vec![step.to_u8()]].concat();
	let mut stream = TcpStream::connect(&address)?;
	stream.write_all(&package)?;
	stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
	let mut buffer_from_server = Vec::new();
	let _n = stream.read_to_end(&mut buffer_from_server)?;
	let received_text = String::from_utf8_lossy(&buffer_from_server);
	info!("End: \"{}\"", received_text);

	Ok(())
}

pub fn get_file_from_host(host:&str, port:u16, src:PathBuf, mut dest:PathBuf, is_continue:bool, chunk_size:Option<usize>) -> Result<(), Box<dyn Error>> {
	info!("receive_file_from_host start");

    let address = format!("{}:{}", host, port);
    
	let chunk_size: usize = chunk_size.unwrap_or(1_048_576); //1MB
	let uuid = Uuid::new_v4();
    let uuid_bytes = uuid.as_bytes();

	// let total_size = src_metadata.len();
	// let mtime = src_metadata.modified().expect("could not get mtime");
	// let mtime = mtime.duration_since(UNIX_EPOCH).expect("could not convert systemtime to unix epoch").as_secs();
	// // let num_chunks = total_size.div_ceil(chunk_size as u64) as usize;
	// let file_crc = checksum_file(Crc64Nvme, &src.to_string_lossy(), None).unwrap();

	// let operation: FileCopyOperation;
	// if is_continue {
	// 	operation = FileCopyOperation::WriteContinue
	// } else {
	// 	operation = FileCopyOperation::WriteReplace
	// }
	// //dest add filename if dest is a dir
	// // if dest.is_dir() {
	// dest.push(src.file_name().expect("no filename in src"));
	// // }
	// let stream_progress = StreamProgress {
	// 	serverside_path: dest.to_string_lossy().to_string(),
	// 	operation: operation,
	// 	total_size: total_size,
	// 	mtime: mtime,
	// 	chunk_size: chunk_size,
	// 	crc: file_crc,
	// };

	// let binary_data = wincode::serialize(&stream_progress)?;
	// // let stream_progress:StreamProgress = wincode::deserialize(&binary_data).expect("Could not deserialize bytes to StreamProgress");
	// // println!("{:#?}", stream_progress);

    // //inital package.
	// let step:FileCopyStep = FileCopyStep::Initialise;
    // let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), vec![step.to_u8()], binary_data].concat();

    // let file_current_len: u64;
	// {
	// 	info!("Attempting to connect to server at {}...", address);
	// 	let mut stream = TcpStream::connect(&address)?;
	// 	// Send the message
	// 	// println!("client start send bytes");
	// 	stream.write_all(&package)?;
	// 	// println!("client end send bytes");
	// 	stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
	// 	// println!("shutdown");
	// 	// get response
	// 	// stream.set_read_timeout(Some(Duration::from_millis(1000)))?;
	// 	let mut buffer_from_server = Vec::new();
	// 	let _n = stream.read_to_end(&mut buffer_from_server)?;
	// 	// println!("Client received echo: {} bytes", n);
	// 	let received_text = String::from_utf8_lossy(&buffer_from_server);
	// 	info!("Recieved: \"{}\"", received_text);
	// 	let file_current_len_str = received_text.split_once("=").expect("file_current_len could not split on =").1;
	// 	file_current_len = file_current_len_str.parse().expect("file_current_len could not parse out u64");
	// }

	// //now we send file bytes, if any left to send.
	// if stream_progress.operation == FileCopyOperation::WriteContinue {
	// 	if file_current_len == src_metadata.len() {
	// 		warn!("File of same size already exists in destination.");
	// 		return Ok(());
	// 	}
	// }
	// {
	// 	let mut file = File::open(src)?;
	// 	file.seek(std::io::SeekFrom::Start(file_current_len))?;
	// 	let mut buffer = vec![0u8; chunk_size];
	// 	loop {
	// 		// let fpos = file.stream_position().expect("could not get stream position");
	// 		let nbytes = file.read(&mut buffer)?;
	// 		if nbytes==0 {
	// 			break;
	// 		}
	// 		let step:FileCopyStep = FileCopyStep::Transfer;
	// 		let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), vec![step.to_u8()], buffer[..nbytes].to_vec()].concat();
	// 		let mut stream = TcpStream::connect(&address)?;
	// 		stream.write_all(&package)?;
	// 		stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
	// 		let mut buffer_from_server = Vec::new();
	// 		let _n = stream.read_to_end(&mut buffer_from_server)?;
	// 		let received_text = String::from_utf8_lossy(&buffer_from_server);
	// 		info!("Sending: \"{}\"", received_text);
	// 	}
	// }

	// //send completed signal
	// let step:FileCopyStep = FileCopyStep::End;
	// let package = [SIGNATURE.to_vec(), uuid_bytes.to_vec(), vec![step.to_u8()]].concat();
	// let mut stream = TcpStream::connect(&address)?;
	// stream.write_all(&package)?;
	// stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
	// let mut buffer_from_server = Vec::new();
	// let _n = stream.read_to_end(&mut buffer_from_server)?;
	// let received_text = String::from_utf8_lossy(&buffer_from_server);
	// info!("End: \"{}\"", received_text);

	Ok(())
}

// cargo test -- --nocapture
#[cfg(test)]
mod tests {
	use super::*;

    // #[test]
    // fn test_send_file_to_host() {
	// 	let result = send_file_to_host("127.0.0.1", 52709, PathBuf::from("./tests/text_utf8bom.txt"), PathBuf::from("."), true, None).unwrap();
	// 	assert_eq!(result, ());
    // }

    #[test]
    fn test_send_file_to_host_large() {
		let host = "127.0.0.1";
		// let host = "XXPA201LAP00072.local";
		let result = send_file_to_host(host, 52709, PathBuf::from("./tests/Bremshley Treadmill Service Manual.pdf"), PathBuf::from("./large"), true, None).unwrap();
		assert_eq!(result, ());
    }

}

