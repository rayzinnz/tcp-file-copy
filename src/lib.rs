use crc_fast::{checksum_file, CrcAlgorithm::Crc64Nvme};
use helper_lib::{datetime::{systemtime_to_unixtimestamp, unixtimestamp_to_systemtime}, paths::format_bytes};
use log::*;
use std::error::Error;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{Read, Seek, Write};
use std::net::{TcpStream};
use std::path::{PathBuf};
use std::time::{SystemTime};
use wincode::{SchemaWrite, SchemaRead};

pub const SIGNATURE: [u8; 4] = [0x54, 0x46, 0x43, 0x31]; //tfc1
// pub const DEFAULT_CHUNK_SIZE: usize = 1_048_576; //1MB
// pub const DEFAULT_CHUNK_SIZE: usize = 3_048_576; //3MB // max size for wincode serialization = 4MB for heap allocated structures https://github.com/anza-xyz/wincode/blob/9f0ffa346d95c31b94486b7bfea724b73330c42f/wincode/src/len.rs#L46
// pub const DEFAULT_CHUNK_SIZE: usize = 10_485_760; //10MB
pub const DEFAULT_CHUNK_SIZE: usize = 104_857_600; //100MB

#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct DownloadClientInitalise {
    pub serverside_path: String,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct DownloadServerInitalise {
	pub error_msg: Option<String>,
	pub filelen: u64,
	pub mtime: u64,
	pub crc: u64,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct DownloadClientTransfer {
    pub serverside_path: String,
    pub from_byte: u64,
    pub chunk_size: usize,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct DownloadServerTransfer {
	pub error_msg: Option<String>,
}

#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct UploadClientInitalise {
    pub serverside_path: String,
	pub is_continue: bool,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct UploadServerInitalise {
	pub error_msg: Option<String>,
	pub filelen: u64,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct UploadClientTransfer {
    pub serverside_path: String,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct UploadServerTransfer {
	pub error_msg: Option<String>,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct UploadClientEnd {
    pub serverside_path: String,
	pub mtime: u64,
	pub crc: u64,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct UploadServerEnd {
	pub error_msg: Option<String>,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct DeleteClientInitalise {
    pub serverside_path: String,
}
#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
pub struct DeleteServerResponse {
	pub error_msg: Option<String>,
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


pub fn download_file_from_server(host:&str, port:u16, src:PathBuf, mut dest:PathBuf, is_continue:bool, chunk_size:Option<usize>) -> Result<(), Box<dyn Error>> {
/*
File Download:
1. client: here is the relative path to the file. What is size of file, mtime, crc
   server: here is size of file, mtime, crc
2. client: for this relative path, give me the bytes from here to here
   server: here are the bytes
3. client: now check crc and mtime
*/

	info!("receive_file_from_host start");

    let address = format!("{}:{}", host, port);
	let chunk_size: usize = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
	
	dest.push(src.file_name().expect("no filename in src"));
	if !is_continue && dest.exists() {
		fs::remove_file(&dest).expect(&format!("Could not delete file: {}", dest.to_string_lossy()));
	}
	match dest.parent() {
		Some(parent_dir) => {fs::create_dir_all(parent_dir)?;}
		None => {}
	}

    //inital package.
	let download_client_initalise = DownloadClientInitalise {
		serverside_path: src.to_string_lossy().to_string(),
	};
	let serialized = wincode::serialize(&download_client_initalise)?;
	let step:FileCopyStep = FileCopyStep::Initialise;
    let package = [SIGNATURE.to_vec(), vec![0u8], vec![step.to_u8()], serialized].concat();
	let download_server_initalise: DownloadServerInitalise;
	{
		info!("Connecting to server at {}...", address);
		let mut stream = TcpStream::connect(&address)?;
		stream.write_all(&package)?;
		stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
		let mut buffer_from_server = Vec::new();
		let _n = stream.read_to_end(&mut buffer_from_server)?;
		// println!("{:?}", buffer_from_server);
		download_server_initalise = wincode::deserialize(&buffer_from_server).expect("Could not deserialize bytes to DownloadServerInitalise");
		debug!("download_server_initalise: {:#?}", download_server_initalise)
	}
	if let Some(errmsg) = download_server_initalise.error_msg {
		error!("{errmsg}");
		return Err(errmsg)?;
	}

	//download bytes until full or error
	loop {
		let filelen: u64;
		if !dest.exists() {
			filelen = 0;
		} else {
			let dest_metadata = dest.metadata().expect("error getting destination metadata");
			filelen = dest_metadata.len();
			if filelen >= download_server_initalise.filelen {
				break;
			}
		}
		if download_server_initalise.filelen==0 {
			info!("creating empty 0 byte file {}", dest.to_string_lossy());
			let _ = fs::write(&dest, &[])?;
			// println!("{:#?}", r);
			break;
		} else {
			info!("{:.1}% {}/{}", filelen as f64 / download_server_initalise.filelen as f64 * 100.0, format_bytes(filelen), format_bytes(download_server_initalise.filelen));
			let download_client_transfer = DownloadClientTransfer {
				serverside_path: src.to_string_lossy().to_string(),
				from_byte: filelen,
				chunk_size: chunk_size,
			};
			let serialized = wincode::serialize(&download_client_transfer)?;
			let step:FileCopyStep = FileCopyStep::Transfer;
			let package = [SIGNATURE.to_vec(), vec![0u8], vec![step.to_u8()], serialized].concat();
			let download_server_transfer: DownloadServerTransfer;
			let file_bytes: Vec<u8>;
			{
				let mut stream = TcpStream::connect(&address)?;
				stream.write_all(&package)?;
				stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
				let mut buffer_from_server = Vec::new();
				let _n = stream.read_to_end(&mut buffer_from_server)?;
				let header_len:[u8; 8] = buffer_from_server[0..8].try_into().expect("Could not convert header_len bytes to fixed length");
				let header_len = u64::from_le_bytes(header_len);
				let byte_starting_pos = 8+header_len as usize;
				let header_bytes = &buffer_from_server[8..byte_starting_pos];
				file_bytes = buffer_from_server[byte_starting_pos..].into();
				download_server_transfer = wincode::deserialize(header_bytes).expect("Could not deserialize bytes to DownloadServerTransfer");
			}
			if let Some(errmsg) = download_server_transfer.error_msg {
				error!("{errmsg}");
				return Err(errmsg)?;
			}
			{
				let mut file = OpenOptions::new().write(true).append(true).create(true).open(&dest)?;
				file.write_all(file_bytes.as_slice())?;
			}
		}
	}

	//check crc
	let file_crc = checksum_file(Crc64Nvme, &dest.to_string_lossy(), None).unwrap();
	if file_crc != download_server_initalise.crc {
		return Err("file crc mismatch")?;
	}

	//set mtime
	let mtime = unixtimestamp_to_systemtime(download_server_initalise.mtime);
	{
		let file = File::open(dest)?;
		let times = FileTimes::new()
			.set_modified(mtime);
		file.set_times(times)?;
	}

	Ok(())
}

pub fn upload_file_to_server(host:&str, port:u16, src:PathBuf, mut dest:PathBuf, is_continue:bool, chunk_size:Option<usize>) -> Result<(), Box<dyn Error>> {
/*
File Upload:
1. client: Here is the relative path to copy the file to, and if it should be continued or overwritten. What is it's current size.
   server: I will delete the file if to be overwritten, and here is the current size.
2. client: for this relative path, here are the bytes to append
   server: OK
3. client: for this relative path, here is the mtime to set to and the crc for checking.
   server: sets mtime and checks crc
*/

    if !src.exists() || !src.is_file() {
		Err(format!("Source path does not exist on client: {}", src.to_string_lossy()))?
	}
    let address = format!("{}:{}", host, port);
	let chunk_size: usize = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE); //1MB

	let src_metadata = src.metadata()?;
	// let total_size = src_metadata.len();
	let mtime = src_metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
	let mtime = systemtime_to_unixtimestamp(mtime);
	let filelen = src_metadata.len();
	let file_crc = checksum_file(Crc64Nvme, &src.to_string_lossy(), None)?;

	//dest add filename
	dest.push(src.file_name().expect("no filename in src"));
	let upload_client_initialise = UploadClientInitalise {
		serverside_path: dest.to_string_lossy().to_string(),
		is_continue: is_continue,
	};
	
	let serialized = wincode::serialize(&upload_client_initialise)?;
	let step:FileCopyStep = FileCopyStep::Initialise;
    let package = [SIGNATURE.to_vec(), vec![1u8], vec![step.to_u8()], serialized].concat();
	let upload_server_initalise: UploadServerInitalise;
	{
		info!("Connecting to server at {}...", address);
		let mut stream = TcpStream::connect(&address)?;
		stream.write_all(&package)?;
		stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
		let mut buffer_from_server = Vec::new();
		let _n = stream.read_to_end(&mut buffer_from_server)?;
		// println!("{:?}", buffer_from_server);
		upload_server_initalise = wincode::deserialize(&buffer_from_server).expect("Could not deserialize bytes to UploadServerInitalise");
		debug!("upload_server_initalise: {:#?}", upload_server_initalise)
	}
	if let Some(errmsg) = upload_server_initalise.error_msg {
		error!("{errmsg}");
		return Err(errmsg)?;
	}


	//now we send file bytes, if any left to send.
	if upload_server_initalise.filelen == filelen  {
		warn!("File of same size already exists in destination.");
		return Ok(());
	}
	{
		let mut file = File::open(src)?;
		file.seek(std::io::SeekFrom::Start(upload_server_initalise.filelen))?;
		let mut buffer = vec![0u8; chunk_size];
		loop {
			let cur_pos = file.stream_position()?;
			info!("{:.1}% {}/{}", cur_pos as f64 / filelen as f64 * 100.0, format_bytes(cur_pos), format_bytes(filelen));
			let nbytes = file.read(&mut buffer)?;
			if nbytes==0 {
				break;
			}
			let bytes: Vec<u8> = buffer[..nbytes].to_vec();
			let upload_client_transfer: UploadClientTransfer = UploadClientTransfer {
				serverside_path: dest.to_string_lossy().to_string(),
			};
			let serialized = wincode::serialize(&upload_client_transfer)?;
			let header_len: u64 = serialized.len() as u64;
			let step:FileCopyStep = FileCopyStep::Transfer;
			let package = [SIGNATURE.to_vec(), vec![1u8], vec![step.to_u8()], header_len.to_le_bytes().to_vec(), serialized, bytes].concat();
			let upload_server_transfer: UploadServerTransfer;
			{
				let mut stream = TcpStream::connect(&address)?;
				stream.write_all(&package)?;
				stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
				let mut buffer_from_server = Vec::new();
				let _n = stream.read_to_end(&mut buffer_from_server)?;
				// println!("buffer_from_server n: {}", _n);
				upload_server_transfer = wincode::deserialize(&buffer_from_server).expect("Could not deserialize bytes to UploadServerTransfer");
			}
			if let Some(errmsg) = upload_server_transfer.error_msg {
				error!("{errmsg}");
				return Err(errmsg)?;
			}
		}
	}

	//end
	let upload_client_end = UploadClientEnd {
		serverside_path: dest.to_string_lossy().to_string(),
		mtime: mtime,
		crc: file_crc,
	};
	let serialized = wincode::serialize(&upload_client_end)?;
	let step:FileCopyStep = FileCopyStep::End;
	let package = [SIGNATURE.to_vec(), vec![1u8], vec![step.to_u8()], serialized].concat();
	let upload_server_end: UploadServerEnd;
	{
		let mut stream = TcpStream::connect(&address)?;
		stream.write_all(&package)?;
		stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
		let mut buffer_from_server = Vec::new();
		let _n = stream.read_to_end(&mut buffer_from_server)?;
		upload_server_end = wincode::deserialize(&buffer_from_server).expect("Could not deserialize bytes to UploadServerEnd");
	}
	if let Some(errmsg) = upload_server_end.error_msg {
		error!("{errmsg}");
		return Err(errmsg)?;
	}

	Ok(())
}

pub fn delete_path_from_server(host:&str, port:u16, path:PathBuf) -> Result<(), Box<dyn Error>> {
/*
File Delete:
1. client: Here is the relative path to the file to be deleted
   server: I will delete the file
*/

    let address = format!("{}:{}", host, port);

	let delete_client_initialise = DeleteClientInitalise {
		serverside_path: path.to_string_lossy().to_string(),
	};
	
	let is_upload:u8 = 2;
	let serialized = wincode::serialize(&delete_client_initialise)?;
	let step:FileCopyStep = FileCopyStep::Initialise;
    let package: Vec<u8> = [SIGNATURE.to_vec(), vec![is_upload], vec![step.to_u8()], serialized].concat();
	let delete_server_response: DeleteServerResponse;
	{
		info!("Connecting to server at {}...", address);
		let mut stream = TcpStream::connect(&address)?;
		stream.write_all(&package)?;
		stream.shutdown(std::net::Shutdown::Write).expect("Error in write stream shutdown");
		let mut buffer_from_server = Vec::new();
		let _n = stream.read_to_end(&mut buffer_from_server)?;
		// println!("{:?}", buffer_from_server);
		delete_server_response = wincode::deserialize(&buffer_from_server).expect("Could not deserialize bytes to UploadServerInitalise");
		debug!("delete_server_response: {:#?}", delete_server_response)
	}
	if let Some(errmsg) = delete_server_response.error_msg {
		error!("{errmsg}");
		return Err(errmsg)?;
	}

	Ok(())
}

// cargo test -- --nocapture
#[cfg(test)]
mod tests {
	// use super::*;

    // #[test]
    // fn test_send_file_to_host() {
	// 	let result = send_file_to_host("127.0.0.1", 52709, PathBuf::from("./tests/text_utf8bom.txt"), PathBuf::from("."), true, None).unwrap();
	// 	assert_eq!(result, ());
    // }

    // #[test]
    // fn test_send_file_to_host_large() {
	// 	let host = "127.0.0.1";
	// 	// let host = "XXPA201LAP00072.local";
	// 	let result = upload_file_to_server(host, 52709, PathBuf::from("./tests/Bremshley Treadmill Service Manual.pdf"), PathBuf::from("./large"), true, None).unwrap();
	// 	assert_eq!(result, ());
    // }

}

