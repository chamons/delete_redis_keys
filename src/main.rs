use eyre::Result;
use flate2::read::ZlibDecoder;
use governor::{Quota, RateLimiter};
use itertools::Itertools;
use redis::Commands;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::num::NonZeroU32;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    match args.len() {
        3 | 4 => {}
        _ => {
            panic!("cargo run -- REDIS_URL FILENAME");
        }
    }

    // First we unzip the data file in question
    extract_file_text(&args[2])?;

    let skip = match args.get(3) {
        Some(skip) => skip.parse::<usize>().expect("Invalid skip argument"),
        None => 0,
    };

    delete_keys(&args[1], &args[2], skip).await?;

    Ok(())
}

const CHUNK_SIZE: usize = 100;
const PRINT_CHUNK_OFFSET: usize = 100;
const REQUESTS_PER_SECOND: u32 = 40;

async fn delete_keys(redis: &str, file: &str, skip: usize) -> Result<(), eyre::Error> {
    let client = redis::Client::open(redis).expect("Unable to create redis client");
    let mut conn = client.get_connection().expect("Failed to connect to redis");

    let reader = BufReader::new(File::open(file)?);

    let limiter = RateLimiter::direct(Quota::per_second(
        NonZeroU32::new(REQUESTS_PER_SECOND).unwrap(),
    ));

    let mut chunk_index = 0;
    for chunk in &reader.lines().skip(skip).chunks(CHUNK_SIZE) {
        if chunk_index % PRINT_CHUNK_OFFSET == 0 {
            println!("{chunk_index}");
        }
        let chunk = chunk.map(|c| c.unwrap()).collect::<Vec<_>>();

        limiter.until_ready().await;

        // println!("{:?}", chunk);
        conn.unlink::<&[String], ()>(&chunk)?;
        chunk_index += 1;
    }

    Ok(())
}

fn extract_file_text(file: &str) -> Result<(), eyre::Error> {
    let compressed_data = std::fs::read(file)?;
    let decompressed_data = decode(&compressed_data)?;
    let decompressed_file_name = Path::new(&file)
        .file_stem()
        .expect("Can't find output path")
        .to_string_lossy();
    std::fs::write(format!("{decompressed_file_name}.txt"), decompressed_data)?;
    Ok(())
}

fn decode(compressed: &[u8]) -> std::result::Result<String, eyre::ErrReport> {
    let mut decoder = ZlibDecoder::new(&*compressed);

    let mut decoded = String::new();
    decoder.read_to_string(&mut decoded)?;

    Ok(decoded)
}
