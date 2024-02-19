use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use compact_str::CompactString;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use rustc_hash::FxHashSet;
use simd_json::derived::ValueObjectAccessAsScalar;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use walkdir::WalkDir;
use zstd::stream::Decoder;

pub async fn extract_parse(input_filename: &Path, use_json: bool) -> FxHashSet<CompactString> {
    let mut usernames: FxHashSet<CompactString> = FxHashSet::default();
    let file = match File::open(input_filename) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{:?}", e);
            return FxHashSet::default();
        }
    };
    let mut reader = match Decoder::with_buffer(BufReader::new(file)) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{:?}", e);
            return FxHashSet::default();
        }
    };
    let _ = reader.window_log_max(31);
    let mut reader = BufReader::new(reader);
    if use_json {
        // There are two ways to parse the data. using simd_json (fastest Rust JSON parser) or manually.
        let mut line: String = "".into();
        while reader.read_line(&mut line).unwrap_or_default() > 0 {
            let line_bytes = line.bytes().collect::<Vec<u8>>();
            match simd_json::to_borrowed_value(&mut line_bytes.clone()) {
                Ok(o) => {
                    let author = o.get_str("author").unwrap_or_default();
                    if author.len() <= 20 {
                        usernames.insert(author.into());
                    }
                }
                Err(e) => eprintln!("{:?}", e),
            }
            line.clear();
        }
    } else {
        // This is the manual parsing for an author. It is a little over two times faster than fully parsing the JSON.
        for line in reader.split(b'\n') {
            let line = line.unwrap();

            for i in 0..line.len() {
                let remaining = &line[i..];
                if remaining.starts_with(b"\"author\":\"") {
                    let remaining = &remaining[10..];
                    let end = remaining.iter().position(|&x| x == b'"').unwrap();
                    let author = &remaining[..end];
                    usernames.insert(CompactString::from_utf8(author).unwrap_or_default());
                    break;
                }
            }
        }
    }
    return usernames;
}

/*
* 8 worker threads is about all I can handle with 96GB of RAM.
* Note this is only due to my use of `compact_str` you may need to reduce this if you plan on
* parsing data other than the username.
* Unfortunately, the only way to figure out if you are going to run out of RAM is by running the
* process to the end.
* I found that on the largest files, each thread will consume about 12GB of RAM. Making 16GB the
* bare mininum amount of RAM to run this tool.
*/
const TASK_COUNT: usize = 8; // set this
#[tokio::main(flavor = "multi_thread")]
pub async fn main() {
    let directory_path = std::env::args()
        .nth(1)
        .expect("Please provide a path to process.");
    let outfile = std::env::args()
        .nth(2)
        .expect("Please provide an output file path.");
    let use_json: bool = std::env::args()
        .nth(3)
        .expect("please provide either true or false to select the use of the JSON parser")
        .parse()
        .unwrap_or(false);
    println!("Current dir: {}", directory_path);

    let mut compressed_total_size: u64 = 0;

    let mut walk: Vec<walkdir::DirEntry> = vec![];
    for entry in WalkDir::new(directory_path) {
        let path = entry.unwrap();
        if path.file_type().is_file()
            & path
                .path()
                .to_str()
                .unwrap()
                .split('.')
                .last()
                .unwrap_or_default()
                .contains("zst")
        {
            compressed_total_size += path.metadata().unwrap().len();
            walk.push(path);
        }
    }

    println!("Processing files:");
    let start = Instant::now();
    let pb1: Arc<Mutex<ProgressBar>> =
        Arc::new(Mutex::new(ProgressBar::new(compressed_total_size)));
    pb1.lock().await.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
        .unwrap()
        .progress_chars("#>-"));
    let final_set: Arc<Mutex<FxHashSet<CompactString>>> =
        Arc::new(Mutex::new(FxHashSet::default()));
    let mut tasks: Vec<JoinHandle<()>> = vec![];
    for entry in walk {
        let mut task_count: usize = 0;
        while task_count >= TASK_COUNT {
            task_count = 0;
            for task_num in 0..tasks.len() {
                if tasks.get(task_num).unwrap().is_finished() {
                    let _ = tokio::join!(tasks.get_mut(task_num).unwrap());
                } else {
                    task_count += 1;
                }
            }
            pb1.lock().await.tick();
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        let progress = pb1.clone();
        let set = final_set.clone();
        tasks.push(tokio::spawn(async move {
            let current_set = extract_parse(entry.path(), use_json).await;
            set.lock().await.extend(current_set);
            progress.lock().await.inc(entry.metadata().unwrap().len());
        }));
    }

    for task in tasks {
        let _ = tokio::join!(task);
    }

    println!("Done. Processing took {}", start.elapsed().as_secs());

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&outfile)
        .unwrap();
    println!("Writing output file.");
    for author in final_set.lock().await.deref() {
        writeln!(file, "{}", author).unwrap();
    }
    println!("Done");
}
