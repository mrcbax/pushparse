use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use compact_str::CompactString;
use indicatif::ProgressBar;
use simd_json::derived::ValueObjectAccessAsScalar;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use walkdir::WalkDir;
use zstd::stream::Decoder;

pub async fn extract_parse(input_filename: &Path) -> HashSet<CompactString> {
    let mut usernames: HashSet<CompactString> = HashSet::new();
    let file = File::open(input_filename).unwrap();
    let mut reader = Decoder::with_buffer(BufReader::new(file)).unwrap();
    let _ = reader.window_log_max(31);
    let mut reader = BufReader::new(reader);
    let mut line: String = "".into();
    while reader.read_line(&mut line).unwrap_or_default() > 0 {
        let line_bytes = line.bytes().collect::<Vec<u8>>();
        match simd_json::to_borrowed_value(&mut line_bytes.clone()) {
            Ok(o) => {
                let author = o.get_str("author").unwrap();
                if author.len() <= 20 {
                    usernames.insert(author.into());
                }
            }
            Err(_) => (),
        }
        line.clear();
    }
    return usernames;
}

/*
* 6 worker threads is about all I can handle with 96GB of RAM.
* Unfortunately, the only way to figure out if you are going to run out of RAM is by running the
* process to the end.
* I found that on the largest files, each thread will consume about 12GB of RAM. Making 16GB the
* bare mininum amount of RAM to run this tool.
*/
#[tokio::main(flavor = "multi_thread", worker_threads = 6)] // SEE ABOVE COMMENT ^^^^^
pub async fn main() {
    let directory_path = std::env::args()
        .nth(1)
        .expect("Please provide a path to process.");
    let outfile = std::env::args()
        .nth(2)
        .expect("Please provide an output file path.");
    println!("Current dir: {}", directory_path);

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
            walk.push(path);
        }
    }

    println!("Processing files:");
    let pb1: Arc<Mutex<ProgressBar>> = Arc::new(Mutex::new(ProgressBar::new(walk.len() as u64)));
    let final_set: Arc<Mutex<HashSet<CompactString>>> = Arc::new(Mutex::new(HashSet::new()));
    let tasks: Vec<JoinHandle<()>> = vec![];
    for entry in walk {
        while tasks.len() >= 6 {
            // Set this to the number of tokio workers you set above.
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
        let progress = pb1.clone();
        let set = final_set.clone();
        tokio::spawn(async move {
            let current_set = extract_parse(entry.path()).await;
            set.lock().await.extend(current_set);
            progress.lock().await.inc(1);
        });
    }

    for task in tasks {
        while !task.is_finished() {
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
    }

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&outfile)
        .unwrap();
    for author in final_set.lock().await.deref() {
        writeln!(file, "{}", author).unwrap();
    }
}
