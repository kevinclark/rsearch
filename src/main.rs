use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    time::Instant,
    thread,
    sync::{Arc, Mutex, mpsc::channel}
};

use rsearch::{Index, IndexWriter};

use clap::{crate_authors, crate_description, crate_name, crate_version, App, SubCommand, Arg};
use mailparse;
use walkdir::{DirEntry, WalkDir};

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with('.'))
         .unwrap_or(false)
}

fn mail_content(path: &Path) -> Result<String, mailparse::MailParseError> {
    let content = fs::read(path).unwrap();
    Ok(mailparse::parse_mail(&content)?.get_body()?.trim().to_string())
}

fn main() -> std::result::Result<(), std::io::Error> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
        //.subcommand(SubCommand::with_name("query")
        .subcommand(SubCommand::with_name("create")
                        .arg(Arg::with_name("input_dir")
                             .help("The directory to walk to find mail dirs")
                             .required(true))
                        .arg(Arg::with_name("output_file")
                             .help("The name of the index file")
                             .required(true)))
        .subcommand(SubCommand::with_name("search")
                        .arg(Arg::with_name("index_file")
                             .help("The name of the index file")
                             .required(true))
                        .arg(Arg::with_name("query")
                             .help("What to search for")
                             .required(true)))
    .get_matches();

    if let Some(matches) = matches.subcommand_matches("search") {
        let query = matches.value_of("query").expect("query required");
        let index_file = matches.value_of("index_file").expect("index_file required");
        let index_file = fs::File::open(index_file).expect("Unable to open index file");
        let index = Index::read(index_file).expect("Unable to read index");

        for doc in index.search(&query) {
            println!("{}", doc.content);
        }
    } else if let Some(matches) = matches.subcommand_matches("create") {
        let input_dir = matches.value_of("input_dir").expect("input_dir required");
        let output_file = matches.value_of("output_file").expect("output_file required");
        let output_file = fs::File::create(output_file).expect("Unable to open output file");

        let mut index = IndexWriter::default();

        let start = Instant::now();

        let walker = WalkDir::new(input_dir).into_iter();
        let paths: Vec<PathBuf> = walker.filter_entry(|e| !is_hidden(e))
                            .filter(|e| !e.as_ref().expect("Path entry in filter blew up").file_type().is_dir())
                            .map(|e| PathBuf::from(e.expect("Path entry in map blew up").path()))
                            .collect();
        let paths = Arc::new(Mutex::new(paths));
        let content_queue = Arc::new(Mutex::new(VecDeque::new()));
        let analyzed_queue = Arc::new(Mutex::new(VecDeque::new()));

        println!("Paths collected at {:?}", start.elapsed());

        let mut handles: Vec<thread::JoinHandle<_>> = Vec::new();
        for _ in 0..20 {
            let paths = Arc::clone(&paths);
            let content_queue = Arc::clone(&content_queue);
            let analyzed_queue = Arc::clone(&analyzed_queue);
            handles.push(thread::spawn(move || {

                while let Some(path) = { let x = (*paths.lock().unwrap()).pop(); x } {
                    if let Ok(content) = mail_content(&path.as_path()) {
                        content_queue.lock().unwrap().push_back(content);
                    }
                }

                while let Some(content) = { let x = (*content_queue.lock().unwrap()).pop_front(); x } {
                    let analyzed = rsearch::analyze(content);
                    analyzed_queue.lock().unwrap().push_back(analyzed);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Join failed");
        }

        println!("Done parsing at {:?}", start.elapsed());

        let analyzed_queue = Arc::try_unwrap(analyzed_queue).ok().unwrap().into_inner().unwrap();

        for analyzed in analyzed_queue {
            index.add(analyzed);
        }

        println!("Done reading at {:?}", start.elapsed());

        index.write(output_file).expect("Unable to write file");

        println!("Done writing at {:?}", start.elapsed());
    }

    Ok(())
}
