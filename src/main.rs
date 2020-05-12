use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
    thread,
    sync::{Arc, Mutex, mpsc::channel}
};

use rsearch::{Document, Index};

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
    .get_matches();

    if let Some(matches) = matches.subcommand_matches("create") {
        let input_dir = matches.value_of("input_dir").expect("input_dir required");
        let output_file = matches.value_of("output_file").expect("output_file required");
        let output_file = fs::File::create(output_file).expect("Unable to open output file");

        let mut index = Index::default();

        let start = Instant::now();

        let (sender, receiver) = channel();

        let walker = WalkDir::new(input_dir).into_iter();
        let paths: Vec<PathBuf> = walker.filter_entry(|e| !is_hidden(e))
                            .filter(|e| !e.as_ref().expect("Path entry in filter blew up").file_type().is_dir())
                            .map(|e| PathBuf::from(e.expect("Path entry in map blew up").path()))
                            .collect();
        let paths = Arc::new(Mutex::new(paths));

        let mut handles: Vec<thread::JoinHandle<_>> = Vec::new();
        for _ in 0..20 {
            let (paths, tx) = (Arc::clone(&paths), sender.clone());
            handles.push(thread::spawn(move || {
                //let mut paths = paths.lock().expect("Mutex blew up");

                while let Some(path) = { let x = (*paths.lock().expect("Mutex blew up")).pop(); x } {
                    if let Ok(content) = mail_content(&path.as_path()) {
                        tx.send(content).expect("Send failed");
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Join failed");
        }

        drop(sender);

        while let Ok(content) = receiver.recv() {
            index.add(Document { content });
        }

        println!("Done reading at {:?}", start.elapsed());

        index.write(output_file)?;

        println!("Done writing at {:?}", start.elapsed());
    }

    Ok(())
}
