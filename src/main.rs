use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use rsearch::{Document, Index};

use clap::{crate_authors, crate_description, crate_name, crate_version, App, SubCommand, Arg};
use mailparse;
use walkdir::{DirEntry, WalkDir};

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
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
        let input_dir = matches.value_of("input_dir").unwrap();
        let output_file = matches.value_of("output_file").unwrap();
        let output_file = fs::File::create(output_file).expect("Unable to open output file");

        let mut index: Index = Default::default();

        let start = Instant::now();

        let walker = WalkDir::new(input_dir).into_iter();
        for entry in walker.filter_entry(|e| !is_hidden(e)) {
            let entry = entry.unwrap();
            if !entry.file_type().is_dir() {
                match mail_content(entry.path()) {
                    Ok(content) => {
                        index.add(Document { content });
                        // print!(".");
                    }
                    Err(_) => {} // print!("-")
                }
            }
        }

        println!("Done reading in {:?}", start.elapsed());

        index.write(output_file);

        println!("Done writing in {:?}", start.elapsed());
    }

    Ok(())
}
