use std::fs;
use std::io::prelude::*;
use std::io;
use std::vec::Vec;
use std::path::PathBuf;
use std::collections::HashMap;
use itertools::Itertools;


use mailparse;

#[derive(Debug)]
enum Error {
    ReadError(io::Error),
    ParseError(mailparse::MailParseError)
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::ReadError(error)
    }
}

impl From<mailparse::MailParseError> for Error {
    fn from(error: mailparse::MailParseError) -> Self {
        Error::ParseError(error)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(PartialEq, Debug, Clone)]
struct Document {
    content: String
}

impl Document {
    // TODO: We should apparently be using P: AsRef<Path> or something similar
    fn from_mail(path: PathBuf) -> Result<Document> {
        let content = fs::read(&path)?;
        let content = mailparse::parse_mail(&content)?.get_body()?.trim().to_string();

        Ok(Document { content })
    }

}

struct Index
{
    // Append only
    docs: Vec<Document>,
    // Terms in postings list are normalized (lowercased for now, more later)
    // TODO: Should probably templatize this later to allow variable numbers
    // but would mean that we need to increment our own counter rather
    // than using the vector size.
    postings: HashMap<String, Vec<usize>>
}

impl Index
{
    fn new() -> Self {
        Index {
            docs: Vec::new(),
            postings: HashMap::new()
        }
    }

    fn add(&mut self, doc: Document) {
        let doc_id = self.docs.len();
        for term in doc.content.to_lowercase().split_whitespace() {
            (self.postings.entry(term.to_string()).or_insert(Vec::new()))
                .push(doc_id);
        }
        self.docs.push(doc);
    }

    fn search<'a>(&'a self, query: &str) -> Vec<&'a Document> {
        query.split_whitespace()
            .unique() // Only non-duplicate tokens
            .map(|tok| self.postings.get(tok))
            .filter(|option| option.is_some())
            // Transform into just unique doc ids
            .flat_map(|option| option.unwrap())
            .unique()
            // Collect the actual documents
            .map(|doc_id| &self.docs[*doc_id])
            .collect()
    }

    fn write<W>(&self, writer: W) -> Result<()>
        where W : io::Write
    {
        let mut writer = io::BufWriter::new(writer);
        // Write postings
        //
        // NOTE: This explicitly casts usize to u32, which is not safe.
        //       We're just doing it here and will do it on read for now.
        //       This isn't correct, but for our purposes is proably good enough.
        //       If we've got more than 2**32 docs to index at one point we should
        //       have implemented index partitioning a while ago.
        //
        // Format:
        //
        // POSTINGS_SIZE:u32 [TERM_SIZE:u8 TERM NUM_DOC_IDS: u32 [u32, u32]], ...
        writer.write(&(self.postings.keys().len() as u32).to_be_bytes());

        for (term, doc_ids) in &self.postings {
            // Term length, then term
            let term_bytes = term.as_bytes();
            writer.write(&(term_bytes.len() as u8).to_be_bytes());
            writer.write(&term_bytes[..]);

            // Number of docs, then the docs
            writer.write(&(doc_ids.len() as u32).to_be_bytes());
            for doc_id in doc_ids {
                writer.write(&(*doc_id as u32).to_be_bytes());
            }
        }

        // Write documents
        //
        // First, convert to bytes and get offsets
        let bytes = self.docs.iter().map(|doc| doc.content.as_bytes());
        let mut offsets: Vec<u32> = vec![];
        let mut sum: u32 = 0;
        for (_, val) in bytes.enumerate() {
            offsets.push(sum);
            sum += val.len() as u32;
        }


        // List of offsets into content, then content
        writer.write(&(self.docs.len() as u32).to_be_bytes());
        for offset in &offsets {
            writer.write(&offset.to_be_bytes());
        }

        for doc in &self.docs {
            writer.write(doc.content.as_bytes());
        }

        writer.flush();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use super::*;

    fn email_path(name: &str) -> PathBuf {
        [env::var("CARGO_MANIFEST_DIR").unwrap().as_str(),
         "tests/fixtures/",
         name].iter().collect()
    }


    // Document tests


    #[test]
    fn from_mail_with_real_email() -> Result<()> {
        let d = Document::from_mail(email_path("1.eml"))?;
        assert_eq!("Please let me know if you still need Curve Shift.\n\nThanks,\nHeather", d.content);
        Ok(())
    }


    // Index tests


    #[test]
    fn add_to_index() -> Result<()> {
        let mut idx = Index::new();

        let d = Document::from_mail(email_path("1.eml"))?;

        idx.add(d);

        assert_eq!(Some(&vec![0]), idx.postings.get("please"));
        Ok(())
    }

    #[test]
    fn search_index() -> Result<()> {
        let mut idx = Index::new();
        let dogs = Document { content: String::from("dogs and cats are super cool") };
        let cats_better = Document { content: String::from("but cats are better") };

        idx.add(dogs.clone());
        idx.add(cats_better.clone());
        idx.add(Document { content: String::from("no") });

        assert_eq!(vec![&dogs, &cats_better], idx.search("cats"));
        Ok(())
    }

    #[test]
    fn write_with_no_documents() {
        let mut buf = io::Cursor::new(vec![0; 1]);

        Index::new().write(&mut buf);

        // No postings (4 bytes) no docs (4 bytes)
        assert_eq!(&[0; 8], &buf.get_ref()[..]);
    }

    #[test]
    fn write_with_one_doc_and_one_term() {
        let foo = Document { content: String::from("foo") };
        // We create an index with a postings list but no docs
        // for test purposes only. This shouldn't really exist in practice.
        let mut index = Index::new();
        index.add(foo);

        let mut buf = io::Cursor::new(vec![]);
        index.write(&mut buf);

        assert_eq!(&[0, 0, 0, 1,        // One posting
                     3,                 // Three letters
                     b'f', b'o', b'o',
                     0, 0, 0, 1,        // One doc_id
                     0, 0, 0, 0,        // Doc 0
                     0, 0, 0, 1,        // One doc
                     0, 0, 0, 0,        // Offset into first doc is 0
                     b'f', b'o', b'o'   // The doc content
                    ],
                   &buf.get_ref()[..]);
    }
}
