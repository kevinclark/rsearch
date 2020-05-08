use std::fs;
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
}
