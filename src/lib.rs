use std::fs;
use std::io;
use std::vec::Vec;
use std::path::PathBuf;
use std::collections::HashMap;


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

struct Document {
    path: PathBuf, // PathBuf?
    content: String
}

impl Document {
    // TODO: We should apparently be using P: AsRef<Path> or something similar
    fn from_mail(path: PathBuf) -> Result<Document> {
        let content = fs::read(&path)?;
        let content = mailparse::parse_mail(&content)?.get_body()?.trim().to_string();
        Ok(Document { path, content })
    }
}

struct Index<T>
    where T: Fn(&str) -> Vec<&str>
{
    tokenizer: T,
    // Append only
    docs: Vec<Document>,
    // Should probably templatize this later to allow variable numbers
    // but would mean that we need to increment our own counter rather
    // than using the vector size.
    postings: HashMap<String, Vec<usize>>
}

impl<T> Index<T>
    where T: Fn(&str) -> Vec<&str>
{
    fn with_tokenizer(tokenizer: T) -> Self {
        Index {
            tokenizer: tokenizer,
            docs: Vec::new(),
            postings: HashMap::new()
        }
    }

    fn add(&mut self, doc: Document) {
        let doc_id = self.docs.len();
        for term in (self.tokenizer)(&doc.content) {
            (self.postings.entry(term.to_string()).or_insert(Vec::new()))
                .push(doc_id);
        }
        self.docs.push(doc);
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
        let mut idx = Index::with_tokenizer(|s: &str| s.split_whitespace().collect() );

        let d = Document::from_mail(email_path("1.eml"))?;

        idx.add(d);

        assert_eq!(Some(&vec![0]), idx.postings.get("Please"));
        Ok(())
    }
}
