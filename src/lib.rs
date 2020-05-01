use std::fs;
use std::io;
use std::vec::Vec;
use std::path::PathBuf;
use std::collections::HashMap;


use mailparse;

#[derive(Debug)]
enum DocumentError {
    ReadError(io::Error),
    ParseError(mailparse::MailParseError)
}

impl From<io::Error> for DocumentError {
    fn from(error: io::Error) -> Self {
        DocumentError::ReadError(error)
    }
}

impl From<mailparse::MailParseError> for DocumentError {
    fn from(error: mailparse::MailParseError) -> Self {
        DocumentError::ParseError(error)
    }
}

struct Document {
    path: PathBuf, // PathBuf?
    content: String
}

impl Document {
    // TODO: We should apparently be using P: AsRef<Path> or something similar
    fn from_mail(path: PathBuf) -> Result<Document, DocumentError> {
        let content = fs::read(&path)?;
        let content = mailparse::parse_mail(&content)?.get_body()?.trim().to_string();
        Ok(Document { path, content })
    }
}

struct Index<'a, T, I>
    where T: Fn(&'a str) -> I,
          I: Iterator<Item = &'a str>
{
    tokenizer: T,
    // Append only
    docs: Vec<&'a Document>,
    // Should probably templatize this later to allow variable numbers
    // but would mean that we need to increment our own counter rather
    // than using the vector size.
    postings: HashMap<String, Vec<usize>>
}

impl<'a, T, I> Index<'a, T, I>
    where T: Fn(&'a str) -> I,
          I: Iterator<Item = &'a str>
{
    fn add(&mut self, doc: &'a Document) {
        let doc_id = self.docs.len();
        self.docs.push(doc);
        for term in (self.tokenizer)(&doc.content) {
            (self.postings.entry(term.to_string()).or_insert(Vec::new())).push(doc_id);
        }
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


    #[test]
    fn from_mail_with_real_email() -> Result<(), DocumentError> {
        let d = Document::from_mail(email_path("1.eml"))?;
        assert_eq!("Please let me know if you still need Curve Shift.\n\nThanks,\nHeather", d.content);
        Ok(())
    }

    #[test]
    fn add_to_index() -> Result<(), DocumentError> {
        let mut idx = Index {
            tokenizer: |s: &str| s.split_whitespace(),
            docs: Vec::new(),
            postings: HashMap::new()
        };

        let d = Document::from_mail(email_path("1.eml"))?;

        idx.add(&d);

        assert_eq!(Some(&vec![0]), idx.postings.get("Please"));
        Ok(())
    }
}
