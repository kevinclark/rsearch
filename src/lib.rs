use std::fs;
use std::io;
use std::path::PathBuf;
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
}
