use std::{
    io, io::prelude::*,
    vec::Vec
};

use itertools::Itertools;
use unicode_segmentation::UnicodeSegmentation;
use snafu::{Snafu, ResultExt, Backtrace};


#[derive(Debug, Snafu)]
pub enum IndexReadError {
    #[snafu_display("{}", "source")]
    IO { source: io::Error, backtrace: Backtrace },
    #[snafu_display("{}", "source")]
    UTF8Read { source: std::string::FromUtf8Error, backtrace: Backtrace}
}

fn read_u32<R>(reader: &mut io::Take<R>) -> Result<u32, IndexReadError>
    where R: io::BufRead
{
    let mut buf = [0 as u8; 4];
    reader.set_limit(4);
    reader.read_exact(&mut buf).context(IO {})?;

    Ok(u32::from_be_bytes(buf))
}

fn read_u8<R>(reader: &mut io::Take<R>) -> Result<u8, IndexReadError>
    where R: io::BufRead
{
    let mut buf = [0 as u8];
    reader.set_limit(1);
    reader.read_exact(&mut buf).context(IO {})?;

    Ok(buf[0])
}

#[derive(PartialEq, Debug, Clone)]
pub struct Document {
    pub content: String
}


use std::collections::HashMap;
type PostingsList = HashMap<String, Vec<usize>>;


#[derive(Default, PartialEq, Debug)]
pub struct Index
{
    // Append only
    docs: Vec<Document>,
    // Terms in postings list are normalized (lowercased for now, more later)
    // TODO: Should probably templatize this later to allow variable numbers
    // but would mean that we need to increment our own counter rather
    // than using the vector size.
    postings: PostingsList
}

impl Index
{
    pub fn add(&mut self, doc: Document) {
        let doc_id = self.docs.len();
        for term in doc.content.to_lowercase().unicode_words() {
            (self.postings.entry(term.to_string()).or_insert_with(Vec::new))
                .push(doc_id);
        }
        self.docs.push(doc);
    }

    pub fn search<'a>(&'a self, query: &str) -> Vec<&'a Document> {
        query.to_lowercase().unicode_words()
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

    pub fn write<W>(&self, writer: W) -> Result<(), io::Error>
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
        writer.write_all(&(self.postings.keys().len() as u32).to_be_bytes())?;

        for (term, doc_ids) in &self.postings {
            // Term length, then term
            let term_bytes = term.as_bytes();
            writer.write_all(&(term_bytes.len() as u8).to_be_bytes())?;
            writer.write_all(&term_bytes[..])?;

            // Number of docs, then the docs
            writer.write_all(&(doc_ids.len() as u32).to_be_bytes())?;
            for doc_id in doc_ids {
                writer.write_all(&(*doc_id as u32).to_be_bytes())?;
            }
        }

        // Write documents
        //
        // Number of documents, then doc length and content pairs
        writer.write_all(&(self.docs.len() as u32).to_be_bytes())?;
        for doc in &self.docs {
            writer.write_all(&(doc.content.len() as u32).to_be_bytes())?;
            writer.write_all(doc.content.as_bytes())?;
        }

        writer.flush()?;

        Ok(())
    }

    pub fn read<R>(reader: R) -> Result<Self, IndexReadError>
        where R: io::Read
    {
        let reader = io::BufReader::new(reader);
        let mut reader = reader.take(4);

        // First, postings size
        let num_terms = read_u32(&mut reader)?;

        let mut postings: PostingsList = PostingsList::with_capacity_and_hasher(num_terms as usize, Default::default());
        for _ in 0..num_terms {
            // Read the size of the term, then the term itself
            let term_size = read_u8(&mut reader)? as usize;

            reader.set_limit(term_size as u64);
            let mut term = String::new();
            reader.read_to_string(&mut term).context(IO {})?;

            // Then the number of doc ids and the doc ids themselves
            let num_doc_ids = read_u32(&mut reader)?;

            let mut doc_ids: Vec<usize> = Vec::with_capacity(num_doc_ids as usize);

            for _ in 0..num_doc_ids {
                doc_ids.push(read_u32(&mut reader)? as usize);
            }

            postings.insert(term, doc_ids);
        }

        let num_docs = read_u32(&mut reader)?;

        let mut docs: Vec<Document> = Vec::with_capacity(num_docs as usize);
        for _ in 0..num_docs {
            let content_size = read_u32(&mut reader)?;

            let mut content = String::new();
            reader.set_limit(content_size as u64);
            reader.read_to_string(&mut content).context(IO {})?;

            docs.push(Document { content })
        }

        Ok(Index { postings, docs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Index tests


    #[test]
    fn add_to_index() {
        let mut idx = Index::default();

        idx.add(Document { content: "hello".to_string() });
        idx.add(Document { content: "world".to_string() });

        assert_eq!(Some(&vec![0]), idx.postings.get("hello"));
        assert_eq!(Some(&vec![1]), idx.postings.get("world"));
    }

    #[test]
    fn search_index() {
        let mut idx = Index::default();
        let dogs = Document { content: String::from("dogs and cats are super cool") };
        let cats_better = Document { content: String::from("but cats are better") };

        idx.add(dogs.clone());
        idx.add(cats_better.clone());
        idx.add(Document { content: String::from("no") });

        assert_eq!(vec![&dogs, &cats_better], idx.search("cats"));
    }

    #[test]
    fn write_with_no_documents() -> Result<(), io::Error> {
        let mut buf = io::Cursor::new(vec![0; 1]);

        let idx = Index::default();
        idx.write(&mut buf)?;

        // No postings (4 bytes) no docs (4 bytes)
        assert_eq!(&[0; 8], &buf.get_ref()[..]);

        Ok(())
    }

    #[test]
    fn write_with_one_doc_and_one_term() -> Result<(), io::Error> {
        let foo = Document { content: String::from("foo") };
        // We create an index with a postings list but no docs
        // for test purposes only. This shouldn't really exist in practice.
        let mut index = Index::default();
        index.add(foo);

        let mut buf = io::Cursor::new(vec![]);
        index.write(&mut buf)?;

        assert_eq!(&[0, 0, 0, 1,        // One posting
                     3,                 // Three letters
                     b'f', b'o', b'o',
                     0, 0, 0, 1,        // One doc_id
                     0, 0, 0, 0,        // Doc 0
                     0, 0, 0, 1,        // One doc
                     0, 0, 0, 3,        // Length of first doc
                     b'f', b'o', b'o'   // The doc content
                    ],
                   &buf.get_ref()[..]);

        Ok(())
    }

    #[test]
    fn read_with_one_doc_and_term() -> Result<(), IndexReadError> {
        let buf =
            // One term in the postings list: foo
            [0, 0, 0, 1,
                3, b'f', b'o', b'o',
            // To one doc, doc_id 0
                0, 0, 0, 1, 0, 0, 0, 0,
            // One stored doc, of length 3
             0, 0, 0, 1,
            // Doc length 3
                0, 0, 0, 3,
            // And the doc content
                b'f', b'o', b'o'];
        let index = Index::read(io::Cursor::new(&buf))?;
        let foo = Document { content: String::from("foo") };
        let mut expected_index = Index::default();
        expected_index.add(foo);

        assert_eq!(expected_index, index);
        Ok(())
    }
}
