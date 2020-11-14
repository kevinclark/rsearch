use std::{collections::HashSet, convert::TryFrom, io, io::prelude::*, vec::Vec};

use itertools::Itertools;
use snafu::{ResultExt, Snafu};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug, Snafu)]
pub enum IndexError {
    UnableToReadPostingListSize {
        source: io::Error,
    },
    UnableToReadTermSize {
        term_id: u32,
        source: io::Error,
    },
    UnableToReadTerm {
        term_id: u32,
        source: io::Error,
    },
    UnableToReadNumberOfDocIds {
        term: String,
        term_id: u32,
        source: io::Error,
    },
    UnableToReadDocId {
        term: String,
        term_id: u32,
        doc_index: u32,
        source: io::Error,
    },
    UnableToReadNumberOfDocs {
        source: io::Error,
    },
    UnableToReadDocSize {
        doc_id: u32,
        source: io::Error,
    },
    UnableToReadDocContent {
        doc_id: u32,
        source: io::Error,
    },
}

fn read_u32(reader: &mut impl io::BufRead) -> Result<u32, io::Error> {
    let mut buf = [0 as u8; 4];
    reader.read_exact(&mut buf)?;

    Ok(u32::from_be_bytes(buf))
}

fn read_u16(reader: &mut impl io::BufRead) -> Result<u16, io::Error> {
    let mut buf = [0 as u8; 2];
    reader.read_exact(&mut buf)?;

    Ok(u16::from_be_bytes(buf))
}

use std::collections::HashMap;
type PostingsList = HashMap<String, Vec<usize>>;

#[derive(PartialEq, Debug)]
pub struct Document {
    pub content: String,
}

type TermList = HashSet<String>;

pub struct AnalyzedDocument {
    terms: TermList,
    content: String,
}

pub fn analyze(content: String) -> AnalyzedDocument {
    let terms = content
        .unicode_words()
        .map(|w| w.to_lowercase().to_string())
        .collect();
    AnalyzedDocument { terms, content }
}

#[derive(Debug, PartialEq)]
pub struct Index {
    // Append only
    docs: Vec<Document>, // Doc ID comes from placement here
    // Terms in postings list are normalized (lowercased for now, more later)
    // TODO: Should probably templatize this later to allow variable numbers
    // but would mean that we need to increment our own counter rather
    // than using the vector size.
    postings: PostingsList,
}

impl Index {
    pub fn read<R>(reader: R) -> Result<Self, IndexError>
    where
        R: io::Read,
    {
        let mut reader = io::BufReader::new(reader);

        // First, postings size
        let num_terms = read_u32(&mut reader).context(UnableToReadPostingListSize)?;
        let mut postings =
            PostingsList::with_capacity_and_hasher(num_terms as usize, Default::default());

        for term_id in 0..num_terms {
            // Read the size of the term, then the term itself
            let term_size = read_u16(&mut reader).context(UnableToReadTermSize { term_id })?;

            let mut term = String::new();
            {
                let mut limited_reader = reader.by_ref().take(term_size as u64);
                limited_reader
                    .read_to_string(&mut term)
                    .context(UnableToReadTerm { term_id })?;
            }

            // Then the number of doc ids and the doc ids themselves
            let num_doc_ids = read_u32(&mut reader).context(UnableToReadNumberOfDocIds {
                term: &term,
                term_id,
            })?;

            let mut doc_ids: Vec<usize> = Vec::with_capacity(num_doc_ids as usize);

            for doc_index in 0..num_doc_ids {
                let doc_id = read_u32(&mut reader).context(UnableToReadDocId {
                    term: &term,
                    term_id,
                    doc_index,
                })?;
                doc_ids.push(doc_id as usize);
            }

            postings.insert(term, doc_ids);
        }

        let num_docs = read_u32(&mut reader).context(UnableToReadNumberOfDocs)?;

        let mut docs: Vec<Document> = Vec::with_capacity(num_docs as usize);
        for doc_id in 0..num_docs {
            let content_size = read_u32(&mut reader).context(UnableToReadDocSize { doc_id })?;

            let mut content = String::new();
            {
                let mut limited_reader = reader.by_ref().take(content_size as u64);
                limited_reader
                    .read_to_string(&mut content)
                    .context(UnableToReadDocContent { doc_id })?;
            }

            docs.push(Document { content })
        }

        Ok(Index { postings, docs })
    }

    pub fn search<'a>(&'a self, query: &str) -> Vec<&'a Document> {
        query
            .to_lowercase()
            .unicode_words()
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

#[derive(Debug, Snafu)]
pub enum IndexWriterError {
    UnableToDownCastPostingsLength {
        len: usize,
        source: core::num::TryFromIntError,
    },
    UnableToWritePostingsLength {
        source: io::Error,
    },

    UnableToDownCastTermLength {
        term: String,
        len: usize,
        source: core::num::TryFromIntError,
    },
    UnableToWriteTermLength {
        term: String,
        source: io::Error,
    },
    UnableToWriteTerm {
        term: String,
        source: io::Error,
    },

    UnableToDownCastNumberOfDocIds {
        num_docs: usize,
        source: core::num::TryFromIntError,
    },
    UnableToWriteNumberOfDocIds {
        num_docs: u32,
        source: io::Error,
    },

    UnableToDownCastDocId {
        doc_id: usize,
        source: core::num::TryFromIntError,
    },
    UnableToWriteDocId {
        doc_id: u32,
        source: io::Error,
    },

    UnableToDownCastNumberOfDocs {
        num_docs: usize,
        source: core::num::TryFromIntError,
    },
    UnableToWriteNumberOfDocs {
        num_docs: u32,
        source: io::Error,
    },

    UnableToDownCastDocLength {
        content_len: usize,
        source: core::num::TryFromIntError,
    },
    UnableToWriteDocLength {
        content_len: u32,
        source: io::Error,
    },
    // TODO: include path?
    UnableToWriteDoc {
        source: io::Error,
    },

    UnableToFlush {
        source: io::Error,
    },
}

#[derive(PartialEq, Debug, Default)]
pub struct IndexWriter {
    docs: Vec<Document>,
    postings: PostingsList,
}

impl From<IndexWriter> for Index {
    fn from(writer: IndexWriter) -> Self {
        Index {
            docs: writer.docs,
            postings: writer.postings,
        }
    }
}

impl IndexWriter {
    pub fn add(&mut self, doc: AnalyzedDocument) {
        let doc_id = self.docs.len();
        for term in doc.terms {
            (self
                .postings
                .entry(term.to_string())
                .or_insert_with(Vec::new))
            .push(doc_id);
        }
        self.docs.push(Document {
            content: doc.content.to_string(),
        });
    }

    pub fn write<W>(&self, writer: W) -> Result<(), IndexWriterError>
    where
        W: io::Write,
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
        let postings_len = self.postings.keys().len();
        let postings_len = u32::try_from(postings_len)
            .context(UnableToDownCastPostingsLength { len: postings_len })?;
        writer
            .write_all(&postings_len.to_be_bytes())
            .context(UnableToWritePostingsLength)?;

        for (term, doc_ids) in &self.postings {
            // Term length, then term
            let term_bytes = term.as_bytes();
            let term_length = term_bytes.len();
            let term_length = u16::try_from(term_length).context(UnableToDownCastTermLength {
                term: term,
                len: term_length,
            })?;

            writer
                .write_all(&term_length.to_be_bytes())
                .context(UnableToWriteTermLength { term: term })?;
            writer
                .write_all(&term_bytes[..])
                .context(UnableToWriteTerm { term: term })?;

            // Number of docs, then the docs
            let num_docs = doc_ids.len();
            let num_docs =
                u32::try_from(num_docs).context(UnableToDownCastNumberOfDocIds { num_docs })?;
            writer
                .write_all(&num_docs.to_be_bytes())
                .context(UnableToWriteNumberOfDocIds { num_docs })?;

            for doc_id in doc_ids {
                let doc_id =
                    u32::try_from(*doc_id).context(UnableToDownCastDocId { doc_id: *doc_id })?;
                writer
                    .write_all(&doc_id.to_be_bytes())
                    .context(UnableToWriteDocId { doc_id })?;
            }
        }

        // Write documents
        //
        // Number of documents, then doc length and content pairs
        let num_docs = self.docs.len();
        let num_docs =
            u32::try_from(num_docs).context(UnableToDownCastNumberOfDocs { num_docs })?;
        writer
            .write_all(&num_docs.to_be_bytes())
            .context(UnableToWriteNumberOfDocs { num_docs })?;

        for doc in &self.docs {
            let content_len = doc.content.len();
            let content_len =
                u32::try_from(content_len).context(UnableToDownCastDocLength { content_len })?;
            writer
                .write_all(&content_len.to_be_bytes())
                .context(UnableToWriteDocLength { content_len })?;
            writer
                .write_all(doc.content.as_bytes())
                .context(UnableToWriteDoc)?;
        }

        writer.flush().context(UnableToFlush)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Index tests

    #[test]
    fn add_to_index() {
        let mut idx = IndexWriter::default();

        idx.add(analyze("hello".to_string()));
        idx.add(analyze("world".to_string()));

        assert_eq!(Some(&vec![0]), idx.postings.get("hello"));
        assert_eq!(Some(&vec![1]), idx.postings.get("world"));
    }

    #[test]
    fn search_index() {
        let mut idx = IndexWriter::default();
        let dogs = analyze("dogs and cats are super cool".to_string());
        let cats_better = analyze("but cats are better".to_string());

        let expected = vec![dogs.content.clone(), cats_better.content.clone()];

        idx.add(dogs);
        idx.add(cats_better);
        idx.add(analyze("no".to_string()));

        let results: Vec<String> = Index::from(idx)
            .search("cats")
            .iter()
            .map(|d| d.content.clone())
            .collect();
        assert_eq!(expected, results);
    }

    #[test]
    fn write_with_no_documents() -> Result<(), IndexWriterError> {
        let mut buf = io::Cursor::new(vec![0; 1]);

        let idx = IndexWriter::default();
        idx.write(&mut buf)?;

        // No postings (4 bytes) no docs (4 bytes)
        assert_eq!(&[0; 8], &buf.get_ref()[..]);

        Ok(())
    }

    #[test]
    fn write_with_one_doc_and_one_term() -> Result<(), IndexWriterError> {
        // We create an index with a postings list but no docs
        // for test purposes only. This shouldn't really exist in practice.
        let mut index = IndexWriter::default();
        index.add(analyze("foo".to_string()));

        let mut buf = io::Cursor::new(vec![]);
        index.write(&mut buf)?;

        assert_eq!(
            &[
                0, 0, 0, 1, // One posting
                0, 3, // Three letters
                b'f', b'o', b'o', 0, 0, 0, 1, // One doc_id
                0, 0, 0, 0, // Doc 0
                0, 0, 0, 1, // One doc
                0, 0, 0, 3, // Length of first doc
                b'f', b'o', b'o' // The doc content
            ],
            &buf.get_ref()[..]
        );

        Ok(())
    }

    #[test]
    fn read_with_one_doc_and_term() -> Result<(), IndexError> {
        let buf =
            // One term in the postings list: foo
            [0, 0, 0, 1,
                0, 3,
                b'f', b'o', b'o',
            // To one doc, doc_id 0
                0, 0, 0, 1, 0, 0, 0, 0,
            // One stored doc, of length 3
             0, 0, 0, 1,
            // Doc length 3
                0, 0, 0, 3,
            // And the doc content
                b'f', b'o', b'o'];
        let index = Index::read(io::Cursor::new(&buf))?;
        let mut expected_index = IndexWriter::default();
        expected_index.add(analyze("foo".to_string()));
        let expected_index = Index::from(expected_index);

        assert_eq!(expected_index, index);
        Ok(())
    }
}
