#[cfg(feature = "std")]
use alloc::string::String;
use alloc::vec::Vec;
use crate::AHashMap;

/// A simple lock-free compatible Inverted Index for Information Retrieval (IR).
/// Maps text tokens (words) to a list of Entity IDs (e.g. Partition keys).
#[derive(Clone, Default)]
pub struct InvertedIndex {
    /// Maps a token string to a sorted list of entity IDs.
    index: AHashMap<String, Vec<usize>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            index: AHashMap::default(),
        }
    }

    /// Basic tokenizer: splits text by whitespace, lowercases, and removes punctuation.
    fn tokenize(text: &str) -> Vec<String> {
        text.split_whitespace()
            .map(|s| {
                s.chars()
                    .filter(|c| c.is_alphanumeric())
                    .collect::<String>()
                    .to_lowercase()
            })
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Indexes a string attribute for a specific entity ID.
    pub fn insert_document(&mut self, entity_id: usize, text: &str) {
        let tokens = Self::tokenize(text);
        for token in tokens {
            if !self.index.contains_key(&token) {
                self.index.insert(token.clone(), Vec::new());
            }
            if let Some(postings) = self.index.get_mut(&token) {
                // Maintain sorted order for efficient intersection later
                if let Err(pos) = postings.binary_search(&entity_id) {
                    postings.insert(pos, entity_id);
                }
            }
        }
    }

    /// Removes an entity ID from the inverted index.
    pub fn remove_document(&mut self, entity_id: usize, text: &str) {
        let tokens = Self::tokenize(text);
        for token in tokens {
            if let Some(postings) = self.index.get_mut(&token)
                && let Ok(pos) = postings.binary_search(&entity_id) {
                    postings.remove(pos);
                }
        }
    }

    /// Performs a boolean AND search across multiple tokens.
    /// Returns a list of entity IDs that match all tokens.
    pub fn search_and(&self, query: &str) -> Vec<usize> {
        let tokens = Self::tokenize(query);
        if tokens.is_empty() {
            return Vec::new();
        }

        let mut iter = tokens.iter().filter_map(|t| self.index.get(t));
        
        let mut result = match iter.next() {
            Some(first) => first.clone(),
            None => return Vec::new(),
        };

        for postings in iter {
            // Intersect `result` with `postings`
            result.retain(|id| postings.binary_search(id).is_ok());
            if result.is_empty() {
                break;
            }
        }

        result
    }

    /// Performs a boolean OR search.
    pub fn search_or(&self, query: &str) -> Vec<usize> {
        let tokens = Self::tokenize(query);
        let mut result_set = Vec::new();

        for token in tokens {
            if let Some(postings) = self.index.get(&token) {
                for id in postings {
                    if let Err(pos) = result_set.binary_search(id) {
                        result_set.insert(pos, *id);
                    }
                }
            }
        }

        result_set
    }
}
