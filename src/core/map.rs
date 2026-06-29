use ahash::RandomState;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};

#[derive(Clone, Debug)]
pub enum Bucket<K, V> {
    Empty,
    Deleted,
    Occupied(K, V),
}

#[derive(Clone, Debug)]
pub struct AHashMap<K, V, S = RandomState> {
    hasher: S,
    table: Vec<Bucket<K, V>>,
    len: usize,
    deleted_count: usize,
}

impl<K, V> Default for AHashMap<K, V, RandomState> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> AHashMap<K, V, RandomState> {
    pub fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            table: Vec::new(),
            len: 0,
            deleted_count: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let cap = (capacity * 3 / 2).next_power_of_two().max(8);
        let mut table = Vec::with_capacity(cap);
        for _ in 0..cap {
            table.push(Bucket::Empty);
        }
        Self {
            hasher: RandomState::new(),
            table,
            len: 0,
            deleted_count: 0,
        }
    }
}

impl<K, V, S> AHashMap<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            hasher,
            table: Vec::new(),
            len: 0,
            deleted_count: 0,
        }
    }
}

impl<K, V, S> AHashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.table.is_empty() || (self.len + self.deleted_count + 1) * 10 >= self.table.len() * 7
        {
            let new_cap = ((self.len + 1) * 2).next_power_of_two().max(8);
            self.resize(new_cap);
        }
        self.insert_no_resize(key, value)
    }

    fn insert_no_resize(&mut self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);

        let cap = self.table.len();
        let mask = cap - 1;
        let mut idx = hash as usize & mask;
        let mut first_deleted = None;

        loop {
            match &mut self.table[idx] {
                Bucket::Empty => {
                    if let Some(del_idx) = first_deleted {
                        self.table[del_idx] = Bucket::Occupied(key, value);
                        self.deleted_count -= 1;
                        self.len += 1;
                        return None;
                    } else {
                        self.table[idx] = Bucket::Occupied(key, value);
                        self.len += 1;
                        return None;
                    }
                }
                Bucket::Deleted => {
                    if first_deleted.is_none() {
                        first_deleted = Some(idx);
                    }
                }
                Bucket::Occupied(k, v) => {
                    if k == &key {
                        return Some(core::mem::replace(v, value));
                    }
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if self.table.is_empty() {
            return None;
        }

        let hash = self.hasher.hash_one(key);

        let cap = self.table.len();
        let mask = cap - 1;
        let mut idx = hash as usize & mask;

        loop {
            match &self.table[idx] {
                Bucket::Empty => return None,
                Bucket::Deleted => {}
                Bucket::Occupied(k, v) => {
                    if k.borrow() == key {
                        return Some(v);
                    }
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if self.table.is_empty() {
            return None;
        }

        let hash = self.hasher.hash_one(key);

        let cap = self.table.len();
        let mask = cap - 1;
        let mut idx = hash as usize & mask;

        let found_idx = loop {
            match &self.table[idx] {
                Bucket::Empty => return None,
                Bucket::Deleted => {}
                Bucket::Occupied(k, _) => {
                    if k.borrow() == key {
                        break idx;
                    }
                }
            }
            idx = (idx + 1) & mask;
        };

        match &mut self.table[found_idx] {
            Bucket::Occupied(_, v) => Some(v),
            _ => unreachable!(),
        }
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if self.table.is_empty() {
            return None;
        }

        let hash = self.hasher.hash_one(key);

        let cap = self.table.len();
        let mask = cap - 1;
        let mut idx = hash as usize & mask;

        let found_idx = loop {
            match &self.table[idx] {
                Bucket::Empty => return None,
                Bucket::Deleted => {}
                Bucket::Occupied(k, _) => {
                    if k.borrow() == key {
                        break idx;
                    }
                }
            }
            idx = (idx + 1) & mask;
        };

        let old = core::mem::replace(&mut self.table[found_idx], Bucket::Deleted);
        self.len -= 1;
        self.deleted_count += 1;
        if let Bucket::Occupied(_, v) = old {
            Some(v)
        } else {
            None
        }
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    pub fn clear(&mut self) {
        self.table.clear();
        self.len = 0;
        self.deleted_count = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn resize(&mut self, new_cap: usize) {
        let mut new_table = Vec::with_capacity(new_cap);
        for _ in 0..new_cap {
            new_table.push(Bucket::Empty);
        }

        let old_table = core::mem::replace(&mut self.table, new_table);
        self.len = 0;
        self.deleted_count = 0;

        for bucket in old_table {
            if let Bucket::Occupied(k, v) = bucket {
                self.insert_no_resize(k, v);
            }
        }
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            iter: self.table.iter(),
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        IterMut {
            iter: self.table.iter_mut(),
        }
    }

    pub fn keys(&self) -> Keys<'_, K, V> {
        Keys { iter: self.iter() }
    }

    pub fn values(&self) -> Values<'_, K, V> {
        Values { iter: self.iter() }
    }
}

pub struct Iter<'a, K, V> {
    iter: core::slice::Iter<'a, Bucket<K, V>>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Bucket::Occupied(k, v)) => return Some((k, v)),
                Some(_) => {}
                None => return None,
            }
        }
    }
}

pub struct IterMut<'a, K, V> {
    iter: core::slice::IterMut<'a, Bucket<K, V>>,
}

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Bucket::Occupied(k, v)) => return Some((k, v)),
                Some(_) => {}
                None => return None,
            }
        }
    }
}

pub struct Keys<'a, K, V> {
    iter: Iter<'a, K, V>,
}

impl<'a, K, V> Iterator for Keys<'a, K, V> {
    type Item = &'a K;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

pub struct Values<'a, K, V> {
    iter: Iter<'a, K, V>,
}

impl<'a, K, V> Iterator for Values<'a, K, V> {
    type Item = &'a V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, v)| v)
    }
}

impl<'a, K, V, S> IntoIterator for &'a AHashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, K, V, S> IntoIterator for &'a mut AHashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'a K, &'a mut V);
    type IntoIter = IterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

pub struct IntoIter<K, V> {
    iter: alloc::vec::IntoIter<Bucket<K, V>>,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Bucket::Occupied(k, v)) => return Some((k, v)),
                Some(_) => {}
                None => return None,
            }
        }
    }
}

impl<K, V, S> IntoIterator for AHashMap<K, V, S> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            iter: self.table.into_iter(),
        }
    }
}

impl<K, V> FromIterator<(K, V)> for AHashMap<K, V, RandomState>
where
    K: Eq + Hash,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut map = Self::new();
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use alloc::string::String;
    use alloc::string::ToString;
    use alloc::vec;
    use alloc::vec::Vec;

    #[test]
    fn test_ahashmap_all_apis() {
        let mut map: AHashMap<String, i32> = AHashMap::default();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        // Test with_capacity
        let map_cap: AHashMap<String, i32> = AHashMap::with_capacity(16);
        assert!(map_cap.is_empty());

        // Test with_hasher
        let map_hasher: AHashMap<String, i32> = AHashMap::with_hasher(RandomState::new());
        assert!(map_hasher.is_empty());

        // Test insert and get
        assert_eq!(map.insert("a".to_string(), 1), None);
        assert_eq!(map.len(), 1);
        assert!(!map.is_empty());
        assert_eq!(map.insert("a".to_string(), 2), Some(1));
        assert_eq!(map.get("a"), Some(&2));

        // Test contains_key
        assert!(map.contains_key("a"));
        assert!(!map.contains_key("b"));

        // Test get_mut
        if let Some(val) = map.get_mut("a") {
            *val = 3;
        }
        assert_eq!(map.get("a"), Some(&3));

        // Test remove
        assert_eq!(map.remove("a"), Some(3));
        assert_eq!(map.remove("a"), None);
        assert_eq!(map.len(), 0);

        // Test resize/rehashing (insert many keys)
        for i in 0..100 {
            map.insert(format!("key_{}", i), i);
        }
        assert_eq!(map.len(), 100);
        for i in 0..100 {
            assert_eq!(map.get(&format!("key_{}", i)), Some(&i));
        }

        // Test iter
        let count = map.iter().count();
        assert_eq!(count, 100);

        // Test iter_mut
        for (k, v) in map.iter_mut() {
            if k.starts_with("key_") {
                *v += 1;
            }
        }
        for i in 0..100 {
            assert_eq!(map.get(&format!("key_{}", i)), Some(&(i + 1)));
        }

        // Test keys and values
        let keys: Vec<&String> = map.keys().collect();
        assert_eq!(keys.len(), 100);
        let values: Vec<&i32> = map.values().collect();
        assert_eq!(values.len(), 100);

        // Test IntoIterator for &AHashMap
        let mut ref_count = 0;
        for _ in &map {
            ref_count += 1;
        }
        assert_eq!(ref_count, 100);

        // Test IntoIterator for &mut AHashMap
        for (k, v) in &mut map {
            if k == "key_0" {
                *v = 999;
            }
        }
        assert_eq!(map.get("key_0"), Some(&999));

        // Test FromIterator
        let pair_vec = vec![("x".to_string(), 10), ("y".to_string(), 20)];
        let map_from: AHashMap<String, i32> = pair_vec.into_iter().collect();
        assert_eq!(map_from.get("x"), Some(&10));
        assert_eq!(map_from.get("y"), Some(&20));

        // Test IntoIterator for owned AHashMap
        let mut owned_count = 0;
        for (k, v) in map_from {
            owned_count += 1;
            assert!(k == "x" || k == "y");
            assert!(v == 10 || v == 20);
        }
        assert_eq!(owned_count, 2);

        // Test clear
        map.clear();
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
    }

    #[test]
    fn test_ahashmap_deleted_buckets() {
        let mut map: AHashMap<i32, i32> = AHashMap::default();

        // Insert many to force collisions and probing
        for i in 0..1000 {
            map.insert(i, i * 10);
        }

        // Remove many to create many Bucket::Deleted
        for i in 0..1000 {
            if i % 2 == 0 {
                assert_eq!(map.remove(&i), Some(i * 10));
            }
        }

        // Insert new ones to reuse Bucket::Deleted and trigger probing over them
        for i in 1000..1500 {
            map.insert(i, i * 10);
        }

        // Get and Get_Mut over Bucket::Deleted
        for i in 0..1000 {
            if i % 2 != 0 {
                assert_eq!(map.get(&i), Some(&(i * 10)));
                assert_eq!(map.get_mut(&i), Some(&mut (i * 10)));
                assert!(map.contains_key(&i));
            } else {
                assert_eq!(map.get(&i), None);
                assert_eq!(map.get_mut(&i), None);
                assert!(!map.contains_key(&i));
            }
        }

        // Remove passing over Bucket::Deleted
        for i in 0..1000 {
            if i % 2 != 0 {
                assert_eq!(map.remove(&i), Some(i * 10));
            }
        }
    }
}
