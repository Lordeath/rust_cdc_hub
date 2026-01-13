use crate::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Keys;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseInsensitiveHashMap {
    map: HashMap<String, Value>,
    raw_map: HashMap<String, Value>,
}

impl CaseInsensitiveHashMap {
    pub fn new_with_no_arg() -> Self {
        CaseInsensitiveHashMap {
            map: HashMap::new(),
            raw_map: HashMap::new(),
        }
    }
    pub fn new(map: HashMap<String, Value>) -> Self {
        let mut map_new: HashMap<String, Value> = HashMap::new();
        for (k, v) in map.clone() {
            map_new.insert(k.to_lowercase(), v);
        }
        CaseInsensitiveHashMap {
            map: map_new,
            raw_map: map,
        }
    }

    pub fn get(&self, key: &str) -> &Value {
        self.map
            .get(key.to_lowercase().as_str())
            .unwrap_or(&Value::None)
    }

    pub fn insert(&mut self, k: String, v: Value) -> Option<Value> {
        self.raw_map.insert(k.clone(), v.clone());
        self.map.insert(k.to_lowercase(), v)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn raw_keys(&self) -> Keys<'_, String, Value> {
        self.raw_map.keys()
    }

    pub fn get_raw_map(&self) -> HashMap<String, Value> {
        self.raw_map.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseInsensitiveHashMapString {
    map: HashMap<String, String>,
}

impl CaseInsensitiveHashMapString {
    pub fn new(map: HashMap<String, String>) -> Self {
        let mut map_new: HashMap<String, String> = HashMap::new();
        for (k, v) in map {
            map_new.insert(k.to_lowercase(), v.to_string());
        }
        CaseInsensitiveHashMapString { map: map_new }
    }

    pub fn get(&self, key: &str) -> String {
        self.map
            .get(key.to_lowercase().as_str())
            .map(|x| x.to_string())
            .unwrap_or("".to_string())
    }

    pub fn keys(&self) -> Keys<'_, String, String> {
        self.map.keys()
    }

    pub fn insert(&mut self, k: String, v: String) -> Option<String> {
        self.map.insert(k.to_lowercase(), v)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseInsensitiveHashMapVecString {
    map: HashMap<String, Vec<String>>,
}

impl CaseInsensitiveHashMapVecString {
    pub fn new_with_no_arg() -> Self {
        CaseInsensitiveHashMapVecString::new(HashMap::new())
    }

    pub fn new(map: HashMap<String, Vec<String>>) -> Self {
        let mut new_map: HashMap<String, Vec<String>> = HashMap::new();
        for (k, v) in map {
            new_map.insert(k.to_lowercase(), v.clone());
        }
        CaseInsensitiveHashMapVecString { map: new_map }
    }

    pub fn get(&self, key: &str) -> Vec<String> {
        self.map
            .get(key.to_lowercase().as_str())
            .cloned()
            .unwrap_or(vec![])
    }

    pub fn keys(&self) -> Keys<'_, String, Vec<String>> {
        self.map.keys()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn insert(&mut self, k: String, v: Vec<String>) -> Option<Vec<String>> {
        self.map.insert(k.to_lowercase(), v)
    }

    pub fn entry_insert(&mut self, k: &str, v: String) {
        self.map.entry(k.to_lowercase()).or_default().push(v)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseInsensitiveHashMapVecCaseInsensitiveHashMap {
    map: HashMap<String, Vec<CaseInsensitiveHashMap>>,
}

impl CaseInsensitiveHashMapVecCaseInsensitiveHashMap {
    pub fn new_with_no_arg() -> Self {
        CaseInsensitiveHashMapVecCaseInsensitiveHashMap::new(HashMap::new())
    }

    pub fn new(map: HashMap<String, Vec<CaseInsensitiveHashMap>>) -> Self {
        let mut new_map: HashMap<String, Vec<CaseInsensitiveHashMap>> = HashMap::new();
        for (k, v) in map {
            new_map.insert(k.to_lowercase(), v.clone());
        }
        CaseInsensitiveHashMapVecCaseInsensitiveHashMap { map: new_map }
    }

    pub fn get(&self, key: &str) -> Vec<CaseInsensitiveHashMap> {
        self.map
            .get(key.to_lowercase().as_str())
            .cloned()
            .unwrap_or(vec![])
    }

    pub fn keys(&self) -> Keys<'_, String, Vec<CaseInsensitiveHashMap>> {
        self.map.keys()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn insert(
        &mut self,
        k: String,
        v: Vec<CaseInsensitiveHashMap>,
    ) -> Option<Vec<CaseInsensitiveHashMap>> {
        self.map.insert(k.to_lowercase(), v)
    }

    pub fn entry_insert(&mut self, k: String, v: CaseInsensitiveHashMap) {
        self.map.entry(k.to_lowercase()).or_default().push(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_case_insensitive_hash_map() {
        let raw_map: HashMap<String, Value> = HashMap::from([
            ("id".to_string(), Value::Int8(1)),
            ("name".to_string(), Value::Int64(64)),
            (
                "created_at".to_string(),
                Value::String("2023-01-01T00:00:00Z".to_string()),
            ),
        ]);
        let mut map = CaseInsensitiveHashMap::new(raw_map);
        assert_eq!(3, map.len());
        assert!(!map.is_empty());
        assert_eq!("1".to_string(), map.get("id").resolve_string());
        assert_eq!("1".to_string(), map.get("ID").resolve_string());
        map.insert("iD".to_string(), Value::Int64(456));
        assert_eq!("456".to_string(), map.get("ID").resolve_string());
        let mut map = CaseInsensitiveHashMap::new_with_no_arg();
        assert_eq!(0, map.len());
        assert!(map.is_empty());
        map.insert("aBc".to_string(), Value::Int64(123));
        assert_eq!(1, map.len());
        assert_eq!("123".to_string(), map.get("ABC").resolve_string());
        assert_eq!("123".to_string(), map.get("abc").resolve_string());
        map.insert("ABc".to_string(), Value::Int64(123));
        assert_eq!(1, map.len());
        assert_eq!(2, map.get_raw_map().len());
    }
    #[test]
    fn test_case_insensitive_hash_map_string() {
        let raw_map: HashMap<String, String> = HashMap::from([
            ("id".to_string(), "123".to_string()),
            ("name".to_string(), "456".to_string()),
            ("created_at".to_string(), "789".to_string()),
        ]);
        let mut map = CaseInsensitiveHashMapString::new(raw_map);
        assert_eq!(3, map.keys().len());
        assert_eq!("123".to_string(), map.get("id"));
        assert_eq!("123".to_string(), map.get("ID"));
        map.insert("iD".to_string(), "456".to_string());
        assert_eq!("456".to_string(), map.get("ID"));
    }
    #[test]
    fn test_case_insensitive_hash_map_vec_string() {
        let raw_map: HashMap<String, Vec<String>> = HashMap::from([
            ("id".to_string(), vec!["123".to_string(), "456".to_string()]),
            (
                "name".to_string(),
                vec!["n123".to_string(), "n456".to_string()],
            ),
            (
                "created_at".to_string(),
                vec!["c123".to_string(), "c456".to_string()],
            ),
        ]);
        let mut map = CaseInsensitiveHashMapVecString::new(raw_map);
        assert_eq!(3, map.len());
        assert!(!map.is_empty());
        assert_eq!(vec!["123".to_string(), "456".to_string()], map.get("id"));
        assert_eq!(vec!["123".to_string(), "456".to_string()], map.get("ID"));
        map.insert(
            "iD".to_string(),
            vec!["x123".to_string(), "x456".to_string()],
        );
        assert_eq!(vec!["x123".to_string(), "x456".to_string()], map.get("ID"));
        map.entry_insert("nAmE", "n789".to_string());
        assert_eq!(
            vec!["n123".to_string(), "n456".to_string(), "n789".to_string()],
            map.get("name")
        );
    }
}
