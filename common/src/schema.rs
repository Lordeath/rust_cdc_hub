use std::collections::HashMap;

pub fn extract_mysql_create_table_column_definitions(create_table_sql: &str) -> HashMap<String, String> {
    let mut result: HashMap<String, String> = HashMap::new();
    for raw_line in create_table_sql.lines() {
        let line = raw_line.trim();
        if !line.starts_with('`') {
            continue;
        }
        let second_tick = match line[1..].find('`') {
            None => continue,
            Some(i) => i + 1,
        };
        let col_name = &line[1..second_tick];
        let lower = col_name.to_ascii_lowercase();
        let mut def = line.trim_end_matches(',').to_string();
        let def_lower = def.to_ascii_lowercase();
        if def_lower.contains("stored generated") || def_lower.contains("virtual generated") {
            continue;
        }
        if def_lower.contains("generated always") {
            continue;
        }
        result.entry(lower).or_insert_with(|| {
            if def.ends_with(',') {
                def.pop();
            }
            def
        });
    }
    result
}

pub fn mysql_type_token_from_column_definition(definition: &str) -> Option<String> {
    let s = definition.trim();
    if !s.starts_with('`') {
        return None;
    }
    let second_tick = s[1..].find('`')? + 1;
    let rest = s[second_tick + 1..].trim();
    let mut iter = rest.split_whitespace();
    let first = iter.next()?;
    let second = iter.next().unwrap_or("");
    if second.eq_ignore_ascii_case("unsigned") {
        return Some(format!("{} {}", first, second));
    }
    Some(first.to_string())
}

pub fn mysql_column_allows_null_from_definition(definition: &str) -> bool {
    let s = definition.to_ascii_lowercase();
    !s.contains("not null")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_mysql_defs_basic() {
        let sql = r#"CREATE TABLE `t` (
  `id` bigint(20) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `g` int GENERATED ALWAYS AS (`id` + 1) STORED,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB"#;
        let defs = extract_mysql_create_table_column_definitions(sql);
        assert!(defs.contains_key("id"));
        assert!(defs.contains_key("name"));
        assert!(!defs.contains_key("g"));
        assert!(defs.get("id").unwrap().contains("bigint(20)"));
    }

    #[test]
    fn mysql_type_token_parses_unsigned() {
        let def = "`x` int(11) unsigned NOT NULL DEFAULT 0";
        assert_eq!(
            mysql_type_token_from_column_definition(def),
            Some("int(11) unsigned".to_string())
        );
    }

    #[test]
    fn mysql_allows_null_parses_not_null() {
        assert!(!mysql_column_allows_null_from_definition(
            "`x` int(11) NOT NULL DEFAULT 0"
        ));
        assert!(mysql_column_allows_null_from_definition(
            "`x` int(11) DEFAULT NULL"
        ));
    }
}

