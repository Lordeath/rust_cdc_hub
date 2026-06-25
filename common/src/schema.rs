use std::collections::HashMap;

pub fn extract_mysql_create_table_column_definitions(
    create_table_sql: &str,
) -> HashMap<String, String> {
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

pub fn normalize_mysql_column_type_token(type_token: &str) -> String {
    let normalized = type_token.trim().to_ascii_lowercase();
    let mut parts = normalized.split_whitespace();
    let Some(first) = parts.next() else {
        return normalized;
    };

    let mut result = normalize_mysql_integer_display_width(first);
    let rest = parts.collect::<Vec<_>>().join(" ");
    if !rest.is_empty() {
        result.push(' ');
        result.push_str(&rest);
    }
    result
}

fn normalize_mysql_integer_display_width(type_name: &str) -> String {
    const INTEGER_TYPES: &[&str] = &[
        "tinyint",
        "smallint",
        "mediumint",
        "int",
        "integer",
        "bigint",
    ];

    for integer_type in INTEGER_TYPES {
        if type_name == *integer_type {
            return type_name.to_string();
        }

        let Some(width) = type_name
            .strip_prefix(integer_type)
            .and_then(|rest| rest.strip_prefix('('))
            .and_then(|rest| rest.strip_suffix(')'))
        else {
            continue;
        };
        if width.chars().all(|c| c.is_ascii_digit()) {
            return (*integer_type).to_string();
        }
    }

    type_name.to_string()
}

pub fn mysql_column_allows_null_from_definition(definition: &str) -> bool {
    let s = definition.to_ascii_lowercase();
    !s.contains("not null")
}

pub fn mysql_column_is_auto_increment_from_definition(definition: &str) -> bool {
    definition.to_ascii_lowercase().contains("auto_increment")
}

pub fn mysql_primary_key_columns_from_create_table_sql(create_table_sql: &str) -> Vec<String> {
    for raw_line in create_table_sql.lines() {
        let line = raw_line.trim().trim_end_matches(',');
        let line_lower = line.to_ascii_lowercase();
        let is_primary_key_constraint = line_lower.starts_with("primary key")
            || (line_lower.starts_with("constraint ") && line_lower.contains(" primary key"));
        if !is_primary_key_constraint {
            continue;
        }

        let Some(primary_key_index) = line_lower.find("primary key") else {
            continue;
        };
        let identifiers = mysql_backtick_identifiers(&line[primary_key_index..]);
        if !identifiers.is_empty() {
            return identifiers;
        }
    }
    Vec::new()
}

fn mysql_backtick_identifiers(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '`' {
            continue;
        }
        let mut identifier = String::new();
        while let Some(identifier_ch) = chars.next() {
            if identifier_ch == '`' {
                if chars.peek() == Some(&'`') {
                    chars.next();
                    identifier.push('`');
                    continue;
                }
                break;
            }
            identifier.push(identifier_ch);
        }
        result.push(identifier);
    }
    result
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
    fn mysql_type_normalization_ignores_integer_display_width() {
        assert_eq!(normalize_mysql_column_type_token("bigint(20)"), "bigint");
        assert_eq!(
            normalize_mysql_column_type_token("int(11) unsigned"),
            "int unsigned"
        );
        assert_eq!(normalize_mysql_column_type_token("TINYINT(1)"), "tinyint");
    }

    #[test]
    fn mysql_type_normalization_preserves_semantic_lengths() {
        assert_ne!(
            normalize_mysql_column_type_token("varchar(64)"),
            normalize_mysql_column_type_token("varchar(128)")
        );
        assert_eq!(
            normalize_mysql_column_type_token("decimal(10,2)"),
            "decimal(10,2)"
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

    #[test]
    fn mysql_auto_increment_parses_flag() {
        assert!(mysql_column_is_auto_increment_from_definition(
            "`id` bigint NOT NULL AUTO_INCREMENT"
        ));
        assert!(!mysql_column_is_auto_increment_from_definition(
            "`id` bigint NOT NULL"
        ));
    }

    #[test]
    fn mysql_primary_key_columns_parse_common_table_constraints() {
        let sql = r#"CREATE TABLE `act_ge_bytearray` (
  `ID_` varchar(64) NOT NULL,
  `DEPLOYMENT_ID_` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`ID_`) USING BTREE
) ENGINE=InnoDB"#;

        assert_eq!(
            mysql_primary_key_columns_from_create_table_sql(sql),
            vec!["ID_".to_string()]
        );
    }

    #[test]
    fn mysql_primary_key_columns_parse_composite_and_named_constraints() {
        let sql = r#"CREATE TABLE `t` (
  `tenant_id` varchar(64) NOT NULL,
  `code` varchar(64) NOT NULL,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`tenant_id`,`code`)
) ENGINE=InnoDB"#;

        assert_eq!(
            mysql_primary_key_columns_from_create_table_sql(sql),
            vec!["tenant_id".to_string(), "code".to_string()]
        );
    }

    #[test]
    fn mysql_primary_key_columns_returns_empty_without_primary_key() {
        let sql = r#"CREATE TABLE `t` (
  `name` varchar(64) DEFAULT NULL COMMENT 'not a primary key'
) ENGINE=InnoDB"#;

        assert!(mysql_primary_key_columns_from_create_table_sql(sql).is_empty());
    }
}
