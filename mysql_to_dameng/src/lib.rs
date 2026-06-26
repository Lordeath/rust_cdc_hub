use common::{MySqlRoutineDefinition, MySqlRoutineKind};

const DAMENG_INLINE_STRING_CHAR_LIMIT: u32 = 512;
const DAMENG_DECIMAL_MAX_PRECISION: u32 = 38;

#[derive(Debug)]
struct MysqlRoutineSignature {
    name: String,
    params: String,
    after_params: String,
}

#[derive(Debug)]
struct DamengRoutineBody {
    declarations: Vec<String>,
    body: String,
}

pub fn convert_mysql_routine_to_dameng(
    target_schema: &str,
    routine: &MySqlRoutineDefinition,
) -> Result<String, String> {
    let signature = parse_mysql_routine_signature(routine.create_sql.as_str(), routine.kind)?;
    let params = convert_mysql_routine_params(signature.params.as_str(), routine.kind)?;
    let params_sql = if params.is_empty() {
        String::new()
    } else {
        format!("({})", params.join(", "))
    };
    let body_pos = find_keyword_outside_quotes(signature.after_params.as_str(), 0, "BEGIN")
        .ok_or_else(|| "routine body BEGIN not found".to_string())?;
    let body = convert_mysql_routine_body(
        &signature.after_params[body_pos..],
        routine.sql_mode.as_str(),
    );
    let declaration_sql = if body.declarations.is_empty() {
        String::new()
    } else {
        format!("{}\n", body.declarations.join("\n"))
    };
    let qualified_name = qualified_routine(target_schema, signature.name.as_str());
    match routine.kind {
        MySqlRoutineKind::Procedure => Ok(format!(
            "CREATE OR REPLACE PROCEDURE {}{}\nAS\n{}{}",
            qualified_name, params_sql, declaration_sql, body.body
        )),
        MySqlRoutineKind::Function => {
            let return_type = mysql_function_return_type(&signature.after_params[..body_pos])?;
            Ok(format!(
                "CREATE OR REPLACE FUNCTION {}{}\nRETURN {}\nAS\n{}{}",
                qualified_name, params_sql, return_type, declaration_sql, body.body
            ))
        }
    }
}

fn parse_mysql_routine_signature(
    create_sql: &str,
    routine_kind: MySqlRoutineKind,
) -> Result<MysqlRoutineSignature, String> {
    let sql = create_sql.trim_start();
    let keyword_pos = find_keyword_outside_quotes(sql, 0, routine_kind.routine_type())
        .ok_or_else(|| format!("CREATE {} keyword not found", routine_kind.routine_type()))?;
    let mut pos = skip_ascii_whitespace(sql, keyword_pos + routine_kind.routine_type().len());
    let (mut name, next_pos) =
        take_mysql_identifier(sql, pos).ok_or_else(|| "routine name not found".to_string())?;
    pos = skip_ascii_whitespace(sql, next_pos);
    loop {
        if sql.as_bytes().get(pos) == Some(&b'.') {
            let next_name_pos = skip_ascii_whitespace(sql, pos + 1);
            let (part, next_pos) = take_mysql_identifier(sql, next_name_pos)
                .ok_or_else(|| "routine name not found after schema qualifier".to_string())?;
            name = part;
            pos = skip_ascii_whitespace(sql, next_pos);
            continue;
        }
        break;
    }

    if sql.as_bytes().get(pos) != Some(&b'(') {
        return Err("routine parameter list not found".to_string());
    }
    let close_pos = find_matching_paren(sql, pos)
        .ok_or_else(|| "routine parameter list is not closed".to_string())?;
    Ok(MysqlRoutineSignature {
        name,
        params: sql[pos + 1..close_pos].to_string(),
        after_params: sql[close_pos + 1..].to_string(),
    })
}

fn convert_mysql_routine_params(
    params: &str,
    routine_kind: MySqlRoutineKind,
) -> Result<Vec<String>, String> {
    let mut result = Vec::new();
    for param in split_top_level_commas(params) {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        let mut pos = skip_ascii_whitespace(param, 0);
        let mut mode = "IN".to_string();
        if let Some((word, next_pos)) = take_ascii_word(param, pos) {
            match word.to_ascii_uppercase().as_str() {
                "IN" | "OUT" | "INOUT" => {
                    mode = if word.eq_ignore_ascii_case("INOUT") {
                        "IN OUT".to_string()
                    } else {
                        word.to_ascii_uppercase()
                    };
                    pos = skip_ascii_whitespace(param, next_pos);
                }
                _ => {}
            }
        }
        let (name, next_pos) = take_mysql_identifier(param, pos)
            .ok_or_else(|| format!("routine parameter name not found: {}", param))?;
        let mysql_type = param[next_pos..].trim();
        if mysql_type.is_empty() {
            return Err(format!("routine parameter type not found: {}", param));
        }
        let dameng_type = map_mysql_type_to_dameng(mysql_type);
        let name = dameng_routine_param_name(name.as_str());
        result.push(format!("{} {} {}", name, mode, dameng_type));
    }

    if matches!(routine_kind, MySqlRoutineKind::Function) {
        for param in &result {
            if param.contains(" OUT ") {
                return Err(format!("function parameter cannot be OUT/INOUT: {}", param));
            }
        }
    }
    Ok(result)
}

fn mysql_function_return_type(before_body: &str) -> Result<String, String> {
    let rest = before_body.trim_start();
    if !starts_with_keyword(rest, 0, "RETURNS") {
        return Err("function RETURNS clause not found".to_string());
    }
    let type_and_attrs = rest["RETURNS".len()..].trim_start();
    let mysql_type = take_mysql_type_before_routine_attrs(type_and_attrs)
        .ok_or_else(|| "function return type not found".to_string())?;
    Ok(map_mysql_type_to_dameng(mysql_type))
}

fn take_mysql_type_before_routine_attrs(value: &str) -> Option<&str> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut paren_depth = 0u32;
    let mut pos = 0usize;
    let mut end = bytes.len();
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    pos += 2;
                } else {
                    quote = None;
                    pos += 1;
                }
            } else {
                pos += 1;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' | b'`' => {
                quote = Some(byte);
                pos += 1;
            }
            b'(' => {
                paren_depth += 1;
                pos += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                pos += 1;
            }
            b if b.is_ascii_whitespace() && paren_depth == 0 => {
                let next = skip_ascii_whitespace(value, pos);
                if next < bytes.len() && is_mysql_routine_attribute_start(value, next) {
                    end = pos;
                    break;
                }
                pos += 1;
            }
            _ => pos += 1,
        }
    }

    let mysql_type = value[..end].trim();
    if mysql_type.is_empty() {
        None
    } else {
        Some(mysql_type)
    }
}

fn is_mysql_routine_attribute_start(value: &str, pos: usize) -> bool {
    const ATTRS: &[&str] = &[
        "deterministic",
        "not",
        "contains",
        "no",
        "reads",
        "modifies",
        "sql",
        "comment",
        "language",
        "begin",
    ];
    ATTRS
        .iter()
        .any(|keyword| starts_with_keyword(value, pos, keyword))
}

fn convert_mysql_routine_body(body: &str, sql_mode: &str) -> DamengRoutineBody {
    let (declarations, body) = extract_mysql_routine_declarations(body.trim());
    let declarations = declarations
        .into_iter()
        .map(|declaration| {
            format!(
                "    {}",
                convert_mysql_sql_tokens(declaration.as_str(), sql_mode)
            )
        })
        .collect::<Vec<_>>();
    let body = convert_mysql_sql_tokens(body.as_str(), sql_mode);
    let body = convert_mysql_set_assignments(body.as_str());
    let mut body = body.trim_end().to_string();
    if !body.ends_with(';') {
        body.push(';');
    }
    DamengRoutineBody { declarations, body }
}

fn extract_mysql_routine_declarations(body: &str) -> (Vec<String>, String) {
    let body = body.trim_start();
    if !starts_with_keyword(body, 0, "BEGIN") {
        return (Vec::new(), body.to_string());
    }

    let after_begin = &body["BEGIN".len()..];
    let mut pos = 0usize;
    let mut declarations = Vec::new();
    loop {
        pos = skip_ascii_whitespace(after_begin, pos);
        if !starts_with_keyword(after_begin, pos, "DECLARE") {
            break;
        }
        let Some(stmt_end) = find_statement_semicolon_outside_quotes(after_begin, pos) else {
            break;
        };
        let declaration = after_begin[pos + "DECLARE".len()..=stmt_end].trim();
        if declaration.is_empty() {
            break;
        }
        declarations.push(declaration.to_string());
        pos = stmt_end + 1;
    }

    if declarations.is_empty() {
        return (declarations, body.to_string());
    }

    let remaining = after_begin[pos..].trim_start();
    let body = if remaining.is_empty() {
        "BEGIN\nEND".to_string()
    } else {
        format!("BEGIN\n{}", remaining)
    };
    (declarations, body)
}

fn convert_mysql_set_assignments(body: &str) -> String {
    body.lines()
        .map(convert_mysql_set_assignment_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_mysql_set_assignment_line(line: &str) -> String {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "SET") {
        return line.to_string();
    }
    let rest = trimmed["SET".len()..].trim_start();
    if split_top_level_commas(rest).len() > 1 {
        return line.to_string();
    }
    let Some(eq_pos) = find_char_outside_quotes(rest, '=') else {
        return line.to_string();
    };
    let lhs = rest[..eq_pos].trim();
    if lhs.is_empty() || lhs.contains(char::is_whitespace) || lhs.contains(',') {
        return line.to_string();
    }
    format!("{}{} :={}", leading, lhs, &rest[eq_pos + 1..])
}

fn convert_mysql_sql_tokens(value: &str, sql_mode: &str) -> String {
    let value = convert_mysql_quotes_to_dameng(value, sql_mode);
    let value = convert_mysql_now_calls(value.as_str());
    convert_mysql_date_format_interval_exprs(value.as_str())
}

fn convert_mysql_quotes_to_dameng(value: &str, sql_mode: &str) -> String {
    let ansi_quotes = mysql_sql_mode_has_ansi_quotes(sql_mode);
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(ch) = chars.next() {
        if let Some(quote_char) = quote {
            result.push(ch);
            if ch == '\\' && quote_char == '\'' {
                if let Some(next) = chars.next() {
                    result.push(next);
                }
                continue;
            }
            if ch == quote_char {
                if chars.peek() == Some(&quote_char) {
                    result.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' => {
                quote = Some(ch);
                result.push(ch);
            }
            '"' if ansi_quotes => {
                quote = Some(ch);
                result.push(ch);
            }
            '"' => {
                let literal = take_mysql_double_quoted_string(&mut chars);
                result.push_str(quote_literal(literal.as_str()).as_str());
            }
            '`' => {
                let mut ident = String::new();
                while let Some(inner) = chars.next() {
                    if inner == '`' {
                        if chars.peek() == Some(&'`') {
                            ident.push('`');
                            chars.next();
                        } else {
                            break;
                        }
                    } else {
                        ident.push(inner);
                    }
                }
                result.push_str(quote_ident(ident.as_str()).as_str());
            }
            _ => result.push(ch),
        }
    }
    result
}

fn mysql_sql_mode_has_ansi_quotes(sql_mode: &str) -> bool {
    sql_mode
        .split(',')
        .any(|mode| mode.trim().eq_ignore_ascii_case("ANSI_QUOTES"))
}

fn take_mysql_double_quoted_string<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut literal = String::new();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                literal.push(next);
            }
            continue;
        }
        if ch == '"' {
            if chars.peek() == Some(&'"') {
                literal.push('"');
                chars.next();
                continue;
            }
            break;
        }
        literal.push(ch);
    }
    literal
}

fn convert_mysql_now_calls(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut result = String::with_capacity(value.len());
    let mut quote = None;
    let mut pos = 0usize;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            let ch = value[pos..].chars().next().unwrap();
            result.push(ch);
            if byte == b'\\' && quote_byte == b'\'' {
                pos += ch.len_utf8();
                if pos < bytes.len() {
                    let next = value[pos..].chars().next().unwrap();
                    result.push(next);
                    pos += next.len_utf8();
                }
                continue;
            }
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    result.push(quote_byte as char);
                    pos += 2;
                } else {
                    quote = None;
                    pos += ch.len_utf8();
                }
            } else {
                pos += ch.len_utf8();
            }
            continue;
        }

        if matches!(byte, b'\'' | b'"') {
            quote = Some(byte);
            result.push(byte as char);
            pos += 1;
            continue;
        }

        if starts_with_keyword(value, pos, "now") {
            let after_keyword = skip_ascii_whitespace(value, pos + "now".len());
            if bytes.get(after_keyword) == Some(&b'(') {
                let after_open = skip_ascii_whitespace(value, after_keyword + 1);
                if bytes.get(after_open) == Some(&b')') {
                    result.push_str("CURRENT_TIMESTAMP");
                    pos = after_open + 1;
                    continue;
                }
            }
        }

        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn convert_mysql_date_format_interval_exprs(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut result = String::with_capacity(value.len());
    let mut quote = None;
    let mut pos = 0usize;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            let ch = value[pos..].chars().next().unwrap();
            result.push(ch);
            if byte == b'\\' && quote_byte == b'\'' {
                pos += ch.len_utf8();
                if pos < bytes.len() {
                    let next = value[pos..].chars().next().unwrap();
                    result.push(next);
                    pos += next.len_utf8();
                }
                continue;
            }
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    result.push(quote_byte as char);
                    pos += 2;
                } else {
                    quote = None;
                    pos += ch.len_utf8();
                }
            } else {
                pos += ch.len_utf8();
            }
            continue;
        }

        if matches!(byte, b'\'' | b'"' | b'`') {
            quote = Some(byte);
            result.push(byte as char);
            pos += 1;
            continue;
        }

        if let Some((converted, next_pos)) = convert_one_date_format_interval_expr(value, pos) {
            result.push_str(converted.as_str());
            pos = next_pos;
            continue;
        }

        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn convert_one_date_format_interval_expr(value: &str, pos: usize) -> Option<(String, usize)> {
    if !starts_with_keyword(value, pos, "DATE_FORMAT") {
        return None;
    }
    let bytes = value.as_bytes();
    let open_pos = skip_ascii_whitespace(value, pos + "DATE_FORMAT".len());
    if bytes.get(open_pos) != Some(&b'(') {
        return None;
    }
    let close_pos = find_matching_paren(value, open_pos)?;
    let mut cursor = skip_ascii_whitespace(value, close_pos + 1);
    let op = match bytes.get(cursor) {
        Some(b'+') => {
            cursor += 1;
            "DATE_ADD"
        }
        Some(b'-') => {
            cursor += 1;
            "DATE_SUB"
        }
        _ => return None,
    };
    cursor = skip_ascii_whitespace(value, cursor);
    if !starts_with_keyword(value, cursor, "INTERVAL") {
        return None;
    }
    cursor = skip_ascii_whitespace(value, cursor + "INTERVAL".len());
    let (amount, amount_end) = take_interval_amount(value, cursor)?;
    cursor = skip_ascii_whitespace(value, amount_end);
    if !starts_with_keyword(value, cursor, "DAY") {
        return None;
    }
    let next_pos = cursor + "DAY".len();
    let expr = &value[pos..=close_pos];
    Some((
        format!(
            "{}({}, INTERVAL {} DAY)",
            op,
            expr,
            quote_literal(amount.as_str())
        ),
        next_pos,
    ))
}

fn take_interval_amount(value: &str, start: usize) -> Option<(String, usize)> {
    let bytes = value.as_bytes();
    let first = *bytes.get(start)?;
    if matches!(first, b'\'' | b'"') {
        let quote = first;
        let mut result = Vec::new();
        let mut pos = start + 1;
        while pos < bytes.len() {
            let byte = bytes[pos];
            if byte == b'\\' && quote == b'\'' {
                let next = *bytes.get(pos + 1)?;
                result.push(next);
                pos += 2;
                continue;
            }
            if byte == quote {
                if bytes.get(pos + 1) == Some(&quote) {
                    result.push(quote);
                    pos += 2;
                } else {
                    return Some((
                        String::from_utf8_lossy(result.as_slice()).into_owned(),
                        pos + 1,
                    ));
                }
            } else {
                result.push(byte);
                pos += 1;
            }
        }
        return None;
    }

    let mut pos = start;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'+' | b'-') {
            pos += 1;
        } else {
            break;
        }
    }
    if pos == start {
        None
    } else {
        Some((value[start..pos].to_string(), pos))
    }
}

fn find_statement_semicolon_outside_quotes(value: &str, start: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut pos = start;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            if byte == b'\\' && quote_byte == b'\'' {
                pos = (pos + 2).min(bytes.len());
                continue;
            }
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    pos += 2;
                } else {
                    quote = None;
                    pos += 1;
                }
            } else {
                pos += 1;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' | b'`' => {
                quote = Some(byte);
                pos += 1;
            }
            b';' => return Some(pos),
            _ => pos += 1,
        }
    }
    None
}

fn take_mysql_identifier(value: &str, start: usize) -> Option<(String, usize)> {
    let bytes = value.as_bytes();
    let first = *bytes.get(start)?;
    if matches!(first, b'`' | b'"') {
        let quote = first;
        let mut pos = start + 1;
        let mut ident = String::new();
        while pos < bytes.len() {
            let byte = bytes[pos];
            if byte == quote {
                if bytes.get(pos + 1) == Some(&quote) {
                    ident.push(quote as char);
                    pos += 2;
                } else {
                    return Some((ident, pos + 1));
                }
            } else {
                let ch = value[pos..].chars().next()?;
                ident.push(ch);
                pos += ch.len_utf8();
            }
        }
        return None;
    }

    let mut pos = start;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if byte.is_ascii_whitespace() || matches!(byte, b'.' | b'(' | b')' | b',') {
            break;
        }
        pos += 1;
    }
    if pos == start {
        None
    } else {
        Some((value[start..pos].to_string(), pos))
    }
}

fn take_ascii_word(value: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = value.as_bytes();
    let mut pos = start;
    while pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
        pos += 1;
    }
    if pos == start {
        None
    } else {
        Some((&value[start..pos], pos))
    }
}

fn find_matching_paren(value: &str, open_pos: usize) -> Option<usize> {
    if value.as_bytes().get(open_pos) != Some(&b'(') {
        return None;
    }
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut depth = 0u32;
    let mut pos = open_pos;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    pos += 2;
                } else {
                    quote = None;
                    pos += 1;
                }
            } else {
                pos += 1;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' | b'`' => {
                quote = Some(byte);
                pos += 1;
            }
            b'(' => {
                depth += 1;
                pos += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(pos);
                }
                pos += 1;
            }
            _ => pos += 1,
        }
    }
    None
}

fn split_top_level_commas(value: &str) -> Vec<&str> {
    let bytes = value.as_bytes();
    let mut result = Vec::new();
    let mut quote = None;
    let mut paren_depth = 0u32;
    let mut start = 0usize;
    let mut pos = 0usize;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    pos += 2;
                } else {
                    quote = None;
                    pos += 1;
                }
            } else {
                pos += 1;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' | b'`' => {
                quote = Some(byte);
                pos += 1;
            }
            b'(' => {
                paren_depth += 1;
                pos += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                pos += 1;
            }
            b',' if paren_depth == 0 => {
                result.push(&value[start..pos]);
                start = pos + 1;
                pos += 1;
            }
            _ => pos += 1,
        }
    }
    result.push(&value[start..]);
    result
}

fn find_char_outside_quotes(value: &str, target: char) -> Option<usize> {
    let mut quote = None;
    let mut chars = value.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if let Some(quote_char) = quote {
            if ch == '\\' && quote_char == '\'' {
                chars.next();
                continue;
            }
            if ch == quote_char {
                if chars.peek().is_some_and(|(_, next)| *next == quote_char) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            _ if ch == target => return Some(idx),
            _ => {}
        }
    }
    None
}

fn dameng_routine_param_name(name: &str) -> String {
    if is_safe_unquoted_identifier(name) {
        name.to_string()
    } else {
        quote_ident(name)
    }
}

fn is_safe_unquoted_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
}

fn qualified_routine(schema: &str, routine_name: &str) -> String {
    if schema.is_empty() {
        quote_ident(routine_name)
    } else {
        format!("{}.{}", quote_ident(schema), quote_ident(routine_name))
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn skip_ascii_whitespace(value: &str, start: usize) -> usize {
    let bytes = value.as_bytes();
    let mut pos = start;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn starts_with_keyword(value: &str, pos: usize, keyword: &str) -> bool {
    let bytes = value.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    if pos + keyword_bytes.len() > bytes.len() {
        return false;
    }
    if !bytes[pos..pos + keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes) {
        return false;
    }
    let before_ok = pos == 0 || !is_ascii_identifier_byte(bytes[pos - 1]);
    let after_pos = pos + keyword_bytes.len();
    let after_ok = after_pos >= bytes.len() || !is_ascii_identifier_byte(bytes[after_pos]);
    before_ok && after_ok
}

fn find_keyword_outside_quotes(value: &str, start: usize, keyword: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut pos = start;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    pos += 2;
                } else {
                    quote = None;
                    pos += 1;
                }
            } else {
                pos += 1;
            }
            continue;
        }
        if matches!(byte, b'`' | b'\'' | b'"') {
            quote = Some(byte);
            pos += 1;
            continue;
        }
        if starts_with_keyword(value, pos, keyword) {
            return Some(pos);
        }
        pos += 1;
    }
    None
}

fn is_ascii_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn map_mysql_type_to_dameng(mysql_type_token: &str) -> String {
    let t = mysql_type_token.to_ascii_lowercase();
    if t.starts_with("tinyint(1)") || t.starts_with("boolean") || t.starts_with("bool") {
        return "BIT".to_string();
    }
    if t.starts_with("tinyint") {
        if t.contains("unsigned") {
            "SMALLINT".to_string()
        } else {
            "TINYINT".to_string()
        }
    } else if t.starts_with("smallint") {
        if t.contains("unsigned") {
            "INT".to_string()
        } else {
            "SMALLINT".to_string()
        }
    } else if t.starts_with("mediumint") || t.starts_with("int") || t.starts_with("integer") {
        if t.contains("unsigned") {
            "BIGINT".to_string()
        } else {
            "INT".to_string()
        }
    } else if t.starts_with("bigint") {
        if t.contains("unsigned") {
            "DECIMAL(20,0)".to_string()
        } else {
            "BIGINT".to_string()
        }
    } else if t.starts_with("float") {
        "FLOAT".to_string()
    } else if t.starts_with("double") || t.starts_with("real") {
        "DOUBLE".to_string()
    } else if t.starts_with("decimal") || t.starts_with("numeric") {
        map_mysql_decimal_type_to_dameng(mysql_type_token)
    } else if t.starts_with("datetime") || t.starts_with("timestamp") {
        "TIMESTAMP".to_string()
    } else if t.starts_with("date") {
        "DATE".to_string()
    } else if t.starts_with("time") {
        "TIME".to_string()
    } else if t.starts_with("varchar") {
        map_mysql_char_type_to_dameng("VARCHAR", mysql_type_token, DAMENG_INLINE_STRING_CHAR_LIMIT)
    } else if t.starts_with("char") {
        map_mysql_char_type_to_dameng("CHAR", mysql_type_token, DAMENG_INLINE_STRING_CHAR_LIMIT)
    } else if t.contains("text")
        || t.starts_with("json")
        || t.starts_with("enum")
        || t.starts_with("set")
    {
        "CLOB".to_string()
    } else if t.contains("blob") || t.contains("binary") {
        "BLOB".to_string()
    } else {
        "VARCHAR(255 CHAR)".to_string()
    }
}

fn map_mysql_char_type_to_dameng(
    dameng_type: &str,
    mysql_type_token: &str,
    char_limit: u32,
) -> String {
    match mysql_type_length(mysql_type_token) {
        Some(len) if len > char_limit => "CLOB".to_string(),
        Some(len) => format!("{}({} CHAR)", dameng_type, len.max(1)),
        None => format!("{}(255 CHAR)", dameng_type),
    }
}

fn map_mysql_decimal_type_to_dameng(mysql_type_token: &str) -> String {
    let Some((precision, scale)) = mysql_decimal_precision_scale(mysql_type_token) else {
        return "DECIMAL".to_string();
    };
    let precision = precision.max(1);
    let scale = scale.min(precision);
    if precision <= DAMENG_DECIMAL_MAX_PRECISION {
        return format!("DECIMAL({},{})", precision, scale);
    }

    let scale = scale.min(DAMENG_DECIMAL_MAX_PRECISION);
    format!("DECIMAL({},{})", DAMENG_DECIMAL_MAX_PRECISION, scale)
}

fn mysql_decimal_precision_scale(mysql_type_token: &str) -> Option<(u32, u32)> {
    let start = mysql_type_token.find('(')? + 1;
    let end = mysql_type_token[start..].find(')')? + start;
    let mut parts = mysql_type_token[start..end].split(',');
    let precision = parts.next()?.trim().parse::<u32>().ok()?;
    let scale = parts
        .next()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(0);
    Some((precision, scale))
}

fn mysql_type_length(mysql_type_token: &str) -> Option<u32> {
    let start = mysql_type_token.find('(')? + 1;
    let end = mysql_type_token[start..].find(')')? + start;
    mysql_type_token[start..end]
        .split(',')
        .next()?
        .trim()
        .parse::<u32>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{MySqlRoutineDefinition, MySqlRoutineKind};

    #[test]
    fn converts_mysql_procedure_to_dameng_routine() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "sync_demo".to_string(),
            create_sql: "CREATE PROCEDURE `sync_demo`(IN p_id bigint unsigned, OUT p_name varchar(50)) BEGIN\n SET p_name = CONCAT('id:', p_id);\n SELECT `name` FROM `orders` WHERE `id` = p_id;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.starts_with(
            "CREATE OR REPLACE PROCEDURE \"target_schema\".\"sync_demo\"(p_id IN DECIMAL(20,0), p_name OUT VARCHAR(50 CHAR))\nAS\nBEGIN"
        ));
        assert!(sql.contains("p_name := CONCAT('id:', p_id);"));
        assert!(sql.contains("SELECT \"name\" FROM \"orders\" WHERE \"id\" = p_id;"));
        assert!(sql.ends_with("END;"));
    }

    #[test]
    fn converts_mysql_function_to_dameng_routine() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Function,
            name: "calc_name".to_string(),
            create_sql: "CREATE FUNCTION `calc_name`(p_name varchar(50)) RETURNS varchar(50) DETERMINISTIC READS SQL DATA BEGIN RETURN CONCAT(p_name, 'x'); END".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.starts_with(
            "CREATE OR REPLACE FUNCTION \"target_schema\".\"calc_name\"(p_name IN VARCHAR(50 CHAR))\nRETURN VARCHAR(50 CHAR)\nAS\nBEGIN"
        ));
        assert!(sql.contains("RETURN CONCAT(p_name, 'x');"));
        assert!(sql.ends_with("END;"));
    }

    #[test]
    fn converts_mysql_routine_declarations_and_literals() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "addAll_ns_canal_config_item_4".to_string(),
            create_sql: r#"CREATE PROCEDURE `addAll_ns_canal_config_item_4`()
BEGIN
    DECLARE existing_count INT DEFAULT 0;

    -- check existing id
    SELECT COUNT(*) INTO existing_count
    FROM `ns_canal_config_item`
    WHERE `id` = 4;
    IF existing_count = 0 THEN
        INSERT INTO `ns_canal_config_field` VALUES (NULL, 4, 'shouldAccountBook', '', '', "", 1, now(), 'admin', now(), 'admin');
        INSERT INTO `ns_canal_config_field` VALUES (NULL, 4, 'actualAccountBook', '', 'split,sharding', "0", 1, now(), 'admin', now(), 'admin');
    END IF;
END"#.to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-bill-10", &routine).unwrap();

        assert!(sql.contains("AS\n    existing_count INT DEFAULT 0;\nBEGIN\n-- check existing id"));
        assert!(!sql.contains("BEGIN\n    DECLARE existing_count"));
        assert!(sql.contains("SELECT COUNT(*) INTO existing_count"));
        assert!(sql.contains("FROM \"ns_canal_config_item\""));
        assert!(sql.contains("'', 1, CURRENT_TIMESTAMP"));
        assert!(sql.contains("'0', 1, CURRENT_TIMESTAMP"));
        assert!(!sql.contains("\"0\""));
    }

    #[test]
    fn mysql_double_quotes_follow_sql_mode() {
        assert_eq!(
            convert_mysql_sql_tokens(r#"SELECT `name`, "0", "", now(), 'now()'"#, ""),
            r#"SELECT "name", '0', '', CURRENT_TIMESTAMP, 'now()'"#
        );
        assert_eq!(
            convert_mysql_sql_tokens(
                r#"SELECT `name`, "quoted_ident", now()"#,
                "STRICT_TRANS_TABLES,ANSI_QUOTES"
            ),
            r#"SELECT "name", "quoted_ident", CURRENT_TIMESTAMP"#
        );
    }

    #[test]
    fn routine_set_assignment_conversion_skips_multi_assignment() {
        assert_eq!(
            convert_mysql_set_assignment_line(" SET p_name = CONCAT('id:', p_id);"),
            " p_name := CONCAT('id:', p_id);"
        );
        assert_eq!(
            convert_mysql_set_assignment_line(" SET a = 1, b = 2;"),
            " SET a = 1, b = 2;"
        );
    }

    #[test]
    fn converts_date_format_plus_day_interval() {
        assert_eq!(
            convert_mysql_sql_tokens(
                "OperatorDate < DATE_FORMAT( end_date,'%Y-%m-%d') +INTERVAL 1 day",
                ""
            ),
            "OperatorDate < DATE_ADD(DATE_FORMAT( end_date,'%Y-%m-%d'), INTERVAL '1' DAY)"
        );
        assert_eq!(
            convert_mysql_sql_tokens(
                "OperatorDate < DATE_FORMAT(end_date, '%Y-%m-%d') + INTERVAL 1 DAY",
                ""
            ),
            "OperatorDate < DATE_ADD(DATE_FORMAT(end_date, '%Y-%m-%d'), INTERVAL '1' DAY)"
        );
    }

    #[test]
    fn converts_date_format_minus_day_interval() {
        assert_eq!(
            convert_mysql_sql_tokens(
                "OperatorDate >= date_format(begin_date,'%Y-%m-%d') - interval 2 day",
                ""
            ),
            "OperatorDate >= DATE_SUB(date_format(begin_date,'%Y-%m-%d'), INTERVAL '2' DAY)"
        );
    }

    #[test]
    fn leaves_interval_inside_string_literal_unchanged() {
        assert_eq!(
            convert_mysql_sql_tokens(
                "SELECT 'DATE_FORMAT(end_date,''%Y-%m-%d'') + INTERVAL 1 DAY'",
                ""
            ),
            "SELECT 'DATE_FORMAT(end_date,''%Y-%m-%d'') + INTERVAL 1 DAY'"
        );
    }
}
