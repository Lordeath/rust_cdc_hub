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
    let routine_param_names = mysql_routine_param_names(signature.params.as_str());
    let body_pos = find_keyword_outside_quotes(signature.after_params.as_str(), 0, "BEGIN");
    let outer_block_labels = body_pos
        .map(|pos| mysql_routine_outer_block_labels(&signature.after_params[..pos]))
        .unwrap_or_default();
    if body_pos.is_none() && matches!(routine.kind, MySqlRoutineKind::Function) {
        return Err("routine body BEGIN not found".to_string());
    }
    let body_sql = body_pos
        .map(|pos| signature.after_params[pos..].to_string())
        .unwrap_or_else(|| format!("BEGIN\n{}\nEND", signature.after_params.trim_start()));
    let body = convert_mysql_routine_body(
        body_sql.as_str(),
        routine.sql_mode.as_str(),
        target_schema,
        &routine_param_names,
        &outer_block_labels,
        routine.kind,
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
            let body_pos = body_pos.expect("function body position checked");
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
    let close_pos = find_matching_routine_params_paren(sql, pos)
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
    let params = strip_mysql_comments(params);
    let mut result = Vec::new();
    for param in split_top_level_commas(params.as_str()) {
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

fn mysql_routine_param_names(params: &str) -> Vec<String> {
    let params = strip_mysql_comments(params);
    split_top_level_commas(params.as_str())
        .into_iter()
        .filter_map(|param| {
            let mut pos = skip_ascii_whitespace(param, 0);
            if let Some((word, next_pos)) = take_ascii_word(param, pos) {
                if matches!(word.to_ascii_uppercase().as_str(), "IN" | "OUT" | "INOUT") {
                    pos = skip_ascii_whitespace(param, next_pos);
                }
            }
            take_mysql_identifier(param, pos).map(|(name, _)| name)
        })
        .collect()
}

fn mysql_routine_outer_block_labels(before_body: &str) -> Vec<String> {
    let Some(before_colon) = before_body.trim_end().strip_suffix(':') else {
        return Vec::new();
    };
    let Some(label_token) = before_colon.split_whitespace().last() else {
        return Vec::new();
    };
    let Some((label, next_pos)) = take_sql_identifier(label_token, 0) else {
        return Vec::new();
    };
    if !label_token[next_pos..].trim().is_empty() {
        return Vec::new();
    }
    vec![label]
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

fn convert_mysql_routine_body(
    body: &str,
    sql_mode: &str,
    target_schema: &str,
    routine_param_names: &[String],
    outer_block_labels: &[String],
    routine_kind: MySqlRoutineKind,
) -> DamengRoutineBody {
    let (declarations, body) = extract_mysql_routine_declarations(body.trim());
    let not_found_handler_var = mysql_not_found_handler_variable(&declarations)
        .or_else(|| mysql_not_found_handler_variable_in_body(body.as_str()));
    let mut set_assignment_variables = routine_param_names.to_vec();
    set_assignment_variables.extend(mysql_routine_declaration_variable_names(&declarations));
    set_assignment_variables.extend(mysql_routine_declaration_variable_names_in_body(
        body.as_str(),
    ));
    let declarations = declarations
        .into_iter()
        .filter_map(|declaration| convert_mysql_routine_declaration(declaration.as_str(), sql_mode))
        .map(|declaration| format!("    {}", declaration))
        .collect::<Vec<_>>();
    let body = convert_mysql_sql_tokens(body.as_str(), sql_mode);
    let body = convert_mysql_nested_declaration_blocks(body.as_str());
    let body = convert_mysql_regexp_operators(body.as_str());
    let body = convert_mysql_information_schema_checks(body.as_str(), target_schema);
    let body = convert_mysql_alter_table_statements(body.as_str(), target_schema);
    let body = convert_mysql_temporary_table_statements(body.as_str(), target_schema);
    let body = convert_mysql_view_statements(body.as_str(), target_schema);
    let body = convert_mysql_single_quoted_aliases(body.as_str());
    let body = convert_mysql_session_set_statements(body.as_str());
    let body = convert_mysql_group_concat_calls(body.as_str());
    let user_variables = mysql_user_variables(body.as_str());
    set_assignment_variables.extend(user_variables.iter().cloned());
    let body = convert_mysql_user_variables(body.as_str());
    let body = convert_mysql_select_end_alias_into(body.as_str());
    let body = convert_mysql_postfix_select_into(body.as_str());
    let body = convert_mysql_limit_clauses(body.as_str());
    let body = convert_mysql_null_safe_equals(body.as_str());
    let body = convert_mysql_update_join_statements(body.as_str());
    let body = convert_mysql_update_order_limit_clauses(body.as_str());
    let body = convert_mysql_insert_ignore_keyword(body.as_str());
    let body = convert_mysql_insert_value_keyword(body.as_str());
    let body = convert_mysql_delete_alias_statements(body.as_str());
    let body = convert_mysql_set_assignments(body.as_str(), &set_assignment_variables);
    let body =
        convert_mysql_fetch_not_found_checks(body.as_str(), not_found_handler_var.as_deref());
    let body = convert_mysql_loop_control(body.as_str(), outer_block_labels, routine_kind);
    let body = convert_mysql_do_statements(body.as_str());
    let body = convert_mysql_prepared_statements(body.as_str());
    let body = convert_mysql_get_diagnostics(body.as_str());
    let body = convert_mysql_signal_statements(body.as_str());
    let body = convert_mysql_hash_line_comments(body.as_str());
    let declarations = mysql_user_variable_declarations(&user_variables)
        .into_iter()
        .chain(declarations)
        .collect::<Vec<_>>();
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
        let before_separator_pos = pos;
        pos = skip_mysql_declaration_separators(after_begin, pos);
        if !starts_with_keyword(after_begin, pos, "DECLARE") {
            pos = before_separator_pos;
            break;
        }
        let Some(stmt_end) = find_mysql_declare_statement_end(after_begin, pos) else {
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

fn convert_mysql_nested_declaration_blocks(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(begin_pos) = find_keyword_outside_quotes(value, search_pos, "BEGIN") {
        let Some((declarations, declarations_end)) =
            mysql_declarations_after_begin(value, begin_pos)
        else {
            search_pos = begin_pos + "BEGIN".len();
            continue;
        };
        let declarations = declarations
            .into_iter()
            .filter_map(|declaration| {
                convert_mysql_routine_declaration(declaration.as_str(), "ANSI_QUOTES")
            })
            .collect::<Vec<_>>();
        result.push_str(&value[copied_pos..begin_pos]);
        if declarations.is_empty() {
            result.push_str("BEGIN");
        } else {
            let indent = current_line_indent(value, begin_pos);
            result.push_str(format!("{}DECLARE\n", indent).as_str());
            for declaration in declarations {
                result.push_str(format!("{}    {}\n", indent, declaration).as_str());
            }
            result.push_str(format!("{}BEGIN", indent).as_str());
        }
        copied_pos = declarations_end;
        search_pos = declarations_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn mysql_declarations_after_begin(value: &str, begin_pos: usize) -> Option<(Vec<String>, usize)> {
    if !starts_with_keyword(value, begin_pos, "BEGIN") {
        return None;
    }
    let after_begin_start = begin_pos + "BEGIN".len();
    let after_begin = &value[after_begin_start..];
    let mut pos = 0usize;
    let mut declarations = Vec::new();
    loop {
        let before_separator_pos = pos;
        pos = skip_mysql_declaration_separators(after_begin, pos);
        if !starts_with_keyword(after_begin, pos, "DECLARE") {
            pos = before_separator_pos;
            break;
        }
        let Some(stmt_end) = find_mysql_declare_statement_end(after_begin, pos) else {
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
        None
    } else {
        Some((declarations, after_begin_start + pos))
    }
}

fn skip_mysql_declaration_separators(value: &str, start: usize) -> usize {
    let bytes = value.as_bytes();
    let mut pos = start;
    loop {
        pos = skip_ascii_whitespace(value, pos);
        if bytes.get(pos) == Some(&b'-') && bytes.get(pos + 1) == Some(&b'-') {
            pos = skip_line_comment(value, pos + 2);
            continue;
        }
        if bytes.get(pos) == Some(&b'#') {
            pos = skip_line_comment(value, pos + 1);
            continue;
        }
        if bytes.get(pos) == Some(&b'/') && bytes.get(pos + 1) == Some(&b'*') {
            let mut cursor = pos + 2;
            while cursor + 1 < bytes.len() {
                if bytes[cursor] == b'*' && bytes[cursor + 1] == b'/' {
                    pos = cursor + 2;
                    break;
                }
                cursor += 1;
            }
            if cursor + 1 >= bytes.len() {
                return bytes.len();
            }
            continue;
        }
        return pos;
    }
}

fn find_mysql_declare_statement_end(value: &str, start: usize) -> Option<usize> {
    let stmt_end = find_statement_semicolon_outside_quotes(value, start)?;
    let after_declare = skip_ascii_whitespace(value, start + "DECLARE".len());
    if !starts_with_keyword(value, after_declare, "CONTINUE")
        && !starts_with_keyword(value, after_declare, "EXIT")
    {
        return Some(stmt_end);
    }
    let Some(block_begin) = find_keyword_outside_quotes(value, after_declare, "BEGIN") else {
        return Some(stmt_end);
    };
    if block_begin > stmt_end {
        return Some(stmt_end);
    }
    let bytes = value.as_bytes();
    let mut search_pos = block_begin + "BEGIN".len();
    while let Some(end_pos) = find_keyword_outside_quotes(value, search_pos, "END") {
        let semicolon_pos = skip_ascii_whitespace(value, end_pos + "END".len());
        if bytes.get(semicolon_pos) == Some(&b';') {
            return Some(semicolon_pos);
        }
        search_pos = end_pos + "END".len();
    }
    Some(stmt_end)
}

fn skip_line_comment(value: &str, start: usize) -> usize {
    let bytes = value.as_bytes();
    let mut pos = start;
    while pos < bytes.len() {
        let byte = bytes[pos];
        pos += 1;
        if byte == b'\n' {
            break;
        }
    }
    pos
}

fn convert_mysql_routine_declaration(declaration: &str, sql_mode: &str) -> Option<String> {
    let trimmed = declaration.trim();
    if mysql_handler_declaration(trimmed) {
        return None;
    }
    if let Some(cursor_declaration) = convert_mysql_cursor_declaration(trimmed, sql_mode) {
        return Some(cursor_declaration);
    }
    if let Some(variable_declaration) = convert_mysql_variable_declaration(trimmed, sql_mode) {
        return Some(variable_declaration);
    }
    Some(convert_mysql_sql_tokens(trimmed, sql_mode))
}

fn convert_mysql_cursor_declaration(declaration: &str, sql_mode: &str) -> Option<String> {
    let (cursor_name, next_pos) = take_sql_identifier(declaration, 0)?;
    let mut pos = skip_ascii_whitespace(declaration, next_pos);
    if !starts_with_keyword(declaration, pos, "CURSOR") {
        return None;
    }
    pos = skip_ascii_whitespace(declaration, pos + "CURSOR".len());
    if !starts_with_keyword(declaration, pos, "FOR") {
        return None;
    }
    let select_sql = declaration[pos + "FOR".len()..].trim_start();
    if select_sql.is_empty() {
        return None;
    }
    Some(format!(
        "CURSOR {} IS {}",
        dameng_routine_param_name(cursor_name.as_str()),
        convert_mysql_sql_tokens(select_sql, sql_mode)
    ))
}

fn convert_mysql_variable_declaration(declaration: &str, sql_mode: &str) -> Option<String> {
    let (variable_names, mysql_type, rest) = mysql_variable_declaration_parts(declaration)?;
    let dameng_type = map_mysql_type_to_dameng(mysql_type);
    let rest = convert_mysql_sql_tokens(rest, sql_mode);
    let separator = if rest.is_empty() || rest.starts_with(';') {
        ""
    } else {
        " "
    };
    Some(
        variable_names
            .into_iter()
            .map(|variable_name| {
                format!(
                    "{} {}{}{}",
                    dameng_routine_param_name(variable_name.as_str()),
                    dameng_type,
                    separator,
                    rest
                )
            })
            .collect::<Vec<_>>()
            .join("\n    "),
    )
}

fn mysql_routine_declaration_variable_names(declarations: &[String]) -> Vec<String> {
    declarations
        .iter()
        .flat_map(|declaration| {
            mysql_routine_declaration_variable_names_from_declaration(declaration.as_str())
        })
        .collect()
}

fn mysql_routine_declaration_variable_names_in_body(body: &str) -> Vec<String> {
    mysql_declarations_in_body(body)
        .into_iter()
        .flat_map(|declaration| {
            mysql_routine_declaration_variable_names_from_declaration(declaration.as_str())
        })
        .collect()
}

fn mysql_routine_declaration_variable_names_from_declaration(declaration: &str) -> Vec<String> {
    let trimmed = declaration.trim();
    if mysql_handler_declaration(trimmed) {
        return Vec::new();
    }
    mysql_variable_declaration_parts(trimmed)
        .map(|(variable_names, _, _)| variable_names)
        .unwrap_or_default()
}

fn mysql_variable_declaration_parts(declaration: &str) -> Option<(Vec<String>, &str, &str)> {
    let bytes = declaration.as_bytes();
    let mut pos = skip_ascii_whitespace(declaration, 0);
    let mut variable_names = Vec::new();
    loop {
        let (variable_name, next_pos) = take_sql_identifier(declaration, pos)?;
        variable_names.push(variable_name);
        pos = skip_ascii_whitespace(declaration, next_pos);
        if bytes.get(pos) == Some(&b',') {
            pos = skip_ascii_whitespace(declaration, pos + 1);
            continue;
        }
        break;
    }
    if starts_with_keyword(declaration, pos, "CURSOR")
        || starts_with_keyword(declaration, pos, "CONDITION")
    {
        return None;
    }
    let (mysql_type, type_len) = take_mysql_type_token(&declaration[pos..])?;
    let rest = declaration[pos + type_len..].trim_start();
    Some((variable_names, mysql_type, rest))
}

fn mysql_not_found_handler_variable(declarations: &[String]) -> Option<String> {
    declarations
        .iter()
        .find_map(|declaration| mysql_not_found_handler_variable_from_declaration(declaration))
}

fn mysql_not_found_handler_variable_in_body(body: &str) -> Option<String> {
    mysql_declarations_in_body(body)
        .into_iter()
        .find_map(|declaration| {
            mysql_not_found_handler_variable_from_declaration(declaration.as_str())
        })
}

fn mysql_declarations_in_body(body: &str) -> Vec<String> {
    let mut declarations = Vec::new();
    let mut search_pos = 0usize;
    while let Some(declare_pos) = find_keyword_outside_quotes(body, search_pos, "DECLARE") {
        let Some(stmt_end) = find_mysql_declare_statement_end(body, declare_pos) else {
            break;
        };
        let declaration = body[declare_pos + "DECLARE".len()..=stmt_end].trim();
        if !declaration.is_empty() {
            declarations.push(declaration.to_string());
        }
        search_pos = stmt_end + 1;
    }
    declarations
}

fn mysql_handler_declaration(declaration: &str) -> bool {
    let trimmed = declaration.trim();
    let Some((handler_type, next_pos)) = take_ascii_word(trimmed, 0) else {
        return false;
    };
    if !matches!(
        handler_type.to_ascii_uppercase().as_str(),
        "CONTINUE" | "EXIT"
    ) {
        return false;
    }
    let handler_pos = skip_ascii_whitespace(trimmed, next_pos);
    starts_with_keyword(trimmed, handler_pos, "HANDLER")
}

fn mysql_not_found_handler_variable_from_declaration(declaration: &str) -> Option<String> {
    let trimmed = declaration.trim();
    if !starts_with_keyword(trimmed, 0, "CONTINUE") {
        return None;
    }
    let mut pos = skip_ascii_whitespace(trimmed, "CONTINUE".len());
    if !starts_with_keyword(trimmed, pos, "HANDLER") {
        return None;
    }
    pos = skip_ascii_whitespace(trimmed, pos + "HANDLER".len());
    if !starts_with_keyword(trimmed, pos, "FOR") {
        return None;
    }
    pos = skip_ascii_whitespace(trimmed, pos + "FOR".len());
    if starts_with_keyword(trimmed, pos, "NOT") {
        pos = skip_ascii_whitespace(trimmed, pos + "NOT".len());
        if !starts_with_keyword(trimmed, pos, "FOUND") {
            return None;
        }
        pos = skip_ascii_whitespace(trimmed, pos + "FOUND".len());
    } else if starts_with_keyword(trimmed, pos, "SQLSTATE") {
        pos = skip_ascii_whitespace(trimmed, pos + "SQLSTATE".len());
        let (sql_state, next_pos) = take_mysql_single_quoted_string(trimmed, pos)?;
        if sql_state != "02000" {
            return None;
        }
        pos = skip_ascii_whitespace(trimmed, next_pos);
    } else {
        return None;
    }
    if !starts_with_keyword(trimmed, pos, "SET") {
        return None;
    }
    let assignment = trim_statement_semicolon(trimmed[pos + "SET".len()..].trim_start());
    let eq_pos = find_char_outside_quotes(assignment, '=')?;
    let variable = assignment[..eq_pos].trim();
    if variable.is_empty() || variable.contains(char::is_whitespace) || variable.contains(',') {
        return None;
    }
    Some(identifier_name_from_token(variable))
}

fn convert_mysql_set_assignments(body: &str, assignment_variables: &[String]) -> String {
    let assignment_variables = assignment_variables
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    body.lines()
        .map(|line| convert_mysql_set_assignment_line(line, &assignment_variables))
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_mysql_set_assignment_line(line: &str, assignment_variables: &[String]) -> String {
    if !starts_with_keyword(line.trim_start(), 0, "SET") {
        return line.to_string();
    }

    let mut result = String::with_capacity(line.len());
    let mut pos = 0usize;
    while pos < line.len() {
        let segment_end = find_statement_semicolon_outside_quotes(line, pos)
            .map(|end| end + 1)
            .unwrap_or(line.len());
        result.push_str(
            convert_mysql_set_assignment_segment(&line[pos..segment_end], assignment_variables)
                .as_str(),
        );
        pos = segment_end;
    }
    result
}

fn convert_mysql_set_assignment_segment(segment: &str, assignment_variables: &[String]) -> String {
    let leading_len = segment.len() - segment.trim_start().len();
    let leading = &segment[..leading_len];
    let trimmed = &segment[leading_len..];
    if !starts_with_keyword(trimmed, 0, "SET") {
        return segment.to_string();
    }
    let rest = trimmed["SET".len()..].trim_start();
    if split_top_level_commas(rest).len() > 1 {
        return segment.to_string();
    }
    let Some(eq_pos) = find_char_outside_quotes(rest, '=') else {
        return segment.to_string();
    };
    let lhs = rest[..eq_pos].trim();
    if lhs.is_empty() || lhs.contains(char::is_whitespace) || lhs.contains(',') {
        return segment.to_string();
    }
    let lhs_name = identifier_name_from_token(lhs).to_ascii_lowercase();
    if !assignment_variables
        .iter()
        .any(|name| name.eq_ignore_ascii_case(lhs_name.as_str()))
    {
        return segment.to_string();
    }
    format!("{}{} :={}", leading, lhs, &rest[eq_pos + 1..])
}

fn convert_mysql_fetch_not_found_checks(body: &str, handler_var: Option<&str>) -> String {
    let Some(handler_var) = handler_var else {
        return body.to_string();
    };
    body.lines()
        .map(|line| convert_mysql_fetch_not_found_check_line(line, handler_var))
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_mysql_fetch_not_found_check_line(line: &str, handler_var: &str) -> String {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "FETCH") {
        return line.to_string();
    }
    let cursor_pos = skip_ascii_whitespace(trimmed, "FETCH".len());
    let Some((cursor_name, next_pos)) = take_sql_identifier(trimmed, cursor_pos) else {
        return line.to_string();
    };
    let into_pos = skip_ascii_whitespace(trimmed, next_pos);
    if !starts_with_keyword(trimmed, into_pos, "INTO") {
        return line.to_string();
    }
    format!(
        "{}\n{}IF {}%NOTFOUND THEN\n{}    {} := 1;\n{}END IF;",
        line, leading, cursor_name, leading, handler_var, leading
    )
}

fn convert_mysql_do_statements(body: &str) -> String {
    body.lines()
        .map(convert_mysql_do_statement_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_mysql_do_statement_line(line: &str) -> String {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if starts_with_keyword(trimmed, 0, "DO") {
        format!("{}NULL;", leading)
    } else {
        line.to_string()
    }
}

fn convert_mysql_loop_control(
    body: &str,
    outer_block_labels: &[String],
    routine_kind: MySqlRoutineKind,
) -> String {
    let outer_block_labels = outer_block_labels
        .iter()
        .map(|label| label.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut block_labels = Vec::new();
    let lines = body.lines().collect::<Vec<_>>();
    let mut result = Vec::with_capacity(lines.len());
    let mut idx = 0usize;
    while idx < lines.len() {
        if let Some((converted, label)) = convert_mysql_labeled_block_begin(lines[idx]) {
            block_labels.push(label.to_ascii_lowercase());
            result.push(converted);
            idx += 1;
            continue;
        }
        if let Some(converted) = convert_mysql_labeled_block_end(lines[idx], &mut block_labels) {
            result.push(converted);
            idx += 1;
            continue;
        }
        if let Some(converted) = convert_mysql_standalone_loop_label(lines[idx], lines.get(idx + 1))
        {
            result.push(converted);
            idx += 1;
            continue;
        }
        if let Some(converted) = convert_mysql_multiline_while_do(lines[idx], lines.get(idx + 1)) {
            result.push(converted);
            idx += 2;
            continue;
        }
        result.push(convert_mysql_loop_control_line(
            lines[idx],
            &outer_block_labels,
            &block_labels,
            routine_kind,
        ));
        idx += 1;
    }
    result.join("\n")
}

fn convert_mysql_labeled_block_begin(line: &str) -> Option<(String, String)> {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    let colon_pos = find_char_outside_quotes(trimmed, ':')?;
    let label = trimmed[..colon_pos].trim();
    if !is_mysql_loop_label(label) {
        return None;
    }
    let begin_pos = skip_ascii_whitespace(trimmed, colon_pos + 1);
    if !starts_with_keyword(trimmed, begin_pos, "BEGIN") {
        return None;
    }
    let rest = &trimmed[begin_pos + "BEGIN".len()..];
    Some((
        format!("{}BEGIN{}", leading, rest),
        identifier_name_from_token(label),
    ))
}

fn convert_mysql_labeled_block_end(line: &str, block_labels: &mut Vec<String>) -> Option<String> {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "END") {
        return None;
    }
    let label_pos = skip_ascii_whitespace(trimmed, "END".len());
    if starts_with_keyword(trimmed, label_pos, "IF")
        || starts_with_keyword(trimmed, label_pos, "LOOP")
        || starts_with_keyword(trimmed, label_pos, "WHILE")
        || starts_with_keyword(trimmed, label_pos, "REPEAT")
    {
        return None;
    }
    let label = trim_statement_semicolon(&trimmed[label_pos..]);
    if label.is_empty() {
        return None;
    }
    let label = identifier_name_from_token(label);
    let label_key = label.to_ascii_lowercase();
    let pos = block_labels
        .iter()
        .rposition(|active_label| active_label == &label_key)?;
    block_labels.truncate(pos);
    Some(format!(
        "{}END;\n{}<<{}_end>>\n{}NULL;",
        leading, leading, label, leading
    ))
}

fn convert_mysql_standalone_loop_label(line: &str, next_line: Option<&&str>) -> Option<String> {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = line[leading_len..].trim_end();
    let label = trimmed.strip_suffix(':')?.trim();
    if !is_mysql_loop_label(label) {
        return None;
    }
    let next_trimmed = next_line?.trim_start();
    if !starts_with_keyword(next_trimmed, 0, "WHILE")
        && !starts_with_keyword(next_trimmed, 0, "LOOP")
        && !starts_with_keyword(next_trimmed, 0, "REPEAT")
    {
        return None;
    }
    Some(format!(
        "{}<<{}>>",
        leading,
        identifier_name_from_token(label)
    ))
}

fn convert_mysql_multiline_while_do(line: &str, next_line: Option<&&str>) -> Option<String> {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    let (label, while_sql) = split_mysql_loop_label(trimmed)
        .map(|(label, while_sql)| (Some(label), while_sql))
        .unwrap_or((None, trimmed));
    if !starts_with_keyword(while_sql, 0, "WHILE")
        || find_keyword_outside_quotes(while_sql, "WHILE".len(), "DO").is_some()
    {
        return None;
    }
    let next_trimmed = next_line?.trim();
    if !next_trimmed.eq_ignore_ascii_case("DO") && !next_trimmed.eq_ignore_ascii_case("DO;") {
        return None;
    }
    let mut converted = String::new();
    if let Some(label) = label {
        converted.push_str(leading);
        converted.push_str("<<");
        converted.push_str(identifier_name_from_token(label).as_str());
        converted.push_str(">>\n");
    }
    converted.push_str(leading);
    converted.push_str(while_sql.trim_end());
    converted.push_str(" LOOP");
    Some(converted)
}

fn convert_mysql_loop_control_line(
    line: &str,
    outer_block_labels: &[String],
    block_labels: &[String],
    routine_kind: MySqlRoutineKind,
) -> String {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if starts_with_keyword(trimmed, 0, "LEAVE") {
        let label = trim_statement_semicolon(trimmed["LEAVE".len()..].trim_start());
        if !label.is_empty() {
            let label = identifier_name_from_token(label);
            if matches!(routine_kind, MySqlRoutineKind::Procedure)
                && outer_block_labels.contains(&label.to_ascii_lowercase())
            {
                return format!("{}RETURN;", leading);
            }
            if block_labels.contains(&label.to_ascii_lowercase()) {
                return format!("{}GOTO {}_end;", leading, label);
            }
            return format!("{}EXIT {};", leading, label);
        }
    }
    if starts_with_keyword(trimmed, 0, "ITERATE") {
        let label = trim_statement_semicolon(trimmed["ITERATE".len()..].trim_start());
        if !label.is_empty() {
            return format!("{}CONTINUE {};", leading, identifier_name_from_token(label));
        }
    }
    if starts_with_keyword(trimmed, 0, "END") {
        let after_end = skip_ascii_whitespace(trimmed, "END".len());
        if starts_with_keyword(trimmed, after_end, "IF") && !trimmed.trim_end().ends_with(';') {
            return format!("{}{};", leading, trimmed.trim_end());
        }
        if starts_with_keyword(trimmed, after_end, "WHILE") {
            return format!("{}END LOOP;", leading);
        }
    }
    if starts_with_keyword(trimmed, 0, "WHILE") {
        if let Some(do_pos) = find_keyword_outside_quotes(trimmed, "WHILE".len(), "DO") {
            return format!(
                "{}{}LOOP{}",
                leading,
                &trimmed[..do_pos],
                &trimmed[do_pos + "DO".len()..]
            );
        }
    }
    let Some(colon_pos) = find_char_outside_quotes(trimmed, ':') else {
        return line.to_string();
    };
    let label = trimmed[..colon_pos].trim();
    if label.is_empty() || label.contains(char::is_whitespace) {
        return line.to_string();
    }
    let loop_pos = skip_ascii_whitespace(trimmed, colon_pos + 1);
    if starts_with_keyword(trimmed, loop_pos, "WHILE") {
        let while_sql = &trimmed[loop_pos..];
        if let Some(do_pos) = find_keyword_outside_quotes(while_sql, "WHILE".len(), "DO") {
            return format!(
                "{}<<{}>>\n{}{}LOOP{}",
                leading,
                identifier_name_from_token(label),
                leading,
                &while_sql[..do_pos],
                &while_sql[do_pos + "DO".len()..]
            );
        }
    }
    if !starts_with_keyword(trimmed, loop_pos, "LOOP") {
        return line.to_string();
    }
    let rest = &trimmed[loop_pos + "LOOP".len()..];
    format!(
        "{}<<{}>>\n{}LOOP{}",
        leading,
        identifier_name_from_token(label),
        leading,
        rest
    )
}

fn split_mysql_loop_label(value: &str) -> Option<(&str, &str)> {
    let colon_pos = find_char_outside_quotes(value, ':')?;
    let label = value[..colon_pos].trim();
    if !is_mysql_loop_label(label) {
        return None;
    }
    let body_pos = skip_ascii_whitespace(value, colon_pos + 1);
    Some((label, &value[body_pos..]))
}

fn is_mysql_loop_label(value: &str) -> bool {
    if value.is_empty() || value.contains(char::is_whitespace) {
        return false;
    }
    let Some((_, next_pos)) = take_sql_identifier(value, 0) else {
        return false;
    };
    value[next_pos..].trim().is_empty()
}

fn convert_mysql_prepared_statements(body: &str) -> String {
    let mut prepared = Vec::<(String, String)>::new();
    let mut result = Vec::new();
    for line in body.lines() {
        if let Some((statement_name, sql_expression)) = parse_mysql_prepare_statement(line) {
            prepared.push((statement_name.to_ascii_lowercase(), sql_expression));
            continue;
        }
        if let Some((leading, statement_name)) = parse_mysql_execute_prepared_statement(line) {
            if let Some((_, sql_expression)) = prepared
                .iter()
                .rev()
                .find(|(name, _)| name.eq_ignore_ascii_case(statement_name.as_str()))
            {
                result.push(format!("{}EXECUTE IMMEDIATE {};", leading, sql_expression));
                continue;
            }
        }
        if parse_mysql_deallocate_prepare_statement(line).is_some() {
            continue;
        }
        result.push(line.to_string());
    }
    result.join("\n")
}

fn parse_mysql_prepare_statement(line: &str) -> Option<(String, String)> {
    let leading_len = line.len() - line.trim_start().len();
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "PREPARE") {
        return None;
    }
    let statement_pos = skip_ascii_whitespace(trimmed, "PREPARE".len());
    let (statement_name, next_pos) = take_sql_identifier(trimmed, statement_pos)?;
    let from_pos = skip_ascii_whitespace(trimmed, next_pos);
    if !starts_with_keyword(trimmed, from_pos, "FROM") {
        return None;
    }
    let sql_expression = trim_statement_semicolon(trimmed[from_pos + "FROM".len()..].trim_start());
    if sql_expression.is_empty() {
        return None;
    }
    Some((statement_name, sql_expression.to_string()))
}

fn parse_mysql_execute_prepared_statement(line: &str) -> Option<(String, String)> {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "EXECUTE") {
        return None;
    }
    let statement_pos = skip_ascii_whitespace(trimmed, "EXECUTE".len());
    let (statement_name, next_pos) = take_sql_identifier(trimmed, statement_pos)?;
    if statement_name.eq_ignore_ascii_case("IMMEDIATE") {
        return None;
    }
    let rest = trim_statement_semicolon(trimmed[next_pos..].trim_start());
    if !rest.is_empty() {
        return None;
    }
    Some((leading.to_string(), statement_name))
}

fn parse_mysql_deallocate_prepare_statement(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    if !starts_with_keyword(trimmed, 0, "DEALLOCATE") {
        return None;
    }
    let prepare_pos = skip_ascii_whitespace(trimmed, "DEALLOCATE".len());
    if !starts_with_keyword(trimmed, prepare_pos, "PREPARE") {
        return None;
    }
    let statement_pos = skip_ascii_whitespace(trimmed, prepare_pos + "PREPARE".len());
    let (statement_name, next_pos) = take_sql_identifier(trimmed, statement_pos)?;
    if !trim_statement_semicolon(trimmed[next_pos..].trim_start()).is_empty() {
        return None;
    }
    Some(statement_name)
}

fn convert_mysql_get_diagnostics(body: &str) -> String {
    body.lines()
        .map(convert_mysql_get_diagnostics_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_mysql_get_diagnostics_line(line: &str) -> String {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "GET") {
        return line.to_string();
    }
    let diagnostics_pos = skip_ascii_whitespace(trimmed, "GET".len());
    if !starts_with_keyword(trimmed, diagnostics_pos, "DIAGNOSTICS") {
        return line.to_string();
    }
    let variable_pos = skip_ascii_whitespace(trimmed, diagnostics_pos + "DIAGNOSTICS".len());
    if starts_with_keyword(trimmed, variable_pos, "CONDITION") {
        return line.to_string();
    }
    let Some((variable_name, next_pos)) = take_sql_identifier(trimmed, variable_pos) else {
        return line.to_string();
    };
    let eq_pos = skip_ascii_whitespace(trimmed, next_pos);
    if trimmed.as_bytes().get(eq_pos) != Some(&b'=') {
        return line.to_string();
    }
    let row_count_pos = skip_ascii_whitespace(trimmed, eq_pos + 1);
    if !starts_with_keyword(trimmed, row_count_pos, "ROW_COUNT") {
        return line.to_string();
    }
    let rest = trim_statement_semicolon(trimmed[row_count_pos + "ROW_COUNT".len()..].trim_start());
    if !rest.is_empty() {
        return line.to_string();
    }
    format!("{}{} := SQL%ROWCOUNT;", leading, variable_name)
}

fn convert_mysql_signal_statements(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(signal_pos) = find_keyword_outside_quotes(value, search_pos, "SIGNAL") {
        let statement_end = find_statement_semicolon_outside_quotes(value, signal_pos)
            .map(|pos| pos + 1)
            .unwrap_or(value.len());
        let statement = &value[signal_pos..statement_end];
        let Some(converted) = convert_mysql_signal_statement(statement) else {
            search_pos = signal_pos + "SIGNAL".len();
            continue;
        };
        result.push_str(&value[copied_pos..signal_pos]);
        let indent = current_line_indent(value, signal_pos);
        result.push_str(indent_converted_statement(converted.as_str(), indent.as_str()).as_str());
        copied_pos = statement_end;
        search_pos = statement_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn convert_mysql_signal_statement(statement: &str) -> Option<String> {
    let trimmed = trim_statement_semicolon(statement.trim());
    if !starts_with_keyword(trimmed, 0, "SIGNAL") {
        return None;
    }
    let mut pos = skip_ascii_whitespace(trimmed, "SIGNAL".len());
    if !starts_with_keyword(trimmed, pos, "SQLSTATE") {
        return None;
    }
    pos = skip_ascii_whitespace(trimmed, pos + "SQLSTATE".len());
    let (_sqlstate, next_pos) = take_mysql_single_quoted_string(trimmed, pos)?;
    pos = skip_ascii_whitespace(trimmed, next_pos);
    if !starts_with_keyword(trimmed, pos, "SET") {
        return Some("RAISE_APPLICATION_ERROR(-20000, 'SQLSTATE 45000');".to_string());
    }
    pos = skip_ascii_whitespace(trimmed, pos + "SET".len());
    if !starts_with_keyword(trimmed, pos, "MESSAGE_TEXT") {
        return None;
    }
    pos = skip_ascii_whitespace(trimmed, pos + "MESSAGE_TEXT".len());
    if trimmed.as_bytes().get(pos) != Some(&b'=') {
        return None;
    }
    let message = trimmed[pos + 1..].trim();
    if message.is_empty() {
        return None;
    }
    Some(format!("RAISE_APPLICATION_ERROR(-20000, {});", message))
}

fn convert_mysql_hash_line_comments(value: &str) -> String {
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

        if byte == b'#' {
            result.push_str("--");
            let next_pos = pos + 1;
            if next_pos < bytes.len() {
                let next = value[next_pos..].chars().next().unwrap();
                if !next.is_whitespace() {
                    result.push(' ');
                }
            }
            pos += 1;
            continue;
        }

        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn convert_mysql_select_end_alias_into(body: &str) -> String {
    body.lines()
        .map(convert_mysql_select_end_alias_into_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_mysql_select_end_alias_into_line(line: &str) -> String {
    let leading_len = line.len() - line.trim_start().len();
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "SELECT") {
        return line.to_string();
    }
    let Some(into_pos) = find_keyword_outside_quotes(line, 0, "INTO") else {
        return line.to_string();
    };
    let Some(end_pos) = last_keyword_outside_quotes_before(line, "END", into_pos) else {
        return line.to_string();
    };
    if skip_ascii_whitespace(line, end_pos + "END".len()) != into_pos {
        return line.to_string();
    }
    if find_keyword_outside_quotes(&line[..end_pos], 0, "CASE").is_some() {
        return line.to_string();
    }
    format!(
        "{}{}",
        line[..end_pos].trim_end(),
        &line[into_pos.saturating_sub(1)..]
    )
}

fn last_keyword_outside_quotes_before(value: &str, keyword: &str, before: usize) -> Option<usize> {
    let mut result = None;
    let mut search_pos = 0usize;
    while let Some(pos) = find_keyword_outside_quotes(value, search_pos, keyword) {
        if pos >= before {
            break;
        }
        result = Some(pos);
        search_pos = pos + keyword.len();
    }
    result
}

fn convert_mysql_postfix_select_into(body: &str) -> String {
    let mut result = String::with_capacity(body.len());
    let mut stmt_start = 0usize;
    while let Some(stmt_end) = find_statement_semicolon_outside_quotes(body, stmt_start) {
        result.push_str(
            convert_mysql_postfix_select_into_statement(&body[stmt_start..=stmt_end]).as_str(),
        );
        stmt_start = stmt_end + 1;
    }
    if stmt_start < body.len() {
        result.push_str(convert_mysql_postfix_select_into_statement(&body[stmt_start..]).as_str());
    }
    result
}

fn convert_mysql_postfix_select_into_statement(statement: &str) -> String {
    if !starts_with_keyword(statement.trim_start(), 0, "SELECT") {
        if let Some(select_pos) = find_keyword_outside_quotes(statement, 0, "SELECT") {
            let prefix = &statement[..select_pos];
            let prefix_trimmed = prefix.trim();
            let prefix_upper = prefix_trimmed.to_ascii_uppercase();
            if prefix_trimmed.eq_ignore_ascii_case("BEGIN")
                || prefix_upper.ends_with(" THEN")
                || prefix_trimmed.eq_ignore_ascii_case("ELSE")
            {
                let converted =
                    convert_mysql_postfix_select_into_statement(&statement[select_pos..]);
                if converted != statement[select_pos..] {
                    return format!("{}{}", prefix, converted);
                }
            }
        }
        return statement.to_string();
    }
    let statement_trimmed_end_len = statement.trim_end().len();
    let has_semicolon = statement[..statement_trimmed_end_len].ends_with(';');
    let statement_without_semicolon = if has_semicolon {
        &statement[..statement_trimmed_end_len - 1]
    } else {
        &statement[..statement_trimmed_end_len]
    };
    let Some(into_pos) = last_keyword_outside_quotes_before(
        statement_without_semicolon,
        "INTO",
        statement_without_semicolon.len(),
    ) else {
        return statement.to_string();
    };
    let Some(from_pos) = find_keyword_outside_quotes(statement_without_semicolon, 0, "FROM") else {
        return statement.to_string();
    };
    if into_pos < from_pos {
        return statement.to_string();
    }
    let target = statement_without_semicolon[into_pos + "INTO".len()..].trim();
    if target.is_empty() || target.contains(';') {
        return statement.to_string();
    }
    let before_into = statement_without_semicolon[..into_pos].trim_end();
    let before_from = before_into[..from_pos].trim_end();
    let after_from = before_into[from_pos..].trim_start();
    let semicolon = if has_semicolon { ";" } else { "" };
    format!(
        "{} INTO {} {}{}",
        before_from, target, after_from, semicolon
    )
}

fn convert_mysql_information_schema_checks(value: &str, target_schema: &str) -> String {
    let value = replace_case_insensitive_outside_quotes(
        value,
        "information_schema.COLUMNS",
        "ALL_TAB_COLUMNS",
    );
    let value = replace_case_insensitive_outside_quotes(
        value.as_str(),
        "information_schema.statistics",
        "ALL_INDEXES",
    );
    let value = replace_case_insensitive_outside_quotes(
        value.as_str(),
        "information_schema.VIEWS",
        "ALL_VIEWS",
    );
    let value = replace_mysql_database_schema_checks(value.as_str(), target_schema);
    let value = replace_metadata_column_predicates(value.as_str(), "table_schema", "OWNER");
    let value = replace_metadata_column_predicates(value.as_str(), "table_name", "TABLE_NAME");
    let value = replace_metadata_column_predicates(value.as_str(), "column_name", "COLUMN_NAME");
    let value = replace_metadata_column_predicates(value.as_str(), "index_name", "INDEX_NAME");
    replace_case_insensitive_outside_quotes(
        value.as_str(),
        "ALL_VIEWS WHERE TABLE_NAME",
        "ALL_VIEWS WHERE VIEW_NAME",
    )
}

fn convert_mysql_regexp_operators(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(regexp_pos) = find_keyword_outside_quotes(value, search_pos, "REGEXP") {
        let (operator_start, operator_negated) = mysql_regexp_operator_start(value, regexp_pos);
        let Some(lhs_start) = mysql_regexp_left_operand_start(value, operator_start) else {
            search_pos = regexp_pos + "REGEXP".len();
            continue;
        };
        let rhs_start = skip_ascii_whitespace(value, regexp_pos + "REGEXP".len());
        let Some(rhs_end) = mysql_regexp_right_operand_end(value, rhs_start) else {
            search_pos = regexp_pos + "REGEXP".len();
            continue;
        };
        let mut next_pos = rhs_end;
        let mut negated = operator_negated;
        let compare_pos = skip_ascii_whitespace(value, rhs_end);
        if bytes.get(compare_pos) == Some(&b'=') {
            let value_pos = skip_ascii_whitespace(value, compare_pos + 1);
            match bytes.get(value_pos) {
                Some(b'0') => {
                    negated = !negated;
                    next_pos = value_pos + 1;
                }
                Some(b'1') => {
                    next_pos = value_pos + 1;
                }
                _ => {}
            }
        }
        let lhs = value[lhs_start..operator_start].trim();
        let rhs = value[rhs_start..rhs_end].trim();
        if lhs.is_empty() || rhs.is_empty() {
            search_pos = regexp_pos + "REGEXP".len();
            continue;
        }
        result.push_str(&value[copied_pos..lhs_start]);
        if negated {
            result.push_str("NOT ");
        }
        result.push_str(format!("REGEXP_LIKE({}, {})", lhs, rhs).as_str());
        copied_pos = next_pos;
        search_pos = next_pos;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn mysql_regexp_operator_start(value: &str, regexp_pos: usize) -> (usize, bool) {
    let bytes = value.as_bytes();
    let mut pos = regexp_pos;
    while pos > 0 && bytes[pos - 1].is_ascii_whitespace() {
        pos -= 1;
    }
    let word_end = pos;
    while pos > 0 && is_ascii_identifier_byte(bytes[pos - 1]) {
        pos -= 1;
    }
    if pos < word_end && starts_with_keyword(value, pos, "NOT") {
        (pos, true)
    } else {
        (regexp_pos, false)
    }
}

fn convert_mysql_session_set_statements(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(set_pos) = find_keyword_outside_quotes(value, search_pos, "SET") {
        let statement_end = find_statement_semicolon_outside_quotes(value, set_pos)
            .map(|pos| pos + 1)
            .unwrap_or(value.len());
        let statement = &value[set_pos..statement_end];
        if !is_mysql_session_set_statement(statement) {
            search_pos = set_pos + "SET".len();
            continue;
        }
        result.push_str(&value[copied_pos..set_pos]);
        let indent = current_line_indent(value, set_pos);
        result.push_str(format!("{}NULL;", indent).as_str());
        copied_pos = statement_end;
        search_pos = statement_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn is_mysql_session_set_statement(statement: &str) -> bool {
    if !starts_with_keyword(statement, 0, "SET") {
        return false;
    }
    let pos = skip_ascii_whitespace(statement, "SET".len());
    starts_with_keyword(statement, pos, "SESSION")
        || starts_with_keyword(statement, pos, "GLOBAL")
        || statement[pos..].starts_with("@@")
        || starts_with_keyword(statement, pos, "sql_safe_updates")
}

fn convert_mysql_group_concat_calls(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(call_pos) = find_keyword_outside_quotes(value, search_pos, "GROUP_CONCAT") {
        let open_pos = skip_ascii_whitespace(value, call_pos + "GROUP_CONCAT".len());
        if value.as_bytes().get(open_pos) != Some(&b'(') {
            search_pos = call_pos + "GROUP_CONCAT".len();
            continue;
        }
        let Some(close_pos) = find_matching_paren(value, open_pos) else {
            search_pos = call_pos + "GROUP_CONCAT".len();
            continue;
        };
        let Some(converted) = convert_mysql_group_concat_args(&value[open_pos + 1..close_pos])
        else {
            search_pos = call_pos + "GROUP_CONCAT".len();
            continue;
        };
        result.push_str(&value[copied_pos..call_pos]);
        result.push_str(converted.as_str());
        copied_pos = close_pos + 1;
        search_pos = close_pos + 1;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn convert_mysql_group_concat_args(args: &str) -> Option<String> {
    let mut expr = args.trim();
    let mut distinct = false;
    if starts_with_keyword(expr, 0, "DISTINCT") {
        distinct = true;
        expr = expr["DISTINCT".len()..].trim_start();
    }
    let mut separator = "','";
    if let Some(separator_pos) = find_keyword_outside_quotes(expr, 0, "SEPARATOR") {
        separator = expr[separator_pos + "SEPARATOR".len()..].trim();
        expr = expr[..separator_pos].trim_end();
    }
    let mut order_by = None;
    if let Some(order_pos) = find_keyword_outside_quotes(expr, 0, "ORDER") {
        let by_pos = skip_ascii_whitespace(expr, order_pos + "ORDER".len());
        if starts_with_keyword(expr, by_pos, "BY") {
            order_by = Some(expr[by_pos + "BY".len()..].trim());
            expr = expr[..order_pos].trim_end();
        }
    }
    if expr.is_empty() || separator.is_empty() {
        return None;
    }
    let listagg_expr = if distinct {
        format!("DISTINCT {}", expr)
    } else {
        expr.to_string()
    };
    let order_by = order_by.filter(|value| !value.is_empty()).unwrap_or(expr);
    Some(format!(
        "LISTAGG({}, {}) WITHIN GROUP (ORDER BY {})",
        listagg_expr, separator, order_by
    ))
}

fn mysql_regexp_left_operand_start(value: &str, regexp_pos: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut pos = regexp_pos;
    while pos > 0 && bytes[pos - 1].is_ascii_whitespace() {
        pos -= 1;
    }
    let end = pos;
    if bytes.get(end.checked_sub(1)?) == Some(&b')') {
        let open_pos = find_matching_open_paren(value, end - 1)?;
        pos = open_pos;
        while pos > 0 && bytes[pos - 1].is_ascii_whitespace() {
            pos -= 1;
        }
        while pos > 0 {
            let byte = bytes[pos - 1];
            if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'"' | b'`') {
                pos -= 1;
            } else {
                break;
            }
        }
        return Some(pos);
    }
    while pos > 0 {
        let byte = bytes[pos - 1];
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'"' | b'`') {
            pos -= 1;
        } else {
            break;
        }
    }
    if pos == end { None } else { Some(pos) }
}

fn find_matching_open_paren(value: &str, close_pos: usize) -> Option<usize> {
    if value.as_bytes().get(close_pos) != Some(&b')') {
        return None;
    }
    let bytes = value.as_bytes();
    let mut depth = 0u32;
    let mut pos = close_pos + 1;
    while pos > 0 {
        pos -= 1;
        match bytes[pos] {
            b')' => depth += 1,
            b'(' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(pos);
                }
            }
            _ => {}
        }
    }
    None
}

fn mysql_regexp_right_operand_end(value: &str, start: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let first = *bytes.get(start)?;
    if matches!(first, b'\'' | b'"') {
        let quote = first;
        let mut pos = start + 1;
        while pos < bytes.len() {
            let byte = bytes[pos];
            if byte == b'\\' && quote == b'\'' {
                pos = (pos + 2).min(bytes.len());
                continue;
            }
            if byte == quote {
                if bytes.get(pos + 1) == Some(&quote) {
                    pos += 2;
                } else {
                    return Some(pos + 1);
                }
            } else {
                pos += 1;
            }
        }
        return None;
    }
    let mut pos = start;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if byte.is_ascii_whitespace() || matches!(byte, b',' | b')' | b';') {
            break;
        }
        pos += 1;
    }
    if pos == start { None } else { Some(pos) }
}

fn convert_mysql_alter_table_statements(value: &str, target_schema: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(alter_pos) = find_keyword_outside_quotes(value, search_pos, "ALTER") {
        let statement_end = find_statement_semicolon_outside_quotes(value, alter_pos)
            .map(|pos| pos + 1)
            .unwrap_or(value.len());
        let statement = &value[alter_pos..statement_end];
        let Some(sql) = convert_mysql_alter_table_statement(statement, target_schema) else {
            search_pos = alter_pos + "ALTER".len();
            continue;
        };
        result.push_str(&value[copied_pos..alter_pos]);
        let indent = current_line_indent(value, alter_pos);
        result.push_str(indent_converted_statement(sql.as_str(), indent.as_str()).as_str());
        copied_pos = statement_end;
        search_pos = statement_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn current_line_indent(value: &str, pos: usize) -> String {
    let line_start = value[..pos]
        .rfind('\n')
        .map(|newline_pos| newline_pos + 1)
        .unwrap_or(0);
    value[line_start..pos]
        .chars()
        .take_while(|ch| ch.is_ascii_whitespace() && *ch != '\r' && *ch != '\n')
        .collect()
}

fn indent_converted_statement(sql: &str, indent: &str) -> String {
    sql.replace('\n', format!("\n{}", indent).as_str())
}

fn convert_mysql_alter_table_statement(statement: &str, target_schema: &str) -> Option<String> {
    if !starts_with_keyword(statement, 0, "ALTER") {
        return None;
    }
    let mut pos = skip_ascii_whitespace(statement, "ALTER".len());
    if !starts_with_keyword(statement, pos, "TABLE") {
        return None;
    }
    pos = skip_ascii_whitespace(statement, pos + "TABLE".len());
    let (table_name, next_pos) = take_sql_table_identifier(statement, pos)?;
    pos = skip_ascii_whitespace(statement, next_pos);
    if !starts_with_keyword(statement, pos, "ADD") {
        if starts_with_keyword(statement, pos, "MODIFY") {
            pos = skip_ascii_whitespace(statement, pos + "MODIFY".len());
            if starts_with_keyword(statement, pos, "COLUMN") {
                pos = skip_ascii_whitespace(statement, pos + "COLUMN".len());
            }
            return convert_mysql_modify_column_statement(
                statement,
                pos,
                target_schema,
                table_name.as_str(),
            );
        }
        return None;
    }
    pos = skip_ascii_whitespace(statement, pos + "ADD".len());
    let mut unique = false;
    if starts_with_keyword(statement, pos, "UNIQUE") {
        unique = true;
        pos = skip_ascii_whitespace(statement, pos + "UNIQUE".len());
    }
    if starts_with_keyword(statement, pos, "INDEX") {
        return convert_mysql_add_index_statement(
            statement,
            pos + "INDEX".len(),
            target_schema,
            table_name.as_str(),
            unique,
        );
    }
    if starts_with_keyword(statement, pos, "KEY") {
        return convert_mysql_add_index_statement(
            statement,
            pos + "KEY".len(),
            target_schema,
            table_name.as_str(),
            unique,
        );
    }
    if starts_with_keyword(statement, pos, "COLUMN") {
        pos = skip_ascii_whitespace(statement, pos + "COLUMN".len());
    }
    convert_mysql_add_column_statement(statement, pos, target_schema, table_name.as_str())
}

fn convert_mysql_temporary_table_statements(value: &str, target_schema: &str) -> String {
    let value = convert_mysql_drop_temporary_table_statements(value, target_schema);
    convert_mysql_create_temporary_table_statements(value.as_str(), target_schema)
}

fn convert_mysql_view_statements(value: &str, target_schema: &str) -> String {
    let value = convert_mysql_drop_view_statements(value, target_schema);
    convert_mysql_create_view_statements(value.as_str(), target_schema)
}

fn convert_mysql_drop_temporary_table_statements(value: &str, target_schema: &str) -> String {
    convert_mysql_ddl_statements_by_keyword(value, "DROP", target_schema, |statement, schema| {
        convert_mysql_drop_temporary_table_statement(statement, schema)
    })
}

fn convert_mysql_create_temporary_table_statements(value: &str, target_schema: &str) -> String {
    convert_mysql_ddl_statements_by_keyword(value, "CREATE", target_schema, |statement, schema| {
        convert_mysql_create_temporary_table_statement(statement, schema)
    })
}

fn convert_mysql_drop_view_statements(value: &str, target_schema: &str) -> String {
    convert_mysql_ddl_statements_by_keyword(value, "DROP", target_schema, |statement, schema| {
        convert_mysql_drop_view_statement(statement, schema)
    })
}

fn convert_mysql_create_view_statements(value: &str, target_schema: &str) -> String {
    convert_mysql_ddl_statements_by_keyword(value, "CREATE", target_schema, |statement, schema| {
        convert_mysql_create_view_statement(statement, schema)
    })
}

fn convert_mysql_ddl_statements_by_keyword<F>(
    value: &str,
    keyword: &str,
    target_schema: &str,
    convert: F,
) -> String
where
    F: Fn(&str, &str) -> Option<String>,
{
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(keyword_pos) = find_keyword_outside_quotes(value, search_pos, keyword) {
        let statement_end = find_statement_semicolon_outside_quotes(value, keyword_pos)
            .map(|pos| pos + 1)
            .unwrap_or(value.len());
        let statement = &value[keyword_pos..statement_end];
        let Some(sql) = convert(statement, target_schema) else {
            search_pos = keyword_pos + keyword.len();
            continue;
        };
        result.push_str(&value[copied_pos..keyword_pos]);
        let indent = current_line_indent(value, keyword_pos);
        result.push_str(indent_converted_statement(sql.as_str(), indent.as_str()).as_str());
        copied_pos = statement_end;
        search_pos = statement_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn convert_mysql_drop_temporary_table_statement(
    statement: &str,
    target_schema: &str,
) -> Option<String> {
    if !starts_with_keyword(statement, 0, "DROP") {
        return None;
    }
    let mut pos = skip_ascii_whitespace(statement, "DROP".len());
    if starts_with_keyword(statement, pos, "TEMPORARY") {
        pos = skip_ascii_whitespace(statement, pos + "TEMPORARY".len());
    }
    if !starts_with_keyword(statement, pos, "TABLE") {
        return None;
    }
    pos = skip_ascii_whitespace(statement, pos + "TABLE".len());
    if starts_with_keyword(statement, pos, "IF") {
        pos = skip_ascii_whitespace(statement, pos + "IF".len());
        if !starts_with_keyword(statement, pos, "EXISTS") {
            return None;
        }
        pos = skip_ascii_whitespace(statement, pos + "EXISTS".len());
    }
    let table_list = trim_statement_semicolon(&statement[pos..]);
    let statements = split_top_level_commas(table_list)
        .into_iter()
        .filter_map(|table| take_sql_identifier(table.trim(), 0).map(|(table_name, _)| table_name))
        .map(|table_name| {
            execute_immediate_sql(
                format!(
                    "DROP TABLE {}",
                    qualified_table(target_schema, table_name.as_str())
                )
                .as_str(),
            )
        })
        .collect::<Vec<_>>();
    if statements.is_empty() {
        None
    } else {
        Some(statements.join("\n"))
    }
}

fn convert_mysql_drop_view_statement(statement: &str, target_schema: &str) -> Option<String> {
    if !starts_with_keyword(statement, 0, "DROP") {
        return None;
    }
    let mut pos = skip_ascii_whitespace(statement, "DROP".len());
    if !starts_with_keyword(statement, pos, "VIEW") {
        return None;
    }
    pos = skip_ascii_whitespace(statement, pos + "VIEW".len());
    if starts_with_keyword(statement, pos, "IF") {
        pos = skip_ascii_whitespace(statement, pos + "IF".len());
        if !starts_with_keyword(statement, pos, "EXISTS") {
            return None;
        }
        pos = skip_ascii_whitespace(statement, pos + "EXISTS".len());
    }
    let view_list = trim_statement_semicolon(&statement[pos..]);
    let statements = split_top_level_commas(view_list)
        .into_iter()
        .filter_map(|view| {
            take_sql_table_identifier(view.trim(), 0).map(|(view_name, _)| view_name)
        })
        .map(|view_name| {
            execute_immediate_sql(
                format!(
                    "DROP VIEW {}",
                    qualified_table(target_schema, view_name.as_str())
                )
                .as_str(),
            )
        })
        .collect::<Vec<_>>();
    if statements.is_empty() {
        None
    } else {
        Some(statements.join("\n"))
    }
}

fn convert_mysql_create_view_statement(statement: &str, target_schema: &str) -> Option<String> {
    if !starts_with_keyword(statement, 0, "CREATE") {
        return None;
    }
    let mut pos = skip_ascii_whitespace(statement, "CREATE".len());
    let mut or_replace = false;
    if starts_with_keyword(statement, pos, "OR") {
        let replace_pos = skip_ascii_whitespace(statement, pos + "OR".len());
        if !starts_with_keyword(statement, replace_pos, "REPLACE") {
            return None;
        }
        or_replace = true;
        pos = skip_ascii_whitespace(statement, replace_pos + "REPLACE".len());
    }
    if !starts_with_keyword(statement, pos, "VIEW") {
        return None;
    }
    pos = skip_ascii_whitespace(statement, pos + "VIEW".len());
    let (view_name, next_pos) = take_sql_table_identifier(statement, pos)?;
    let as_pos = find_keyword_outside_quotes(statement, next_pos, "AS")?;
    let select_sql = trim_statement_semicolon(&statement[as_pos + "AS".len()..]);
    if !starts_with_keyword(select_sql, 0, "SELECT") {
        return None;
    }
    let create_kind = if or_replace {
        "CREATE OR REPLACE VIEW"
    } else {
        "CREATE VIEW"
    };
    let sql = format!(
        "{} {} AS {}",
        create_kind,
        qualified_table(target_schema, view_name.as_str()),
        select_sql
    );
    Some(execute_immediate_sql(sql.as_str()))
}

fn convert_mysql_create_temporary_table_statement(
    statement: &str,
    target_schema: &str,
) -> Option<String> {
    if !starts_with_keyword(statement, 0, "CREATE") {
        return None;
    }
    let mut pos = skip_ascii_whitespace(statement, "CREATE".len());
    let mut temporary = false;
    if starts_with_keyword(statement, pos, "TEMPORARY") {
        temporary = true;
        pos = skip_ascii_whitespace(statement, pos + "TEMPORARY".len());
    }
    if !starts_with_keyword(statement, pos, "TABLE") {
        return None;
    }
    pos = skip_ascii_whitespace(statement, pos + "TABLE".len());
    if starts_with_keyword(statement, pos, "IF") {
        pos = skip_ascii_whitespace(statement, pos + "IF".len());
        if !starts_with_keyword(statement, pos, "NOT") {
            return None;
        }
        pos = skip_ascii_whitespace(statement, pos + "NOT".len());
        if !starts_with_keyword(statement, pos, "EXISTS") {
            return None;
        }
        pos = skip_ascii_whitespace(statement, pos + "EXISTS".len());
    }
    let (table_name, next_pos) = take_sql_table_identifier(statement, pos)?;
    let open_pos = skip_ascii_whitespace(statement, next_pos);
    let table_kind = if temporary || mysql_name_looks_temporary(table_name.as_str()) {
        "CREATE GLOBAL TEMPORARY TABLE"
    } else {
        "CREATE TABLE"
    };
    if starts_with_keyword(statement, open_pos, "AS") {
        let select_pos = skip_ascii_whitespace(statement, open_pos + "AS".len());
        let select_sql = mysql_create_table_as_select_sql(statement, select_pos)?;
        let sql = format!(
            "{} {} AS {}",
            table_kind,
            qualified_table(target_schema, table_name.as_str()),
            select_sql
        );
        return Some(execute_immediate_sql(sql.as_str()));
    }
    if starts_with_keyword(statement, open_pos, "SELECT") {
        let select_sql = trim_statement_semicolon(&statement[open_pos..]);
        let sql = format!(
            "{} {} AS {}",
            table_kind,
            qualified_table(target_schema, table_name.as_str()),
            select_sql
        );
        return Some(execute_immediate_sql(sql.as_str()));
    }
    if statement.as_bytes().get(open_pos) != Some(&b'(') {
        return None;
    }
    let close_pos = find_matching_paren(statement, open_pos)?;
    let columns = split_top_level_commas(&statement[open_pos + 1..close_pos])
        .into_iter()
        .filter_map(convert_mysql_temporary_table_column)
        .collect::<Vec<_>>()
        .join(", ");
    if columns.is_empty() {
        return None;
    }
    let commit_clause = if table_kind == "CREATE GLOBAL TEMPORARY TABLE" {
        " ON COMMIT PRESERVE ROWS"
    } else {
        ""
    };
    let sql = format!(
        "{} {} ({}){}",
        table_kind,
        qualified_table(target_schema, table_name.as_str()),
        columns,
        commit_clause
    );
    Some(execute_immediate_sql(sql.as_str()))
}

fn mysql_name_looks_temporary(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("tmp") || lower.starts_with("temp")
}

fn mysql_create_table_as_select_sql(statement: &str, select_pos: usize) -> Option<&str> {
    let bytes = statement.as_bytes();
    if bytes.get(select_pos) == Some(&b'(') {
        let close_pos = find_matching_paren(statement, select_pos)?;
        let after_close = trim_statement_semicolon(&statement[close_pos + 1..]);
        if after_close.is_empty() {
            let inner = statement[select_pos + 1..close_pos].trim();
            if starts_with_keyword(inner, 0, "SELECT") {
                return Some(inner);
            }
        }
    }
    let select_sql = trim_statement_semicolon(&statement[select_pos..]);
    if starts_with_keyword(select_sql, 0, "SELECT") {
        Some(select_sql)
    } else {
        None
    }
}

fn convert_mysql_temporary_table_column(column: &str) -> Option<String> {
    let column = column.trim();
    if column.is_empty() {
        return None;
    }
    if mysql_table_constraint_definition(column) {
        return None;
    }
    let (column_name, next_pos) = take_sql_identifier(column, 0)?;
    let rest = trim_statement_semicolon(&column[next_pos..]);
    let (definition, _) = split_mysql_column_comment(rest);
    let definition = convert_mysql_column_definition_fragment(definition.trim())?;
    Some(format!(
        "{} {}",
        quote_ident(column_name.as_str()),
        definition
    ))
}

fn mysql_table_constraint_definition(column: &str) -> bool {
    starts_with_keyword(column, 0, "PRIMARY")
        || starts_with_keyword(column, 0, "KEY")
        || starts_with_keyword(column, 0, "INDEX")
        || starts_with_keyword(column, 0, "UNIQUE")
        || starts_with_keyword(column, 0, "CONSTRAINT")
        || starts_with_keyword(column, 0, "FOREIGN")
}

fn convert_mysql_single_quoted_aliases(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(as_pos) = find_keyword_outside_quotes(value, search_pos, "AS") {
        let alias_pos = skip_ascii_whitespace(value, as_pos + "AS".len());
        if value.as_bytes().get(alias_pos) == Some(&b'\'') {
            let Some((alias, alias_end)) = take_mysql_single_quoted_string(value, alias_pos) else {
                search_pos = as_pos + "AS".len();
                continue;
            };
            result.push_str(&value[copied_pos..as_pos]);
            result.push_str("AS ");
            result.push_str(quote_ident(alias.as_str()).as_str());
            copied_pos = alias_end;
            search_pos = alias_end;
        } else {
            let Some((alias, alias_end)) = take_unquoted_alias_token(value, alias_pos) else {
                search_pos = as_pos + "AS".len();
                continue;
            };
            let after_alias = skip_ascii_whitespace(value, alias_end);
            if value.as_bytes().get(after_alias) == Some(&b'(')
                || is_safe_unquoted_identifier(alias)
            {
                search_pos = alias_end;
                continue;
            }
            result.push_str(&value[copied_pos..as_pos]);
            result.push_str("AS ");
            result.push_str(quote_ident(alias).as_str());
            copied_pos = alias_end;
            search_pos = alias_end;
        }
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn take_unquoted_alias_token(value: &str, start: usize) -> Option<(&str, usize)> {
    let mut pos = start;
    while pos < value.len() {
        let ch = value[pos..].chars().next()?;
        if ch.is_whitespace() || matches!(ch, '(' | ',' | ';' | ')') {
            break;
        }
        pos += ch.len_utf8();
    }
    if pos == start {
        None
    } else {
        Some((&value[start..pos], pos))
    }
}

fn take_mysql_single_quoted_string(value: &str, start: usize) -> Option<(String, usize)> {
    if value.as_bytes().get(start) != Some(&b'\'') {
        return None;
    }
    let bytes = value.as_bytes();
    let mut pos = start + 1;
    let mut result = String::new();
    while pos < bytes.len() {
        let ch = value[pos..].chars().next()?;
        if ch == '\\' {
            pos += ch.len_utf8();
            let escaped = value[pos..].chars().next()?;
            result.push(escaped);
            pos += escaped.len_utf8();
            continue;
        }
        if ch == '\'' {
            let next_pos = pos + ch.len_utf8();
            if bytes.get(next_pos) == Some(&b'\'') {
                result.push(ch);
                pos = next_pos + 1;
            } else {
                return Some((result, next_pos));
            }
            continue;
        }
        result.push(ch);
        pos += ch.len_utf8();
    }
    None
}

fn convert_mysql_add_column_statement(
    statement: &str,
    column_pos: usize,
    target_schema: &str,
    table_name: &str,
) -> Option<String> {
    let (column_name, next_pos) = take_sql_identifier(statement, column_pos)?;
    let rest = trim_statement_semicolon(&statement[next_pos..]);
    let (definition, comment_literal) = split_mysql_column_comment(rest);
    let definition = convert_mysql_column_definition_fragment(definition.trim())?;
    let alter_sql = format!(
        "ALTER TABLE {} ADD {} {}",
        qualified_table(target_schema, table_name),
        quote_ident(column_name.as_str()),
        definition
    );
    let mut result = vec![execute_immediate_sql(alter_sql.as_str())];
    if let Some(comment_literal) = comment_literal {
        result.push(execute_immediate_sql(
            format!(
                "COMMENT ON COLUMN {}.{} IS {}",
                qualified_table(target_schema, table_name),
                quote_ident(column_name.as_str()),
                comment_literal
            )
            .as_str(),
        ));
    }
    Some(result.join("\n"))
}

fn convert_mysql_modify_column_statement(
    statement: &str,
    column_pos: usize,
    target_schema: &str,
    table_name: &str,
) -> Option<String> {
    let (column_name, next_pos) = take_sql_identifier(statement, column_pos)?;
    let rest = trim_statement_semicolon(&statement[next_pos..]);
    let (definition, comment_literal) = split_mysql_column_comment(rest);
    let definition = convert_mysql_column_definition_fragment(definition.trim())?;
    let modify_sql = format!(
        "ALTER TABLE {} MODIFY {} {}",
        qualified_table(target_schema, table_name),
        quote_ident(column_name.as_str()),
        definition
    );
    let mut result = vec![execute_immediate_sql(modify_sql.as_str())];
    if let Some(comment_literal) = comment_literal {
        result.push(execute_immediate_sql(
            format!(
                "COMMENT ON COLUMN {}.{} IS {}",
                qualified_table(target_schema, table_name),
                quote_ident(column_name.as_str()),
                comment_literal
            )
            .as_str(),
        ));
    }
    Some(result.join("\n"))
}

fn convert_mysql_add_index_statement(
    statement: &str,
    index_pos: usize,
    target_schema: &str,
    table_name: &str,
    unique: bool,
) -> Option<String> {
    let index_pos = skip_ascii_whitespace(statement, index_pos);
    let (index_name, next_pos) = take_sql_identifier(statement, index_pos)?;
    let open_pos = skip_ascii_whitespace(statement, next_pos);
    if statement.as_bytes().get(open_pos) != Some(&b'(') {
        return None;
    }
    let close_pos = find_matching_paren(statement, open_pos)?;
    let columns = split_top_level_commas(&statement[open_pos + 1..close_pos])
        .into_iter()
        .map(|column| quote_ident(identifier_name_from_token(column.trim()).as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let index_kind = if unique {
        "CREATE UNIQUE INDEX"
    } else {
        "CREATE INDEX"
    };
    let sql = format!(
        "{} {} ON {} ({})",
        index_kind,
        quote_ident(index_name.as_str()),
        qualified_table(target_schema, table_name),
        columns
    );
    Some(execute_immediate_sql(sql.as_str()))
}

fn convert_mysql_column_definition_fragment(definition: &str) -> Option<String> {
    let (mysql_type, rest_pos) = take_mysql_type_token(definition)?;
    let rest = clean_mysql_column_definition_rest(definition[rest_pos..].trim_start());
    let dameng_type = map_mysql_type_to_dameng(mysql_type);
    if rest.is_empty() {
        Some(dameng_type)
    } else {
        Some(format!("{} {}", dameng_type, rest))
    }
}

fn clean_mysql_column_definition_rest(rest: &str) -> String {
    let auto_increment = find_keyword_outside_quotes(rest, 0, "AUTO_INCREMENT").is_some();
    let mut cleaned = replace_keyword_with_space_outside_quotes(rest, "AUTO_INCREMENT");
    if auto_increment {
        cleaned = replace_keyword_with_space_outside_quotes(cleaned.as_str(), "NOT");
        cleaned = replace_keyword_with_space_outside_quotes(cleaned.as_str(), "NULL");
    }
    cleaned.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn replace_keyword_with_space_outside_quotes(value: &str, keyword: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(keyword_pos) = find_keyword_outside_quotes(value, search_pos, keyword) {
        result.push_str(&value[copied_pos..keyword_pos]);
        result.push(' ');
        copied_pos = keyword_pos + keyword.len();
        search_pos = copied_pos;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn take_mysql_type_token(value: &str) -> Option<(&str, usize)> {
    let bytes = value.as_bytes();
    let mut pos = 0usize;
    while pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
        pos += 1;
    }
    if pos == 0 {
        return None;
    }
    let mut end = pos;
    pos = skip_ascii_whitespace(value, pos);
    if bytes.get(pos) == Some(&b'(') {
        let close_pos = find_matching_paren(value, pos)?;
        end = close_pos + 1;
        pos = skip_ascii_whitespace(value, end);
    }
    if starts_with_keyword(value, pos, "UNSIGNED") {
        end = pos + "UNSIGNED".len();
    }
    Some((value[..end].trim(), end))
}

fn split_mysql_column_comment(definition: &str) -> (&str, Option<&str>) {
    let Some(comment_pos) = find_keyword_outside_quotes(definition, 0, "COMMENT") else {
        return (definition, None);
    };
    let before = definition[..comment_pos].trim_end();
    let comment_literal = definition[comment_pos + "COMMENT".len()..].trim();
    if comment_literal.is_empty() {
        (before, None)
    } else {
        (before, Some(comment_literal))
    }
}

fn trim_statement_semicolon(value: &str) -> &str {
    value
        .trim()
        .strip_suffix(';')
        .map(str::trim_end)
        .unwrap_or_else(|| value.trim())
}

fn execute_immediate_sql(sql: &str) -> String {
    format!("EXECUTE IMMEDIATE {};", quote_literal(sql))
}

fn qualified_table(schema: &str, table_name: &str) -> String {
    if schema.is_empty() {
        quote_ident(table_name)
    } else {
        format!("{}.{}", quote_ident(schema), quote_ident(table_name))
    }
}

fn take_sql_table_identifier(value: &str, start: usize) -> Option<(String, usize)> {
    let (first, first_end) = take_sql_identifier(value, start)?;
    let dot_pos = skip_ascii_whitespace(value, first_end);
    if value.as_bytes().get(dot_pos) != Some(&b'.') {
        return Some((first, first_end));
    }
    let second_start = skip_ascii_whitespace(value, dot_pos + 1);
    let (second, second_end) = take_sql_identifier(value, second_start)?;
    Some((second, second_end))
}

fn take_sql_identifier(value: &str, start: usize) -> Option<(String, usize)> {
    let bytes = value.as_bytes();
    let first = *bytes.get(start)?;
    if matches!(first, b'"' | b'`') {
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
        if byte.is_ascii_whitespace() || matches!(byte, b'.' | b'(' | b')' | b',' | b';') {
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

fn identifier_name_from_token(token: &str) -> String {
    if let Some((name, next_pos)) = take_sql_identifier(token, 0) {
        if token[next_pos..].trim().is_empty() {
            return name;
        }
    }
    token.to_string()
}

fn replace_case_insensitive_outside_quotes(
    value: &str,
    pattern: &str,
    replacement: &str,
) -> String {
    let bytes = value.as_bytes();
    let pattern_bytes = pattern.as_bytes();
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

        if pos + pattern_bytes.len() <= bytes.len()
            && bytes[pos..pos + pattern_bytes.len()].eq_ignore_ascii_case(pattern_bytes)
        {
            result.push_str(replacement);
            pos += pattern_bytes.len();
            continue;
        }

        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn replace_mysql_database_schema_checks(value: &str, target_schema: &str) -> String {
    let bytes = value.as_bytes();
    let mut result = String::with_capacity(value.len());
    let mut pos = 0usize;
    while pos < bytes.len() {
        if starts_with_keyword(value, pos, "table_schema") {
            let mut cursor = skip_ascii_whitespace(value, pos + "table_schema".len());
            if bytes.get(cursor) == Some(&b'=') {
                cursor = skip_ascii_whitespace(value, cursor + 1);
                let had_open = bytes.get(cursor) == Some(&b'(');
                if had_open {
                    cursor = skip_ascii_whitespace(value, cursor + 1);
                }
                if starts_with_keyword(value, cursor, "select") {
                    cursor = skip_ascii_whitespace(value, cursor + "select".len());
                    if starts_with_keyword(value, cursor, "database") {
                        cursor = skip_ascii_whitespace(value, cursor + "database".len());
                        if bytes.get(cursor) == Some(&b'(') {
                            let close = skip_ascii_whitespace(value, cursor + 1);
                            if bytes.get(close) == Some(&b')') {
                                cursor = skip_ascii_whitespace(value, close + 1);
                                if had_open && bytes.get(cursor) == Some(&b')') {
                                    cursor += 1;
                                }
                                result.push_str(
                                    format!(
                                        "UPPER(OWNER) = UPPER({})",
                                        quote_literal(target_schema)
                                    )
                                    .as_str(),
                                );
                                pos = cursor;
                                continue;
                            }
                        }
                    }
                }
            }
        }
        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn replace_metadata_column_predicates(
    value: &str,
    mysql_column: &str,
    dameng_column: &str,
) -> String {
    let bytes = value.as_bytes();
    let mut result = String::with_capacity(value.len());
    let mut pos = 0usize;
    while pos < bytes.len() {
        if starts_with_keyword(value, pos, mysql_column) {
            let mut cursor = skip_ascii_whitespace(value, pos + mysql_column.len());
            if bytes.get(cursor) == Some(&b'=') {
                cursor = skip_ascii_whitespace(value, cursor + 1);
                if let Some(literal_end) = find_sql_literal_end(value, cursor) {
                    result.push_str(
                        format!(
                            "UPPER({}) = UPPER({})",
                            dameng_column,
                            &value[cursor..literal_end]
                        )
                        .as_str(),
                    );
                    pos = literal_end;
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

fn find_sql_literal_end(value: &str, start: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let quote = *bytes.get(start)?;
    if !matches!(quote, b'\'' | b'"') {
        return None;
    }
    let mut pos = start + 1;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if byte == b'\\' && quote == b'\'' {
            pos = (pos + 2).min(bytes.len());
            continue;
        }
        if byte == quote {
            if bytes.get(pos + 1) == Some(&quote) {
                pos += 2;
            } else {
                return Some(pos + 1);
            }
        } else {
            pos += 1;
        }
    }
    None
}

fn mysql_user_variables(value: &str) -> Vec<String> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut result = Vec::new();
    let mut seen = Vec::new();
    let mut pos = 0usize;
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
        if matches!(byte, b'\'' | b'"' | b'`') {
            quote = Some(byte);
            pos += 1;
            continue;
        }
        if byte == b'@' {
            let start = pos + 1;
            let mut end = start;
            while end < bytes.len() && is_ascii_identifier_byte(bytes[end]) {
                end += 1;
            }
            if end > start {
                let name = value[start..end].to_string();
                let key = name.to_ascii_lowercase();
                if !seen.contains(&key) {
                    seen.push(key);
                    result.push(name);
                }
                pos = end;
                continue;
            }
        }
        pos += 1;
    }
    result
}

fn mysql_user_variable_declarations(variables: &[String]) -> Vec<String> {
    variables
        .iter()
        .map(|name| format!("    {} VARCHAR(255);", dameng_routine_param_name(name)))
        .collect()
}

fn convert_mysql_user_variables(value: &str) -> String {
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
        if byte == b'@' {
            let start = pos + 1;
            let mut end = start;
            while end < bytes.len() && is_ascii_identifier_byte(bytes[end]) {
                end += 1;
            }
            if end > start {
                result.push_str(dameng_routine_param_name(&value[start..end]).as_str());
                pos = end;
                continue;
            }
        }
        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn convert_mysql_limit_clauses(value: &str) -> String {
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
        if starts_with_keyword(value, pos, "LIMIT") {
            let amount_start = skip_ascii_whitespace(value, pos + "LIMIT".len());
            let mut amount_end = amount_start;
            while amount_end < bytes.len() && bytes[amount_end].is_ascii_digit() {
                amount_end += 1;
            }
            if amount_end > amount_start {
                result.push_str(
                    format!("FETCH FIRST {} ROWS ONLY", &value[amount_start..amount_end]).as_str(),
                );
                pos = amount_end;
                continue;
            }
        }
        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn convert_mysql_insert_value_keyword(value: &str) -> String {
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
        if starts_with_keyword(value, pos, "VALUE") {
            let after_value = skip_mysql_whitespace_and_comments(value, pos + "VALUE".len());
            if bytes.get(after_value) == Some(&b'(') {
                result.push_str("VALUES");
                pos += "VALUE".len();
                continue;
            }
        }
        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn convert_mysql_update_order_limit_clauses(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(update_pos) = find_keyword_outside_quotes(value, search_pos, "UPDATE") {
        let statement_end = find_statement_semicolon_outside_quotes(value, update_pos)
            .map(|pos| pos + 1)
            .unwrap_or(value.len());
        let statement = &value[update_pos..statement_end];
        let converted = convert_mysql_update_order_limit_statement(statement);
        if converted == statement {
            search_pos = update_pos + "UPDATE".len();
            continue;
        }
        result.push_str(&value[copied_pos..update_pos]);
        result.push_str(converted.as_str());
        copied_pos = statement_end;
        search_pos = statement_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn convert_mysql_update_order_limit_statement(statement: &str) -> String {
    let trimmed = statement.trim_start();
    if !starts_with_keyword(trimmed, 0, "UPDATE") {
        return statement.to_string();
    }
    let Some(order_pos) = find_top_level_keyword_outside_quotes(statement, 0, "ORDER") else {
        return statement.to_string();
    };
    let by_pos = skip_ascii_whitespace(statement, order_pos + "ORDER".len());
    if !starts_with_keyword(statement, by_pos, "BY") {
        return statement.to_string();
    }
    let trimmed_end = statement.trim_end();
    let has_semicolon = trimmed_end.ends_with(';');
    let mut converted = statement[..order_pos].trim_end().to_string();
    if has_semicolon {
        converted.push(';');
    }
    converted
}

#[derive(Debug)]
struct MysqlUpdateJoinParts {
    target_table: String,
    target_alias: String,
    source_table: String,
    source_alias: String,
    on_clause: String,
    set_clause: String,
    where_clause: Option<String>,
}

fn convert_mysql_update_join_statements(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(update_pos) = find_keyword_outside_quotes(value, search_pos, "UPDATE") {
        let statement_end = find_statement_semicolon_outside_quotes(value, update_pos)
            .map(|pos| pos + 1)
            .unwrap_or(value.len());
        let statement = &value[update_pos..statement_end];
        let Some(converted) = convert_mysql_update_join_statement(statement) else {
            search_pos = update_pos + "UPDATE".len();
            continue;
        };
        result.push_str(&value[copied_pos..update_pos]);
        result.push_str(converted.as_str());
        copied_pos = statement_end;
        search_pos = statement_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn convert_mysql_update_join_statement(statement: &str) -> Option<String> {
    let trimmed = trim_statement_semicolon(statement.trim());
    if !starts_with_keyword(trimmed, 0, "UPDATE") {
        return None;
    }
    let parts = parse_mysql_update_join_statement(trimmed)?;
    mysql_update_join_to_correlated_update(&parts)
}

fn parse_mysql_update_join_statement(statement: &str) -> Option<MysqlUpdateJoinParts> {
    let after_update = skip_ascii_whitespace(statement, "UPDATE".len());
    let join_pos = find_top_level_keyword_outside_quotes(statement, after_update, "JOIN")?;
    let target_ref = statement[after_update..join_pos].trim();
    let (target_table, target_alias) = parse_mysql_update_join_table_ref(target_ref)?;

    let after_join = skip_ascii_whitespace(statement, join_pos + "JOIN".len());
    let on_pos = find_top_level_keyword_outside_quotes(statement, after_join, "ON")?;
    let source_ref = statement[after_join..on_pos].trim();
    let (source_table, source_alias) = parse_mysql_update_join_table_ref(source_ref)?;

    let after_on = skip_ascii_whitespace(statement, on_pos + "ON".len());
    let set_pos = find_top_level_keyword_outside_quotes(statement, after_on, "SET")?;
    let on_clause = statement[after_on..set_pos].trim();
    if on_clause.is_empty() {
        return None;
    }

    let after_set = skip_ascii_whitespace(statement, set_pos + "SET".len());
    let where_pos = find_top_level_keyword_outside_quotes(statement, after_set, "WHERE");
    let (set_clause, where_clause) = if let Some(where_pos) = where_pos {
        (
            statement[after_set..where_pos].trim(),
            Some(statement[skip_ascii_whitespace(statement, where_pos + "WHERE".len())..].trim()),
        )
    } else {
        (statement[after_set..].trim(), None)
    };
    if set_clause.is_empty() {
        return None;
    }
    let where_clause = where_clause
        .filter(|where_clause| !where_clause.is_empty())
        .map(str::to_string);

    Some(MysqlUpdateJoinParts {
        target_table: target_table.to_string(),
        target_alias,
        source_table: source_table.to_string(),
        source_alias,
        on_clause: on_clause.to_string(),
        set_clause: set_clause.to_string(),
        where_clause,
    })
}

fn parse_mysql_update_join_table_ref(value: &str) -> Option<(&str, String)> {
    let value = value.trim();
    let (table_end, _) = take_sql_table_reference(value, 0)?;
    let mut alias_pos = skip_ascii_whitespace(value, table_end);
    if starts_with_keyword(value, alias_pos, "AS") {
        alias_pos = skip_ascii_whitespace(value, alias_pos + "AS".len());
    }
    let (alias, alias_end) = take_sql_identifier(value, alias_pos)?;
    if !value[alias_end..].trim().is_empty() {
        return None;
    }
    Some((value[..table_end].trim(), alias))
}

fn take_sql_table_reference(value: &str, start: usize) -> Option<(usize, usize)> {
    let (_, mut end) = take_sql_identifier(value, start)?;
    loop {
        let dot_pos = skip_ascii_whitespace(value, end);
        if value.as_bytes().get(dot_pos) != Some(&b'.') {
            break;
        }
        let next_start = skip_ascii_whitespace(value, dot_pos + 1);
        let (_, next_end) = take_sql_identifier(value, next_start)?;
        end = next_end;
    }
    Some((end, end))
}

fn mysql_update_join_to_correlated_update(parts: &MysqlUpdateJoinParts) -> Option<String> {
    let target_alias = "dm_target";
    let source_alias = "dm_source";
    let on_clause = replace_mysql_update_join_aliases(
        parts.on_clause.as_str(),
        parts.target_alias.as_str(),
        target_alias,
        parts.source_alias.as_str(),
        source_alias,
    );
    let on_clause = convert_mysql_null_safe_equals(on_clause.as_str());
    let where_clause = parts.where_clause.as_ref().map(|where_clause| {
        let where_clause = replace_mysql_update_join_aliases(
            where_clause.as_str(),
            parts.target_alias.as_str(),
            target_alias,
            parts.source_alias.as_str(),
            source_alias,
        );
        convert_mysql_null_safe_equals(where_clause.as_str())
    });
    let combined_condition = if let Some(where_clause) = where_clause {
        format!("{}\n    AND {}", on_clause.trim(), where_clause.trim())
    } else {
        on_clause.trim().to_string()
    };
    if combined_condition.is_empty() {
        return None;
    }

    let mut assignments = Vec::new();
    for assignment in split_top_level_commas(parts.set_clause.as_str()) {
        let assignment = assignment.trim();
        if assignment.is_empty() {
            continue;
        }
        let eq_pos = find_assignment_equal_outside_quotes(assignment)?;
        let lhs = assignment[..eq_pos].trim();
        let rhs = assignment[eq_pos + 1..].trim();
        if lhs.is_empty() || rhs.is_empty() {
            return None;
        }
        let lhs = replace_mysql_update_join_aliases(
            lhs,
            parts.target_alias.as_str(),
            target_alias,
            parts.source_alias.as_str(),
            source_alias,
        );
        let rhs = replace_mysql_update_join_aliases(
            rhs,
            parts.target_alias.as_str(),
            target_alias,
            parts.source_alias.as_str(),
            source_alias,
        );
        let rhs = convert_mysql_null_safe_equals(rhs.as_str());
        let lhs = strip_sql_qualified_alias(lhs.trim(), target_alias)?;
        assignments.push(format!(
            "    {} = (\n        SELECT {}\n        FROM {} {}\n        WHERE {}\n    )",
            lhs,
            indent_continued_condition(rhs.trim(), 15),
            parts.source_table,
            source_alias,
            indent_continued_condition(combined_condition.as_str(), 14)
        ));
    }
    if assignments.is_empty() {
        return None;
    }

    Some(format!(
        "UPDATE {} {}\nSET\n{}\nWHERE EXISTS (\n    SELECT 1\n    FROM {} {}\n    WHERE {}\n);",
        parts.target_table,
        target_alias,
        assignments.join(",\n"),
        parts.source_table,
        source_alias,
        indent_continued_condition(combined_condition.as_str(), 10)
    ))
}

fn replace_mysql_update_join_aliases(
    value: &str,
    target_alias: &str,
    replacement_target_alias: &str,
    source_alias: &str,
    replacement_source_alias: &str,
) -> String {
    let value = replace_sql_qualified_alias(value, target_alias, replacement_target_alias);
    replace_sql_qualified_alias(value.as_str(), source_alias, replacement_source_alias)
}

fn replace_sql_qualified_alias(value: &str, alias: &str, replacement: &str) -> String {
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
        if let Some((identifier, next_pos)) = take_sql_identifier(value, pos) {
            let dot_pos = skip_ascii_whitespace(value, next_pos);
            if identifier.eq_ignore_ascii_case(alias)
                && value.as_bytes().get(dot_pos) == Some(&b'.')
            {
                result.push_str(replacement);
                result.push_str(&value[next_pos..dot_pos]);
                pos = dot_pos;
                continue;
            }
        }
        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn strip_sql_qualified_alias<'a>(value: &'a str, alias: &str) -> Option<&'a str> {
    let (identifier, next_pos) = take_sql_identifier(value, 0)?;
    if !identifier.eq_ignore_ascii_case(alias) {
        return Some(value);
    }
    let dot_pos = skip_ascii_whitespace(value, next_pos);
    if value.as_bytes().get(dot_pos) != Some(&b'.') {
        return Some(value);
    }
    let column_start = skip_ascii_whitespace(value, dot_pos + 1);
    let (_column, column_end) = take_sql_identifier(value, column_start)?;
    if !value[column_end..].trim().is_empty() {
        return None;
    }
    Some(value[column_start..].trim())
}

fn find_assignment_equal_outside_quotes(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut paren_depth = 0u32;
    let mut pos = 0usize;
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
            b'(' => {
                paren_depth += 1;
                pos += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                pos += 1;
            }
            b'=' if paren_depth == 0 => {
                if matches!(
                    bytes.get(pos.wrapping_sub(1)),
                    Some(b'<') | Some(b'>') | Some(b'!')
                ) || bytes.get(pos + 1) == Some(&b'>')
                {
                    pos += 1;
                    continue;
                }
                return Some(pos);
            }
            _ => pos += 1,
        }
    }
    None
}

fn indent_continued_condition(value: &str, indent: usize) -> String {
    let continuation = format!("\n{}", " ".repeat(indent));
    value.trim().replace('\n', continuation.as_str())
}

fn convert_mysql_null_safe_equals(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(op_pos) = find_null_safe_equal_operator(value, search_pos) {
        let Some(lhs_start) = mysql_null_safe_lhs_start(value, op_pos) else {
            search_pos = op_pos + 3;
            continue;
        };
        let rhs_start = skip_ascii_whitespace(value, op_pos + 3);
        let Some(rhs_end) = mysql_null_safe_rhs_end(value, rhs_start) else {
            search_pos = op_pos + 3;
            continue;
        };
        let lhs = value[lhs_start..op_pos].trim();
        let rhs = value[rhs_start..rhs_end].trim();
        if lhs.is_empty() || rhs.is_empty() {
            search_pos = op_pos + 3;
            continue;
        }
        result.push_str(&value[copied_pos..lhs_start]);
        result.push_str(
            format!(
                "(({} = {}) OR ({} IS NULL AND {} IS NULL))",
                lhs, rhs, lhs, rhs
            )
            .as_str(),
        );
        copied_pos = rhs_end;
        search_pos = rhs_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn find_null_safe_equal_operator(value: &str, start: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut pos = start;
    while pos + 2 < bytes.len() {
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
            b'<' if bytes.get(pos + 1) == Some(&b'=') && bytes.get(pos + 2) == Some(&b'>') => {
                return Some(pos);
            }
            _ => pos += 1,
        }
    }
    None
}

fn mysql_null_safe_lhs_start(value: &str, op_pos: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut pos = 0usize;
    let mut boundary = 0usize;
    while pos < op_pos {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            if byte == b'\\' && quote_byte == b'\'' {
                pos = (pos + 2).min(op_pos);
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
            b'(' | b',' | b';' => {
                pos += 1;
                boundary = pos;
            }
            _ if null_safe_lhs_boundary_keyword(value, pos) => {
                let (_, next_pos) = take_ascii_word(value, pos)?;
                boundary = next_pos;
                pos = next_pos;
            }
            _ => pos += 1,
        }
    }
    Some(skip_ascii_whitespace(value, boundary))
}

fn null_safe_lhs_boundary_keyword(value: &str, pos: usize) -> bool {
    ["AND", "OR", "WHERE", "ON", "WHEN", "THEN", "ELSE", "SET"]
        .iter()
        .any(|keyword| starts_with_keyword(value, pos, keyword))
}

fn mysql_null_safe_rhs_end(value: &str, start: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    if start >= bytes.len() {
        return None;
    }
    let mut quote = None;
    let mut paren_depth = 0u32;
    let mut case_depth = 0u32;
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
            b'(' => {
                paren_depth += 1;
                pos += 1;
            }
            b')' if paren_depth == 0 && case_depth == 0 => break,
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                pos += 1;
            }
            b',' | b';' if paren_depth == 0 && case_depth == 0 => break,
            _ if paren_depth == 0 && starts_with_keyword(value, pos, "CASE") => {
                case_depth += 1;
                pos += "CASE".len();
            }
            _ if paren_depth == 0 && case_depth > 0 && starts_with_keyword(value, pos, "END") => {
                case_depth -= 1;
                pos += "END".len();
            }
            _ if paren_depth == 0
                && case_depth == 0
                && null_safe_rhs_boundary_keyword(value, pos) =>
            {
                break;
            }
            _ => pos += 1,
        }
    }
    let end = value[..pos].trim_end().len();
    if end <= start { None } else { Some(end) }
}

fn null_safe_rhs_boundary_keyword(value: &str, pos: usize) -> bool {
    ["AND", "OR", "WHERE", "ON", "WHEN", "THEN", "ELSE"]
        .iter()
        .any(|keyword| starts_with_keyword(value, pos, keyword))
}

fn convert_mysql_insert_ignore_keyword(value: &str) -> String {
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
        if byte == b'-' && bytes.get(pos + 1) == Some(&b'-') {
            let comment_end = skip_line_comment(value, pos + 2);
            result.push_str(&value[pos..comment_end]);
            pos = comment_end;
            continue;
        }
        if byte == b'#' {
            let comment_end = skip_line_comment(value, pos + 1);
            result.push_str(&value[pos..comment_end]);
            pos = comment_end;
            continue;
        }
        if byte == b'/' && bytes.get(pos + 1) == Some(&b'*') {
            let mut comment_end = pos + 2;
            while comment_end + 1 < bytes.len() {
                if bytes[comment_end] == b'*' && bytes[comment_end + 1] == b'/' {
                    comment_end += 2;
                    break;
                }
                comment_end += 1;
            }
            result.push_str(&value[pos..comment_end]);
            pos = comment_end;
            continue;
        }
        if starts_with_keyword(value, pos, "INSERT") {
            let ignore_pos = skip_mysql_whitespace_and_comments(value, pos + "INSERT".len());
            if starts_with_keyword(value, ignore_pos, "IGNORE") {
                let into_pos =
                    skip_mysql_whitespace_and_comments(value, ignore_pos + "IGNORE".len());
                if starts_with_keyword(value, into_pos, "INTO") {
                    result.push_str("INSERT ");
                    pos = into_pos;
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

fn skip_mysql_whitespace_and_comments(value: &str, start: usize) -> usize {
    let bytes = value.as_bytes();
    let mut pos = start;
    loop {
        pos = skip_ascii_whitespace(value, pos);
        if bytes.get(pos) == Some(&b'-') && bytes.get(pos + 1) == Some(&b'-') {
            pos = skip_line_comment(value, pos + 2);
            continue;
        }
        if bytes.get(pos) == Some(&b'#') {
            pos = skip_line_comment(value, pos + 1);
            continue;
        }
        if bytes.get(pos) == Some(&b'/') && bytes.get(pos + 1) == Some(&b'*') {
            let mut cursor = pos + 2;
            while cursor + 1 < bytes.len() {
                if bytes[cursor] == b'*' && bytes[cursor + 1] == b'/' {
                    pos = cursor + 2;
                    break;
                }
                cursor += 1;
            }
            if cursor + 1 >= bytes.len() {
                return bytes.len();
            }
            continue;
        }
        return pos;
    }
}

fn convert_mysql_delete_alias_statements(value: &str) -> String {
    value
        .lines()
        .map(convert_mysql_delete_alias_statement_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_mysql_delete_alias_statement_line(line: &str) -> String {
    let leading_len = line.len() - line.trim_start().len();
    let leading = &line[..leading_len];
    let trimmed = &line[leading_len..];
    if !starts_with_keyword(trimmed, 0, "DELETE") {
        return line.to_string();
    }
    let alias_pos = skip_ascii_whitespace(trimmed, "DELETE".len());
    if starts_with_keyword(trimmed, alias_pos, "FROM") {
        return line.to_string();
    }
    let Some((_alias, next_pos)) = take_sql_identifier(trimmed, alias_pos) else {
        return line.to_string();
    };
    let mut from_pos = skip_ascii_whitespace(trimmed, next_pos);
    if trimmed.as_bytes().get(from_pos) == Some(&b'.')
        && trimmed.as_bytes().get(from_pos + 1) == Some(&b'*')
    {
        from_pos = skip_ascii_whitespace(trimmed, from_pos + 2);
    }
    if !starts_with_keyword(trimmed, from_pos, "FROM") {
        return line.to_string();
    }
    format!(
        "{}DELETE FROM{}",
        leading,
        &trimmed[from_pos + "FROM".len()..]
    )
}

fn convert_mysql_sql_tokens(value: &str, sql_mode: &str) -> String {
    let value = convert_mysql_quotes_to_dameng(value, sql_mode);
    let value = convert_mysql_index_hints(value.as_str());
    let value = convert_mysql_charset_casts(value.as_str());
    let value = remove_mysql_character_set_clauses(value.as_str());
    let value = remove_mysql_collate_clauses(value.as_str());
    let value = convert_mysql_now_calls(value.as_str());
    let value = convert_mysql_date_add_sub_interval_calls(value.as_str());
    convert_mysql_date_format_interval_exprs(value.as_str())
}

fn convert_mysql_index_hints(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut result = String::with_capacity(value.len());
    let mut quote = None;
    let mut pos = 0usize;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            let ch = value[pos..].chars().next().unwrap();
            result.push(ch);
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

        if byte == b'-' && bytes.get(pos + 1) == Some(&b'-') {
            let comment_end = skip_line_comment(value, pos + 2);
            result.push_str(&value[pos..comment_end]);
            pos = comment_end;
            continue;
        }
        if byte == b'#' {
            let comment_end = skip_line_comment(value, pos + 1);
            result.push_str(&value[pos..comment_end]);
            pos = comment_end;
            continue;
        }
        if byte == b'/' && bytes.get(pos + 1) == Some(&b'*') {
            let mut comment_end = pos + 2;
            while comment_end + 1 < bytes.len() {
                if bytes[comment_end] == b'*' && bytes[comment_end + 1] == b'/' {
                    comment_end += 2;
                    break;
                }
                comment_end += 1;
            }
            result.push_str(&value[pos..comment_end]);
            pos = comment_end;
            continue;
        }

        if let Some(hint_end) = mysql_index_hint_end(value, pos) {
            pos = hint_end;
            continue;
        }

        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
}

fn mysql_index_hint_end(value: &str, start: usize) -> Option<usize> {
    let keyword_len = if starts_with_keyword(value, start, "USE") {
        "USE".len()
    } else if starts_with_keyword(value, start, "FORCE") {
        "FORCE".len()
    } else if starts_with_keyword(value, start, "IGNORE") {
        "IGNORE".len()
    } else {
        return None;
    };
    let mut pos = skip_ascii_whitespace(value, start + keyword_len);
    let index_keyword_len = if starts_with_keyword(value, pos, "INDEX") {
        "INDEX".len()
    } else if starts_with_keyword(value, pos, "KEY") {
        "KEY".len()
    } else {
        return None;
    };
    pos = skip_ascii_whitespace(value, pos + index_keyword_len);
    if starts_with_keyword(value, pos, "FOR") {
        pos = skip_ascii_whitespace(value, pos + "FOR".len());
        if starts_with_keyword(value, pos, "JOIN") {
            pos = skip_ascii_whitespace(value, pos + "JOIN".len());
        } else if starts_with_keyword(value, pos, "ORDER") {
            pos = skip_ascii_whitespace(value, pos + "ORDER".len());
            if !starts_with_keyword(value, pos, "BY") {
                return None;
            }
            pos = skip_ascii_whitespace(value, pos + "BY".len());
        } else if starts_with_keyword(value, pos, "GROUP") {
            pos = skip_ascii_whitespace(value, pos + "GROUP".len());
            if !starts_with_keyword(value, pos, "BY") {
                return None;
            }
            pos = skip_ascii_whitespace(value, pos + "BY".len());
        } else {
            return None;
        }
    }
    if value.as_bytes().get(pos) != Some(&b'(') {
        return None;
    }
    find_matching_paren(value, pos).map(|close_pos| close_pos + 1)
}

fn convert_mysql_quotes_to_dameng(value: &str, sql_mode: &str) -> String {
    let ansi_quotes = mysql_sql_mode_has_ansi_quotes(sql_mode);
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(ch) = chars.next() {
        if let Some(quote_char) = quote {
            if ch == '\\' && quote_char == '\'' {
                if let Some(next) = chars.next() {
                    if next == '\'' {
                        result.push_str("''");
                    } else {
                        result.push(ch);
                        result.push(next);
                    }
                }
                continue;
            }
            result.push(ch);
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

fn strip_mysql_comments(value: &str) -> String {
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

        if byte == b'-' && bytes.get(pos + 1) == Some(&b'-') {
            pos = skip_line_comment(value, pos + 2);
            continue;
        }

        if byte == b'#' {
            pos = skip_line_comment(value, pos + 1);
            continue;
        }

        if byte == b'/' && bytes.get(pos + 1) == Some(&b'*') {
            pos += 2;
            while pos + 1 < bytes.len() {
                if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                    pos += 2;
                    break;
                }
                if bytes[pos] == b'\n' {
                    result.push('\n');
                }
                pos += 1;
            }
            continue;
        }

        let ch = value[pos..].chars().next().unwrap();
        result.push(ch);
        pos += ch.len_utf8();
    }
    result
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

fn convert_mysql_charset_casts(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(cast_pos) = find_keyword_outside_quotes(value, search_pos, "CAST") {
        let open_pos = skip_ascii_whitespace(value, cast_pos + "CAST".len());
        if value.as_bytes().get(open_pos) != Some(&b'(') {
            search_pos = cast_pos + "CAST".len();
            continue;
        }
        let Some(close_pos) = find_matching_paren(value, open_pos) else {
            search_pos = cast_pos + "CAST".len();
            continue;
        };
        let Some(converted) = convert_mysql_charset_cast_args(&value[open_pos + 1..close_pos])
        else {
            search_pos = close_pos + 1;
            continue;
        };
        result.push_str(&value[copied_pos..cast_pos]);
        result.push_str(converted.as_str());
        copied_pos = close_pos + 1;
        search_pos = close_pos + 1;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn convert_mysql_charset_cast_args(args: &str) -> Option<String> {
    let as_pos = last_keyword_outside_quotes_before(args, "AS", args.len())?;
    let expr = args[..as_pos].trim();
    let mysql_type = args[as_pos + "AS".len()..].trim();
    if expr.is_empty() || !mysql_cast_type_is_charset_char(mysql_type) {
        return None;
    }
    Some(format!("CAST({} AS VARCHAR(255))", expr))
}

fn mysql_cast_type_is_charset_char(mysql_type: &str) -> bool {
    let mut pos = skip_ascii_whitespace(mysql_type, 0);
    if !starts_with_keyword(mysql_type, pos, "CHAR") {
        return false;
    }
    pos = skip_ascii_whitespace(mysql_type, pos + "CHAR".len());
    if pos >= mysql_type.len() {
        return true;
    }
    if starts_with_keyword(mysql_type, pos, "CHARACTER") {
        pos = skip_ascii_whitespace(mysql_type, pos + "CHARACTER".len());
        if !starts_with_keyword(mysql_type, pos, "SET") {
            return false;
        }
        pos = skip_ascii_whitespace(mysql_type, pos + "SET".len());
    } else if starts_with_keyword(mysql_type, pos, "CHARSET") {
        pos = skip_ascii_whitespace(mysql_type, pos + "CHARSET".len());
    } else {
        return false;
    }
    let Some((_charset, next_pos)) = take_sql_identifier(mysql_type, pos) else {
        return false;
    };
    mysql_type[next_pos..].trim().is_empty()
}

fn remove_mysql_collate_clauses(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(collate_pos) = find_keyword_outside_quotes(value, search_pos, "COLLATE") {
        let collation_pos = skip_ascii_whitespace(value, collate_pos + "COLLATE".len());
        let Some((_collation, next_pos)) = take_sql_identifier(value, collation_pos) else {
            search_pos = collate_pos + "COLLATE".len();
            continue;
        };
        result.push_str(&value[copied_pos..collate_pos]);
        copied_pos = next_pos;
        search_pos = next_pos;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn remove_mysql_character_set_clauses(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut copied_pos = 0usize;
    let mut search_pos = 0usize;
    while let Some(charset_pos) = find_mysql_character_set_clause(value, search_pos) {
        let Some(clause_end) = mysql_character_set_clause_end(value, charset_pos) else {
            search_pos = charset_pos + 1;
            continue;
        };
        result.push_str(&value[copied_pos..charset_pos]);
        copied_pos = clause_end;
        search_pos = clause_end;
    }
    result.push_str(&value[copied_pos..]);
    result
}

fn find_mysql_character_set_clause(value: &str, start: usize) -> Option<usize> {
    let character_pos = find_keyword_outside_quotes(value, start, "CHARACTER");
    let charset_pos = find_keyword_outside_quotes(value, start, "CHARSET");
    match (character_pos, charset_pos) {
        (Some(character_pos), Some(charset_pos)) => Some(character_pos.min(charset_pos)),
        (Some(character_pos), None) => Some(character_pos),
        (None, Some(charset_pos)) => Some(charset_pos),
        (None, None) => None,
    }
}

fn mysql_character_set_clause_end(value: &str, start: usize) -> Option<usize> {
    let mut pos = if starts_with_keyword(value, start, "CHARACTER") {
        let set_pos = skip_ascii_whitespace(value, start + "CHARACTER".len());
        if !starts_with_keyword(value, set_pos, "SET") {
            return None;
        }
        set_pos + "SET".len()
    } else if starts_with_keyword(value, start, "CHARSET") {
        start + "CHARSET".len()
    } else {
        return None;
    };
    pos = skip_ascii_whitespace(value, pos);
    if value.as_bytes().get(pos) == Some(&b'=') {
        pos = skip_ascii_whitespace(value, pos + 1);
    }
    let charset_start = pos;
    while pos < value.len() {
        let byte = value.as_bytes()[pos];
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
            pos += 1;
        } else {
            break;
        }
    }
    if pos == charset_start {
        None
    } else {
        Some(pos)
    }
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

fn convert_mysql_date_add_sub_interval_calls(value: &str) -> String {
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

        if let Some((converted, next_pos)) = convert_one_date_add_sub_interval_call(value, pos) {
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

fn convert_one_date_add_sub_interval_call(value: &str, pos: usize) -> Option<(String, usize)> {
    let (keyword, op) = if starts_with_keyword(value, pos, "DATE_ADD") {
        ("DATE_ADD", 1)
    } else if starts_with_keyword(value, pos, "DATE_SUB") {
        ("DATE_SUB", -1)
    } else {
        return None;
    };
    let bytes = value.as_bytes();
    let open_pos = skip_ascii_whitespace(value, pos + keyword.len());
    if bytes.get(open_pos) != Some(&b'(') {
        return None;
    }
    let close_pos = find_matching_paren(value, open_pos)?;
    let args = split_top_level_commas(&value[open_pos + 1..close_pos]);
    if args.len() != 2 {
        return None;
    }
    let expr = args[0].trim();
    let interval_arg = args[1].trim();
    if expr.is_empty() || !starts_with_keyword(interval_arg, 0, "INTERVAL") {
        return None;
    }
    let mut cursor = skip_ascii_whitespace(interval_arg, "INTERVAL".len());
    let (amount, amount_end) = take_interval_amount(interval_arg, cursor)?;
    cursor = skip_ascii_whitespace(interval_arg, amount_end);
    let (unit, unit_end) = take_interval_unit(interval_arg, cursor)?;
    if !interval_arg[unit_end..].trim().is_empty() {
        return None;
    }
    let amount_sql = if op < 0 {
        format!("-({})", amount)
    } else {
        amount
    };
    Some((
        format!("DATEADD({}, {}, {})", unit, amount_sql, expr),
        close_pos + 1,
    ))
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
            1
        }
        Some(b'-') => {
            cursor += 1;
            -1
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
    let (unit, next_pos) = take_interval_unit(value, cursor)?;
    let expr = &value[pos..=close_pos];
    let amount_sql = if op < 0 {
        format!("-({})", amount)
    } else {
        amount
    };
    Some((
        format!("DATEADD({}, {}, {})", unit, amount_sql, expr),
        next_pos,
    ))
}

fn take_interval_unit(value: &str, start: usize) -> Option<(&'static str, usize)> {
    for unit in ["YEAR", "MONTH", "DAY"] {
        if starts_with_keyword(value, start, unit) {
            return Some((unit, start + unit.len()));
        }
    }
    None
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

fn find_matching_routine_params_paren(value: &str, open_pos: usize) -> Option<usize> {
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

        if byte == b'-' && bytes.get(pos + 1) == Some(&b'-') {
            pos = skip_line_comment(value, pos + 2);
            continue;
        }
        if byte == b'#' {
            pos = skip_line_comment(value, pos + 1);
            continue;
        }
        if byte == b'/' && bytes.get(pos + 1) == Some(&b'*') {
            pos += 2;
            while pos + 1 < bytes.len() {
                if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                    pos += 2;
                    break;
                }
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

fn find_top_level_keyword_outside_quotes(
    value: &str,
    start: usize,
    keyword: &str,
) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut paren_depth = 0u32;
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
            b'`' | b'\'' | b'"' => {
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
            _ if paren_depth == 0 && starts_with_keyword(value, pos, keyword) => {
                return Some(pos);
            }
            _ => pos += 1,
        }
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
    fn removes_mysql_character_set_from_routine_declarations() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "charset_decl".to_string(),
            create_sql: "CREATE PROCEDURE `charset_decl`(IN p_value TEXT) BEGIN\n    DECLARE v_moduleId VARCHAR(50) CHARACTER SET utf8mb3;\n    SET v_moduleId = CAST(TRIM(p_value) AS CHAR CHARACTER SET utf8mb3);\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("    v_moduleId VARCHAR(50 CHAR);"));
        assert!(sql.contains("v_moduleId := CAST(TRIM(p_value) AS VARCHAR(255));"));
        assert!(!sql.contains("CHARACTER SET"));
        assert!(!sql.contains("utf8mb3"));
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
        assert_eq!(
            convert_mysql_sql_tokens(r#"SELECT 'charge.IsCheck=\'审核通过\''"#, ""),
            r#"SELECT 'charge.IsCheck=''审核通过'''"#
        );
        assert_eq!(
            convert_mysql_sql_tokens(
                "SET v = CAST(p AS CHAR CHARACTER SET utf8) COLLATE utf8_general_ci;",
                ""
            ),
            "SET v = CAST(p AS VARCHAR(255)) ;"
        );
        assert_eq!(
            convert_mysql_sql_tokens("SELECT CAST(v AS DECIMAL(10,2))", ""),
            "SELECT CAST(v AS DECIMAL(10,2))"
        );
    }

    #[test]
    fn converts_mysql_hash_comments_and_multi_variable_declarations() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "ns_services_import".to_string(),
            create_sql: r#"CREATE PROCEDURE `ns_services_import`()
begin
    DECLARE vID,fID,vParentID,vTemplateId,vflowId,vStepId
        ,ReceptionUserID,DispatchingUserID,AcceptanceUserID,OrderUserID,VisitUserID,CreateUserID,isDone bigint default 0;
    DECLARE serviceNo VARCHAR(20) default DATE_FORMAT(CURDATE(), '%Y%m%d');
    #如果有启用的通用流程，默认用通用流程，没有的话根据styleId获取流程
    set serviceNo = CONCAT("PBSBX",serviceNo,1);
    insert into ns_sr_services_log(op_remark,op_state,op_stepname)
    values(CONCAT("报事工单来源于",sourceName,"，报事分类：",typeName),"提","提交");
    set vID= LAST_INSERT_ID();
END"#
                .to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-service", &routine).unwrap();

        assert!(
            sql.contains("-- 如果有启用的通用流程，默认用通用流程，没有的话根据styleId获取流程")
        );
        assert!(sql.contains("    vID BIGINT default 0;"));
        assert!(sql.contains("    fID BIGINT default 0;"));
        assert!(sql.contains("    ReceptionUserID BIGINT default 0;"));
        assert!(
            sql.contains(
                "    serviceNo VARCHAR(20 CHAR) default DATE_FORMAT(CURDATE(), '%Y%m%d');"
            )
        );
        assert!(sql.contains("    serviceNo := CONCAT('PBSBX',serviceNo,1);"));
        assert!(sql.contains(
            "values(CONCAT('报事工单来源于',sourceName,'，报事分类：',typeName),'提','提交');"
        ));
        assert!(sql.contains("    vID := LAST_INSERT_ID();"));
        assert!(!sql.contains("#如果"));
        assert!(!sql.contains("set vID"));
    }

    #[test]
    fn strips_mysql_comments_from_routine_parameter_list() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "service_flowAndStepInsertOrUpdateStandard".to_string(),
            create_sql: r#"CREATE PROCEDURE `service_flowAndStepInsertOrUpdateStandard`(
    in p_templateName varchar(100),
    in p_flow_name varchar(100),
    in P_step_name varchar(100),        -- '步骤名称',
    in p_step_flag int,                 -- '步骤属性 1开始步骤 2中间步骤 3结束步骤',
    in p_service_status int,            -- '工单状态 10待提交, 11待受理',
    in p_approve_flag varchar(50),      -- '审核标志',#DELAY 表示延期审批流程
    in p_action_tag int,                -- 动作标签 230确认-通过 231确认-驳回 310作废-提交'
    /*
    延期审批 (approve_flag=DELAY)
    */
    in p_approve_id int,
    in p_kind_id int
)
label_exit:begin
    set @enterpriseID=107;
    leave label_exit;
end"#
                .to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-service", &routine).unwrap();

        assert!(sql.contains(
            r#""service_flowAndStepInsertOrUpdateStandard"(p_templateName IN VARCHAR(100 CHAR), p_flow_name IN VARCHAR(100 CHAR), P_step_name IN VARCHAR(100 CHAR), p_step_flag IN INT, p_service_status IN INT, p_approve_flag IN VARCHAR(50 CHAR), p_action_tag IN INT, p_approve_id IN INT, p_kind_id IN INT)"#
        ));
        assert!(sql.contains("enterpriseID :=107;"));
        assert!(sql.contains("RETURN;"));
        assert!(!sql.contains(r#""--""#));
        assert!(!sql.contains("#DELAY"));
    }

    #[test]
    fn routine_set_assignment_conversion_skips_multi_assignment() {
        let variables = vec!["p_name".to_string(), "a".to_string()];
        assert_eq!(
            convert_mysql_set_assignment_line(" SET p_name = CONCAT('id:', p_id);", &variables),
            " p_name := CONCAT('id:', p_id);"
        );
        assert_eq!(
            convert_mysql_set_assignment_line(" SET a = 1, b = 2;", &variables),
            " SET a = 1, b = 2;"
        );
    }

    #[test]
    fn routine_set_assignment_conversion_handles_multiple_set_statements() {
        let variables = vec![
            "selectSql".to_string(),
            "joinSql".to_string(),
            "n".to_string(),
            "m".to_string(),
        ];
        assert_eq!(
            convert_mysql_set_assignment_line(" SET selectSql = ''; SET joinSql = '';", &variables),
            " selectSql := ''; joinSql := '';"
        );
        assert_eq!(
            convert_mysql_set_assignment_line(" SET n = 0;SET m = 1;", &variables),
            " n := 0;m := 1;"
        );
    }

    #[test]
    fn routine_set_assignment_conversion_skips_update_set_clause() {
        let variables = vec!["new_form_data".to_string(), "new_buttonJson".to_string()];
        let converted = convert_mysql_set_assignments(
            "UPDATE ns_core_form\n  SET form_content = new_form_data\n  , button_json = new_buttonJson\n  WHERE form_code = 'caseManagement';",
            &variables,
        );

        assert!(converted.contains("  SET form_content = new_form_data"));
        assert!(converted.contains("  , button_json = new_buttonJson"));
        assert!(!converted.contains("form_content :="));
        assert!(!converted.contains("button_json :="));
    }

    #[test]
    fn removes_mysql_update_order_limit_tail() {
        let converted = convert_mysql_update_order_limit_clauses(
            "UPDATE ns_system_sql_log SET log_state = 'DONE'\nWHERE uuid = uuid_val ORDER BY id DESC FETCH FIRST 1 ROWS ONLY;",
        );

        assert_eq!(
            converted,
            "UPDATE ns_system_sql_log SET log_state = 'DONE'\nWHERE uuid = uuid_val;"
        );

        let with_subquery =
            "UPDATE t SET a = (SELECT id FROM x ORDER BY id FETCH FIRST 1 ROWS ONLY) WHERE id = 1;";
        assert_eq!(
            convert_mysql_update_order_limit_clauses(with_subquery),
            with_subquery
        );
    }

    #[test]
    fn converts_mysql_update_join_to_correlated_update() {
        let converted = convert_mysql_update_join_statements(
            "UPDATE ns_core_module target
    JOIN ns_core_module ref
ON ref.enterprise_id = target.enterprise_id
    AND ref.ver = target.ver
    AND ref.module_id = p_ref_module_id
    AND ref.order_id IS NOT NULL
    SET
        target.module_group = ref.module_group,
        target.order_id = CASE
        WHEN UPPER(p_order_mode) = 'BEFORE' THEN ref.order_id - 1
        ELSE ref.order_id + 1
END
WHERE target.module_id = p_module_id
          AND target.ver = p_ver
          AND NOT (
              target.module_group <=> ref.module_group
              AND target.order_id <=> CASE
                  WHEN UPPER(p_order_mode) = 'BEFORE' THEN ref.order_id - 1
                  ELSE ref.order_id + 1
              END
          );",
        );

        assert!(converted.starts_with("UPDATE ns_core_module dm_target\nSET"));
        assert!(converted.contains("FROM ns_core_module dm_source"));
        assert!(converted.contains("WHERE EXISTS ("));
        assert!(converted.contains("dm_source.enterprise_id = dm_target.enterprise_id"));
        assert!(converted.contains("module_group = (\n        SELECT dm_source.module_group"));
        assert!(converted.contains("order_id = (\n        SELECT CASE"));
        assert!(converted.contains(
            "((dm_target.module_group = dm_source.module_group) OR (dm_target.module_group IS NULL AND dm_source.module_group IS NULL))"
        ));
        assert!(converted.contains("((dm_target.order_id = CASE\n"));
        assert!(!converted.contains("JOIN ns_core_module ref"));
        assert!(!converted.contains("UPDATE ns_core_module target"));
        assert!(!converted.contains("ref."));
        assert!(!converted.contains("<=>"));
    }

    #[test]
    fn converts_mysql_null_safe_equal_operator() {
        let converted = convert_mysql_null_safe_equals(
            "AND target.order_id <=> CASE
                  WHEN UPPER(mode) = 'BEFORE' THEN ref.order_id - 1
                  ELSE ref.order_id + 1
              END
          );",
        );

        assert!(converted.contains("((target.order_id = CASE"));
        assert!(converted.contains("target.order_id IS NULL AND CASE"));
        assert!(!converted.contains("<=>"));
        assert!(converted.trim_end().ends_with(");"));
    }

    #[test]
    fn converts_outer_block_leave_to_return_for_procedure() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "leave_outer".to_string(),
            create_sql: "CREATE PROCEDURE `leave_outer`() outer_label: BEGIN\n    IF 1 = 1 THEN\n        LEAVE outer_label;\n    END IF;\n    SELECT 1;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("        RETURN;"));
        assert!(!sql.contains("EXIT outer_label;"));
    }

    #[test]
    fn wraps_mysql_single_statement_procedure_body() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "single_statement".to_string(),
            create_sql: "CREATE PROCEDURE `single_statement`() IF NOT EXISTS (\nSELECT 1 FROM information_schema.COLUMNS WHERE table_schema = (select database()) AND table_name = 'demo' AND column_name = 'name'\n) THEN\nALTER TABLE demo ADD COLUMN `name` varchar(50) DEFAULT '';\nEND IF".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("AS\nBEGIN\nIF NOT EXISTS"));
        assert!(sql.contains("END IF;\nEND;"));
        assert!(sql.trim_end().ends_with("END;"));
        assert!(sql.contains("FROM ALL_TAB_COLUMNS"));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'ALTER TABLE "target_schema"."demo" ADD "name" VARCHAR(50 CHAR) DEFAULT ''''';"#
        ));
    }

    #[test]
    fn converts_labeled_begin_block_leave_to_goto() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "report_procedure".to_string(),
            create_sql: "CREATE PROCEDURE `report_procedure`() BEGIN\n  report_procedure: BEGIN\n    IF NOT EXISTS (SELECT 1 FROM t) THEN\n      LEAVE report_procedure;\n    END IF;\n    SELECT 1;\n  END report_procedure;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-hr", &routine).unwrap();

        assert!(!sql.contains("report_procedure: BEGIN"));
        assert!(sql.contains("      GOTO report_procedure_end;"));
        assert!(sql.contains("  END;\n  <<report_procedure_end>>\n  NULL;"));
        assert!(!sql.contains("EXIT report_procedure;"));
    }

    #[test]
    fn converts_nested_mysql_declare_block() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "nested_declare".to_string(),
            create_sql: "CREATE PROCEDURE `nested_declare`() BEGIN\n  BEGIN\n    DECLARE done INT DEFAULT FALSE;\n    DECLARE cur CURSOR FOR SELECT `id` FROM `orders`;\n    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;\n    OPEN cur;\n    read_loop: LOOP\n      FETCH cur INTO done;\n      IF done THEN\n        LEAVE read_loop;\n      END IF;\n    END LOOP;\n  END;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("  DECLARE\n      done INT DEFAULT FALSE;"));
        assert!(sql.contains(r#"CURSOR cur IS SELECT "id" FROM "orders";"#));
        assert!(!sql.contains("DECLARE done"));
        assert!(!sql.contains("CONTINUE HANDLER"));
        assert!(sql.contains("      IF cur%NOTFOUND THEN\n          done := 1;"));
    }

    #[test]
    fn fetch_not_found_conversion_ignores_fetch_first_limit() {
        let converted = convert_mysql_fetch_not_found_checks(
            "SELECT id FROM orders\nFETCH FIRST 1 ROWS ONLY;",
            Some("done"),
        );

        assert_eq!(converted, "SELECT id FROM orders\nFETCH FIRST 1 ROWS ONLY;");
        assert!(!converted.contains("FIRST%NOTFOUND"));
    }

    #[test]
    fn converts_mysql_delete_alias_statement() {
        assert_eq!(
            convert_mysql_delete_alias_statement_line(
                "delete x.* from tmp_all_precinct_to_insert_20220317 x where key_code in ("
            ),
            "DELETE FROM tmp_all_precinct_to_insert_20220317 x where key_code in ("
        );
        assert_eq!(
            convert_mysql_delete_alias_statement_line("DELETE x FROM demo x WHERE x.id = 1;"),
            "DELETE FROM demo x WHERE x.id = 1;"
        );
    }

    #[test]
    fn removes_mysql_index_hints() {
        let sql = "DELETE FROM ns_core_role_perm x USE index(ns_core_role_perm_idx) WHERE id = 1;\nSELECT * FROM orders FORCE KEY FOR ORDER BY (idx_orders) WHERE note = 'USE index(keep_me)';\nSELECT * FROM users IGNORE INDEX FOR JOIN (idx_users);";

        let converted = convert_mysql_index_hints(sql);

        assert!(converted.contains("DELETE FROM ns_core_role_perm x  WHERE id = 1;"));
        assert!(converted.contains("SELECT * FROM orders  WHERE note = 'USE index(keep_me)';"));
        assert!(converted.contains("SELECT * FROM users ;"));
        assert!(!converted.contains("ns_core_role_perm_idx"));
        assert!(!converted.contains("idx_orders"));
        assert!(!converted.contains("idx_users"));
    }

    #[test]
    fn converts_multiline_while_do_to_loop() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "multi_while".to_string(),
            create_sql: "CREATE PROCEDURE `multi_while`() BEGIN\n    DECLARE i INT DEFAULT 1;\n    WHILE (i <= 3)\n        DO\n            SET i = i + 1;\n    END WHILE;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("WHILE (i <= 3) LOOP"));
        assert!(sql.contains("            i := i + 1;"));
        assert!(sql.contains("    END LOOP;"));
        assert!(!sql.contains("WHILE (i <= 3)\n        NULL;"));
    }

    #[test]
    fn converts_mysql_labeled_while_to_dameng_label() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "labeled_while".to_string(),
            create_sql: "CREATE PROCEDURE `labeled_while`() BEGIN\n    DECLARE i INT DEFAULT 1;\n    my_while_loop:\n    WHILE (i <= 3)\n        DO\n            SET i = i + 1;\n    END WHILE my_while_loop;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(
            sql.contains("<<my_while_loop>>\n    WHILE (i <= 3) LOOP"),
            "{sql}"
        );
        assert!(sql.contains("            i := i + 1;"));
        assert!(sql.contains("    END LOOP;"));
        assert!(!sql.contains("my_while_loop:"));
        assert!(!sql.contains("END WHILE my_while_loop;"));
    }

    #[test]
    fn converts_mysql_prepared_statement_to_execute_immediate() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "dynamic_sql".to_string(),
            create_sql: "CREATE PROCEDURE `dynamic_sql`() BEGIN\n    DECLARE dynamicSQL VARCHAR(1000) DEFAULT '';\n    SET dynamicSQL = 'SELECT 1';\n    PREPARE stmt FROM dynamicSQL;\n    EXECUTE stmt;\n    DEALLOCATE PREPARE stmt;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("    EXECUTE IMMEDIATE dynamicSQL;"));
        assert!(!sql.contains("PREPARE stmt"));
        assert!(!sql.contains("EXECUTE stmt;"));
        assert!(!sql.contains("DEALLOCATE PREPARE"));
    }

    #[test]
    fn removes_mysql_end_alias_before_select_into() {
        let line = "    SELECT DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m') END INTO sDate;";
        assert_eq!(
            convert_mysql_select_end_alias_into_line(line),
            "    SELECT DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m') INTO sDate;"
        );
        let case_line = "    SELECT CASE WHEN searchType = '0' THEN LEFT(searchDate, 4) ELSE searchDate END INTO sDate;";
        assert_eq!(
            convert_mysql_select_end_alias_into_line(case_line),
            case_line
        );
    }

    #[test]
    fn converts_mysql_postfix_select_into() {
        assert_eq!(
            convert_mysql_postfix_select_into_statement(
                "            select count(1)\n            from dws_chargepaid_month_sum\n            into nrows;"
            ),
            "            select count(1) INTO nrows from dws_chargepaid_month_sum;"
        );
        assert_eq!(
            convert_mysql_postfix_select_into_statement(
                "            select count(1) from dim_org_precinct where deleteFlag = 0 into trows;"
            ),
            "            select count(1) INTO trows from dim_org_precinct where deleteFlag = 0;"
        );
        let already_converted = "            SELECT COUNT(1) INTO n FROM temp_target;";
        assert_eq!(
            convert_mysql_postfix_select_into_statement(already_converted),
            already_converted
        );
    }

    #[test]
    fn converts_mysql_limit_before_postfix_select_into() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "select_limit_into".to_string(),
            create_sql: "CREATE PROCEDURE `select_limit_into`() BEGIN\n    DECLARE source VARCHAR(10);\n    SELECT dataSource FROM dw_datacenter_charge LIMIT 1 INTO source;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(
            sql.contains(
                "SELECT dataSource INTO source FROM dw_datacenter_charge FETCH FIRST 1 ROWS ONLY;"
            ),
            "{sql}"
        );
        assert!(!sql.contains("FETCH FIRST 1 ROWS ONLY INTO"));
    }

    #[test]
    fn converts_date_format_plus_day_interval() {
        assert_eq!(
            convert_mysql_sql_tokens(
                "OperatorDate < DATE_FORMAT( end_date,'%Y-%m-%d') +INTERVAL 1 day",
                ""
            ),
            "OperatorDate < DATEADD(DAY, 1, DATE_FORMAT( end_date,'%Y-%m-%d'))"
        );
        assert_eq!(
            convert_mysql_sql_tokens(
                "OperatorDate < DATE_FORMAT(end_date, '%Y-%m-%d') + INTERVAL 1 DAY",
                ""
            ),
            "OperatorDate < DATEADD(DAY, 1, DATE_FORMAT(end_date, '%Y-%m-%d'))"
        );
    }

    #[test]
    fn converts_date_format_minus_day_interval() {
        assert_eq!(
            convert_mysql_sql_tokens(
                "OperatorDate >= date_format(begin_date,'%Y-%m-%d') - interval 2 day",
                ""
            ),
            "OperatorDate >= DATEADD(DAY, -(2), date_format(begin_date,'%Y-%m-%d'))"
        );
    }

    #[test]
    fn converts_date_format_variable_interval_units() {
        assert_eq!(
            convert_mysql_sql_tokens(
                "LEFT(DATE_FORMAT(searchDate, '%Y-%m-%d') + INTERVAL offsetDate year, 4)",
                ""
            ),
            "LEFT(DATEADD(YEAR, offsetDate, DATE_FORMAT(searchDate, '%Y-%m-%d')), 4)"
        );
        assert_eq!(
            convert_mysql_sql_tokens(
                "LEFT(DATE_FORMAT(searchDate, '%Y-%m-%d') + INTERVAL offsetDate month, 7)",
                ""
            ),
            "LEFT(DATEADD(MONTH, offsetDate, DATE_FORMAT(searchDate, '%Y-%m-%d')), 7)"
        );
        assert_eq!(
            convert_mysql_sql_tokens(
                "DATE_FORMAT(searchDate, '%Y-%m-%d') + INTERVAL offsetDate day",
                ""
            ),
            "DATEADD(DAY, offsetDate, DATE_FORMAT(searchDate, '%Y-%m-%d'))"
        );
    }

    #[test]
    fn converts_mysql_date_add_sub_interval_calls() {
        assert_eq!(
            convert_mysql_sql_tokens("DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)", ""),
            "DATEADD(DAY, -(1), CURRENT_DATE)"
        );
        assert_eq!(
            convert_mysql_sql_tokens("DATE_ADD(order_date, INTERVAL offsetDate MONTH)", ""),
            "DATEADD(MONTH, offsetDate, order_date)"
        );
        assert_eq!(
            convert_mysql_sql_tokens(
                "DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), '%Y-%m')",
                ""
            ),
            "DATE_FORMAT(DATEADD(DAY, -(1), CURRENT_DATE), '%Y-%m')"
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

    #[test]
    fn converts_mysql_alter_table_in_routine_to_dynamic_dameng_sql() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "addColPaidDetId".to_string(),
            create_sql: r#"CREATE PROCEDURE `addColPaidDetId`()
BEGIN
IF NOT EXISTS (
    SELECT COLUMN_NAME FROM information_schema.COLUMNS
    WHERE table_schema = (select database()) AND table_name = 'ns_payment_order' AND column_name = 'PaidDetId'
) THEN
    alter table `ns_payment_order` add PaidDetId varchar(50) NULL DEFAULT NULL COMMENT 'third paid id';
    alter table `ns_process_task` add  column `buinessKey` varchar(50) DEFAULT '' COMMENT 'business key';
    alter table charge_goodstaxrate add column
        differentialTaxPercentage varchar(8) null comment 'tax percentage';
    IF 1 = 1 THEN alter table charge_goodstaxrate add column inlineCol varchar(8) null comment 'inline column'; END IF;
    alter table `ns_payment_order` modify HouseList longtext NULL DEFAULT NULL COMMENT 'room list';
    alter table ns_soss_publicizer_home_banner modify COLUMN `pictureGroupId` varchar(255) default NULL COMMENT '图片';
    alter table `ns_payment_order` add index idx_precinctIDandPaidDetid(PaidDetId,PrecinctID);
    alter table `ns_payment_orderpaydetail`add index idx_PaidDetId(PaidDetId);
END IF;
IF NOT EXISTS (
    SELECT 1 FROM information_schema.statistics
    WHERE table_schema='newsee-bill-10' AND table_name = 'ns_equip_custom_type' AND index_name = 'record_uidx_idCode'
) THEN
    ALTER TABLE `newsee-bill-10`.`ns_equip_custom_type` ADD UNIQUE INDEX `record_uidx_idCode` (`enterpriseID`, `customerTypecode`);
END IF;
END"#
                .to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-bill-10", &routine).unwrap();

        assert!(sql.contains("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS"));
        assert!(sql.contains("UPPER(OWNER) = UPPER('newsee-bill-10')"));
        assert!(sql.contains("UPPER(TABLE_NAME) = UPPER('ns_payment_order')"));
        assert!(sql.contains("UPPER(COLUMN_NAME) = UPPER('PaidDetId')"));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'ALTER TABLE "newsee-bill-10"."ns_payment_order" ADD "PaidDetId" VARCHAR(50 CHAR) NULL DEFAULT NULL';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'COMMENT ON COLUMN "newsee-bill-10"."ns_payment_order"."PaidDetId" IS ''third paid id''';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'ALTER TABLE "newsee-bill-10"."ns_process_task" ADD "buinessKey" VARCHAR(50 CHAR) DEFAULT ''''';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'COMMENT ON COLUMN "newsee-bill-10"."ns_process_task"."buinessKey" IS ''business key''';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'ALTER TABLE "newsee-bill-10"."charge_goodstaxrate" ADD "differentialTaxPercentage" VARCHAR(8 CHAR) null';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'COMMENT ON COLUMN "newsee-bill-10"."charge_goodstaxrate"."differentialTaxPercentage" IS ''tax percentage''';"#
        ));
        assert!(sql.contains(
            r#"THEN EXECUTE IMMEDIATE 'ALTER TABLE "newsee-bill-10"."charge_goodstaxrate" ADD "inlineCol" VARCHAR(8 CHAR) null';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'COMMENT ON COLUMN "newsee-bill-10"."charge_goodstaxrate"."inlineCol" IS ''inline column'''; END IF;"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'ALTER TABLE "newsee-bill-10"."ns_payment_order" MODIFY "HouseList" CLOB NULL DEFAULT NULL';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'COMMENT ON COLUMN "newsee-bill-10"."ns_payment_order"."HouseList" IS ''room list''';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'ALTER TABLE "newsee-bill-10"."ns_soss_publicizer_home_banner" MODIFY "pictureGroupId" VARCHAR(255 CHAR) default NULL';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'COMMENT ON COLUMN "newsee-bill-10"."ns_soss_publicizer_home_banner"."pictureGroupId" IS ''图片''';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE INDEX "idx_precinctIDandPaidDetid" ON "newsee-bill-10"."ns_payment_order" ("PaidDetId", "PrecinctID")';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE INDEX "idx_PaidDetId" ON "newsee-bill-10"."ns_payment_orderpaydetail" ("PaidDetId")';"#
        ));
        assert!(sql.contains("SELECT 1 FROM ALL_INDEXES"));
        assert!(sql.contains("UPPER(INDEX_NAME) = UPPER('record_uidx_idCode')"));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX "record_uidx_idCode" ON "newsee-bill-10"."ns_equip_custom_type" ("enterpriseID", "customerTypecode")';"#
        ));
    }

    #[test]
    fn converts_mysql_temporary_table_as_select_with_parentheses() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "temp_as_select".to_string(),
            create_sql: "CREATE PROCEDURE `temp_as_select`() BEGIN\n    CREATE TEMPORARY TABLE tmp_demo AS (SELECT id FROM src_demo);\n    DROP TABLE tmp_demo;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE "target_schema"."tmp_demo" AS SELECT id FROM src_demo';"#
        ));
        assert!(sql.contains(r#"EXECUTE IMMEDIATE 'DROP TABLE "target_schema"."tmp_demo"';"#));
        assert!(!sql.contains("CREATE TEMPORARY TABLE tmp_demo"));
    }

    #[test]
    fn converts_mysql_view_ddl_in_routine_to_dynamic_sql() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "create_demo_view".to_string(),
            create_sql: "CREATE PROCEDURE `create_demo_view`() BEGIN\nIF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = N'v_demo') THEN\nDROP VIEW v_demo;\nEND IF;\nCREATE VIEW v_demo AS SELECT `a`.`id` AS `id`, 'x' AS label FROM demo a;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("FROM ALL_VIEWS WHERE VIEW_NAME = N'v_demo'"));
        assert!(sql.contains(r#"EXECUTE IMMEDIATE 'DROP VIEW "target_schema"."v_demo"';"#));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE VIEW "target_schema"."v_demo" AS SELECT "a"."id" AS "id", ''x'' AS label FROM demo a';"#
        ));
        assert!(!sql.contains("\nDROP VIEW v_demo;"));
        assert!(!sql.contains("\nCREATE VIEW v_demo AS"));
    }

    #[test]
    fn converts_mysql_create_table_with_indexes_in_routine_to_dynamic_sql() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "create_table_with_indexes".to_string(),
            create_sql: "CREATE PROCEDURE `create_table_with_indexes`() BEGIN\n    CREATE TABLE IF NOT EXISTS `source_schema`.`temp_house_id` (\n        `id` bigint(32) NOT NULL AUTO_INCREMENT,\n        `house_id` bigint(32) NOT NULL,\n        PRIMARY KEY (`id`) USING BTREE,\n        INDEX idx_houseId(`house_id`)\n    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE "target_schema"."temp_house_id" ("id" BIGINT, "house_id" BIGINT NOT NULL) ON COMMIT PRESERVE ROWS';"#
        ));
        assert!(!sql.contains("AUTO_INCREMENT"));
        assert!(!sql.contains("USING BTREE"));
        assert!(!sql.contains("ENGINE=InnoDB"));
    }

    #[test]
    fn converts_mysql_user_variables_and_limit_in_routine() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "add_menu".to_string(),
            create_sql: r#"CREATE PROCEDURE `add_menu`()
BEGIN
    IF NOT EXISTS (
        select * from ns_core_funcinfo where ENTERPRISE_ID=@enterpriseId and ORGANIZATION_ID=@organizationId limit 1
    ) THEN
        INSERT INTO ns_core_role_perm (ENTERPRISE_ID, ORGANIZATION_ID, PERID, ROLEID)
        SELECT @enterpriseId,@organizationId, PERID,@roleId FROM ns_core_permission WHERE FUNCID IN ('demo');
    END IF;
END"#
                .to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-bill-10", &routine).unwrap();

        assert!(sql.contains("AS\n    enterpriseId VARCHAR(255);"));
        assert!(sql.contains("    organizationId VARCHAR(255);"));
        assert!(sql.contains("    roleId VARCHAR(255);"));
        assert!(sql.contains("ENTERPRISE_ID=enterpriseId"));
        assert!(sql.contains("ORGANIZATION_ID=organizationId FETCH FIRST 1 ROWS ONLY"));
        assert!(sql.contains("SELECT enterpriseId,organizationId, PERID,roleId"));
        assert!(!sql.contains("@enterpriseId"));
        assert!(!sql.contains(" limit 1"));
    }

    #[test]
    fn converts_mysql_insert_value_keyword() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "insert_value".to_string(),
            create_sql: "CREATE PROCEDURE `insert_value`() BEGIN\n    INSERT INTO log_table(id, msg)\n        value (1, 'keep VALUE (text) literal');\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("        VALUES (1, 'keep VALUE (text) literal');"));
        assert!(!sql.contains("\n        value ("));
        assert!(sql.contains("'keep VALUE (text) literal'"));
    }

    #[test]
    fn converts_mysql_insert_value_keyword_before_comments() {
        let converted = convert_mysql_insert_value_keyword(
            "INSERT INTO t(a)\nVALUE\n  -- comment\n  (1),\n  /* block */\n  (2);",
        );

        assert!(converted.contains("VALUES\n  -- comment\n  (1)"));
        assert!(!converted.contains("\nVALUE\n"));
    }

    #[test]
    fn converts_mysql_insert_ignore_keyword() {
        let converted = convert_mysql_insert_ignore_keyword(
            "INSERT IGNORE INTO t(a) VALUES (1);\nSELECT 'INSERT IGNORE INTO keep';\n-- INSERT IGNORE INTO comment\nINSERT /* gap */ IGNORE\nINTO t2(a) VALUES (2);",
        );

        assert!(converted.contains("INSERT INTO t(a) VALUES (1);"));
        assert!(converted.contains("SELECT 'INSERT IGNORE INTO keep';"));
        assert!(converted.contains("-- INSERT IGNORE INTO comment"));
        assert!(converted.contains("INSERT INTO t2(a) VALUES (2);"));
        assert!(!converted.contains("INSERT IGNORE INTO t(a)"));
    }

    #[test]
    fn converts_mysql_regexp_operator() {
        assert_eq!(
            convert_mysql_regexp_operators(
                "IF item != '' and item REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' = 0 THEN"
            ),
            "IF item != '' and NOT REGEXP_LIKE(item, '^-?[0-9]+(\\\\.[0-9]+)?$') THEN"
        );
        assert_eq!(
            convert_mysql_regexp_operators(
                "IF SUBSTRING(reserved_backup_table, LOCATE('_bak_', reserved_backup_table) + 5) NOT REGEXP '^[0-9]{8}$' THEN"
            ),
            "IF NOT REGEXP_LIKE(SUBSTRING(reserved_backup_table, LOCATE('_bak_', reserved_backup_table) + 5), '^[0-9]{8}$') THEN"
        );
        assert_eq!(
            convert_mysql_regexp_operators("SELECT 'item REGEXP pattern = 0'"),
            "SELECT 'item REGEXP pattern = 0'"
        );
    }

    #[test]
    fn converts_mysql_session_set_and_group_concat_in_routine() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "lastSalary_procedure".to_string(),
            create_sql: r#"CREATE PROCEDURE `lastSalary_procedure`(IN salaryMonth DATE, IN userIds VARCHAR(8000), IN enterpriseId BIGINT)
BEGIN
 SET SESSION group_concat_max_len=102400;
 SET sql_safe_updates = 0;
 SET @sql = NULL;
 SELECT GROUP_CONCAT(DISTINCT
   CONCAT('SUM(CASE WHEN a.salaryId = ''', id, ''' THEN a.money ELSE 0 END) AS ', CONCAT('`', salaryItemName, '`'))
 ) INTO @sql
 FROM ns_salary_setting
 WHERE deleteFlag=0;
 PREPARE stmt FROM @sql;
 EXECUTE stmt;
 DEALLOCATE PREPARE stmt;
END"#
                .to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-hr", &routine).unwrap();

        assert!(!sql.contains("SET SESSION"));
        assert!(!sql.contains("sql_safe_updates"));
        assert!(sql.contains("NULL;"));
        assert!(sql.contains("LISTAGG(DISTINCT"));
        assert!(sql.contains("WITHIN GROUP (ORDER BY"));
        assert!(sql.contains("EXECUTE IMMEDIATE sql;"));
    }

    #[test]
    fn extracts_declarations_separated_by_comments() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "compare_payment_precinct_data".to_string(),
            create_sql: r#"CREATE PROCEDURE `compare_payment_precinct_data`()
BEGIN
    DECLARE etrId BIGINT (20);
    DECLARE v_preinctId BIGINT;

    -- cursor by precinct
    DECLARE cur_precinct CURSOR FOR
        SELECT DISTINCT PrecinctID
        FROM ns_payment_chargepayment
        ORDER BY PrecinctID;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    -- executable section
    DROP TABLE IF EXISTS tmp_a,tmp_b;
    DROP TEMPORARY TABLE IF EXISTS tmp_payment_compare_result;
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_a SELECT id FROM source_table;
    CREATE TEMPORARY TABLE tmp_payment_compare_result (
        preinctId BIGINT,
        diff_type VARCHAR(100)
    ) ENGINE=Memory;
    SELECT 'static text' AS summary;
    SELECT v_diff_count AS 'diff count';
    OPEN cur_precinct;
    read_loop: LOOP
        FETCH cur_precinct INTO v_preinctId;
        IF v_done = 1 THEN
            LEAVE read_loop;
        END IF;
    END LOOP;
    WHILE v_done <> 1 DO
        FETCH cur_precinct INTO v_preinctId;
        DO SLEEP(0);
    END WHILE;
END"#
                .to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("newsee-bill-10", &routine).unwrap();

        assert!(sql.contains("AS\n    etrId BIGINT;\n    v_preinctId BIGINT;"));
        assert!(sql.contains("    CURSOR cur_precinct IS SELECT DISTINCT PrecinctID"));
        assert!(!sql.contains("CONTINUE HANDLER"));
        assert!(sql.contains("BEGIN\n-- executable section"));
        assert!(!sql.contains("BEGIN\n    DECLARE cur_precinct"));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'DROP TABLE "newsee-bill-10"."tmp_payment_compare_result"';"#
        ));
        assert!(sql.contains(r#"EXECUTE IMMEDIATE 'DROP TABLE "newsee-bill-10"."tmp_a"';"#));
        assert!(sql.contains(r#"EXECUTE IMMEDIATE 'DROP TABLE "newsee-bill-10"."tmp_b"';"#));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE "newsee-bill-10"."tmp_a" AS SELECT id FROM source_table';"#
        ));
        assert!(sql.contains(
            r#"EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE "newsee-bill-10"."tmp_payment_compare_result" ("preinctId" BIGINT, "diff_type" VARCHAR(100 CHAR)) ON COMMIT PRESERVE ROWS';"#
        ));
        assert!(sql.contains("SELECT 'static text' AS summary;"));
        assert!(sql.contains(r#"SELECT v_diff_count AS "diff count";"#));
        assert!(sql.contains("    <<read_loop>>\n    LOOP"));
        assert!(sql.contains("        FETCH cur_precinct INTO v_preinctId;"));
        assert!(sql.contains("        IF cur_precinct%NOTFOUND THEN\n            v_done := 1;"));
        assert!(sql.contains("            EXIT read_loop;"));
        assert!(sql.contains("    WHILE v_done <> 1 LOOP"));
        assert!(sql.contains("        NULL;"));
        assert!(sql.contains("    END LOOP;"));
    }

    #[test]
    fn parses_mysql_not_found_handlers() {
        assert_eq!(
            mysql_not_found_handler_variable_from_declaration(
                "CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;"
            ),
            Some("v_done".to_string())
        );
        assert_eq!(
            mysql_not_found_handler_variable_from_declaration(
                "CONTINUE HANDLER FOR SQLSTATE '02000'SET STOP=1;"
            ),
            Some("STOP".to_string())
        );
    }

    #[test]
    fn skips_mysql_exception_handler_blocks_and_converts_row_count_diagnostics() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "handler_demo".to_string(),
            create_sql: "CREATE PROCEDURE `handler_demo`() BEGIN\n    DECLARE code CHAR(5) DEFAULT '00000';\n    DECLARE nrows INT;\n    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION\n        BEGIN\n            GET DIAGNOSTICS CONDITION 1 code = RETURNED_SQLSTATE;\n        END;\n    INSERT INTO t(id) VALUES (1);\n    GET DIAGNOSTICS nrows = ROW_COUNT;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(!sql.contains("CONTINUE HANDLER"));
        assert!(!sql.contains("RETURNED_SQLSTATE"));
        assert!(sql.contains("    nrows := SQL%ROWCOUNT;"));
        assert!(sql.contains("BEGIN\nINSERT INTO t(id) VALUES (1);"));
    }

    #[test]
    fn skips_nested_exit_handler_only_declaration_block() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "atomic_backup_and_compare".to_string(),
            create_sql: "CREATE PROCEDURE `atomic_backup_and_compare`(OUT p_final_result INT, OUT p_error_message VARCHAR(1000))\nproc_block: BEGIN\n    DECLARE v_sql_err_msg TEXT;\n    DECLARE v_sql_err_no INT;\n    proc_main: BEGIN\n        DECLARE EXIT HANDLER FOR SQLEXCEPTION \n        BEGIN\n            GET DIAGNOSTICS CONDITION 1\n                v_sql_err_no = MYSQL_ERRNO,\n                v_sql_err_msg = MESSAGE_TEXT;\n            SET p_error_message = CONCAT('SQL错误 [', v_sql_err_no, ']: ', v_sql_err_msg);\n            SET p_final_result = 4;\n        END;\n        LEAVE proc_main;\n    END proc_main;\nEND proc_block".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(!sql.contains("EXIT HANDLER"));
        assert!(!sql.contains("GET DIAGNOSTICS CONDITION"));
        assert!(!sql.contains("MYSQL_ERRNO"));
        assert!(sql.contains("        GOTO proc_main_end;"));
        assert!(sql.contains("    <<proc_main_end>>\n    NULL;"));
    }

    #[test]
    fn converts_mysql_signal_sqlstate_to_dameng_error() {
        let routine = MySqlRoutineDefinition {
            kind: MySqlRoutineKind::Procedure,
            name: "signal_demo".to_string(),
            create_sql: "CREATE PROCEDURE `signal_demo`() BEGIN\n    DECLARE message_text VARCHAR(255);\n    SET message_text = 'bad input';\n    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = message_text;\nEND".to_string(),
            sql_mode: String::new(),
        };

        let sql = convert_mysql_routine_to_dameng("target_schema", &routine).unwrap();

        assert!(sql.contains("message_text := 'bad input';"));
        assert!(sql.contains("    RAISE_APPLICATION_ERROR(-20000, message_text);"));
        assert!(!sql.contains("SIGNAL SQLSTATE"));
    }

    #[test]
    fn quotes_mysql_unsafe_unquoted_aliases() {
        let alias = "\u{9884}\u{6536}\u{6b3e}\u{7ed3}\u{8f6c}\u{6027}\u{80fd}\u{ff1a}";
        let sql = format!(
            "SELECT 1 AS {}; SELECT CAST(v AS DECIMAL(10,2)) AS amount;",
            alias
        );
        let converted = convert_mysql_single_quoted_aliases(sql.as_str());

        assert!(converted.contains(format!("AS \"{}\"", alias).as_str()));
        assert!(converted.contains("CAST(v AS DECIMAL(10,2)) AS amount"));
    }
}
