// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// SQL-Like Predicate Expression Parser and Evaluator

string expression_str;

void create(string _expr) {
    expression_str = String.trim_all_whites(_expr || "1=1");
}

//! Evaluate the predicate expression against an evaluation context.
//! Context holds topic, parsed payload, and properties.
int evaluate(mapping(string:mixed) context) {
    if (expression_str == "1=1" || expression_str == "true" || sizeof(expression_str) == 0) {
        return 1;
    }

    // Split on AND (simplified conjunction evaluator)
    array(string) and_clauses = expression_str / " AND ";
    foreach (and_clauses, string clause) {
        if (!_evaluate_single_clause(String.trim_all_whites(clause), context)) {
            return 0;
        }
    }

    return 1;
}

//! Evaluate a single binary comparison clause: left OP right
private int _evaluate_single_clause(string clause, mapping(string:mixed) ctx) {
    array(string) ops = ({ ">=", "<=", "!=", "==", ">", "<", " CONTAINS " });
    string matched_op = 0;

    foreach (ops, string op) {
        if (search(clause, op) >= 0) {
            matched_op = op;
            break;
        }
    }

    if (!matched_op) return 1;

    int op_pos = search(clause, matched_op);
    string left_expr = String.trim_all_whites(clause[0..op_pos - 1]);
    string right_expr = String.trim_all_whites(clause[op_pos + sizeof(matched_op)..]);

    mixed left_val = _resolve_val(left_expr, ctx);
    mixed right_val = _resolve_val(right_expr, ctx);

    string op_trimmed = String.trim_all_whites(matched_op);

    switch (op_trimmed) {
        case "==":
            return sprintf("%O", left_val) == sprintf("%O", right_val);
        case "!=":
            return sprintf("%O", left_val) != sprintf("%O", right_val);
        case ">":
            return (float)left_val > (float)right_val;
        case "<":
            return (float)left_val < (float)right_val;
        case ">=":
            return (float)left_val >= (float)right_val;
        case "<=":
            return (float)left_val <= (float)right_val;
        case "CONTAINS":
            if (stringp(left_val) && stringp(right_val)) {
                return search(left_val, right_val) >= 0;
            }
            return 0;
        default:
            return 0;
    }
}

private mixed _resolve_val(string expr, mapping(string:mixed) ctx) {
    // Quoted string literal
    if (sizeof(expr) >= 2 && ((expr[0] == '"' && expr[sizeof(expr)-1] == '"') ||
                             (expr[0] == '\'' && expr[sizeof(expr)-1] == '\''))) {
        return expr[1..sizeof(expr)-2];
    }

    // Number literal
    float f_val;
    if (sscanf(expr, "%f", f_val) == 1 && (search(expr, ".") >= 0 || sprintf("%f", f_val) == expr)) {
        return f_val;
    }
    int i_val;
    if (sscanf(expr, "%d", i_val) == 1 && sprintf("%d", i_val) == expr) {
        return i_val;
    }

    // JSONPath variable lookup
    if (has_prefix(expr, "payload.")) {
        string path = expr[8..];
        return .JsonPath.get_value(ctx->payload, path);
    }
    if (expr == "topic") {
        return ctx->topic;
    }

    return .JsonPath.get_value(ctx, expr);
}
