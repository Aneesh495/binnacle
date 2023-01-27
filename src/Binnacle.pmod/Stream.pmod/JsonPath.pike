// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Lightweight JSONPath Field Extractor

//! Extract nested field value from mapping/array data structure using dot-separated path.
//! Supports dot notation (e.g. "payload.temperature") and array indices (e.g. "readings[0].val").
mixed get_value(mixed root, string path) {
    if (!root || !path || sizeof(path) == 0) return 0;

    array(string) parts = path / ".";
    mixed curr = root;

    foreach (parts, string part) {
        if (!curr) return 0;

        // Check for array index notation: "field[idx]"
        int bracket_idx = search(part, "[");
        if (bracket_idx >= 0 && has_suffix(part, "]")) {
            string field_name = part[0..bracket_idx-1];
            int array_idx;
            sscanf(part[bracket_idx+1..sizeof(part)-2], "%d", array_idx);

            if (sizeof(field_name) > 0) {
                if (!mappingp(curr) || !has_index(curr, field_name)) return 0;
                curr = curr[field_name];
            }

            if (!arrayp(curr) || array_idx < 0 || array_idx >= sizeof(curr)) return 0;
            curr = curr[array_idx];
        } else {
            if (mappingp(curr)) {
                if (!has_index(curr, part)) return 0;
                curr = curr[part];
            } else {
                return 0;
            }
        }
    }

    return curr;
}

//! Check if a given JSONPath exists in the structure.
int has_path(mixed root, string path) {
    return !zero_type(get_value(root, path));
}
