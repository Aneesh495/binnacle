// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Declarative Configuration Parser

//! Represents parsed broker configuration with typed getters.
class Config {
    mapping(string:mixed) values = ([]);

    void set(string key, mixed val) {
        values[key] = val;
    }

    mixed get(string key, mixed|void default_val) {
        if (has_index(values, key)) {
            return values[key];
        }
        return default_val;
    }

    string get_string(string key, string|void default_val) {
        mixed v = get(key, default_val);
        if (stringp(v)) return v;
        if (v == 0) return default_val || "";
        return sprintf("%O", v);
    }

    int get_int(string key, int|void default_val) {
        mixed v = get(key, default_val);
        if (intp(v)) return v;
        if (stringp(v)) {
            int parsed;
            if (sscanf(v, "%d", parsed) == 1) return parsed;
        }
        return default_val || 0;
    }

    int get_bool(string key, int|void default_val) {
        mixed v = get(key, default_val);
        if (intp(v)) return v != 0;
        if (stringp(v)) {
            string s = lower_case(String.trim_all_whites(v));
            if (s == "true" || s == "yes" || s == "1" || s == "on") return 1;
            if (s == "false" || s == "no" || s == "0" || s == "off") return 0;
        }
        return default_val || 0;
    }

    array(string) get_string_list(string key, array(string)|void default_val) {
        mixed v = get(key, default_val);
        if (arrayp(v)) return v;
        if (stringp(v)) {
            array(string) raw = v / ",";
            array(string) clean = ({});
            foreach (raw, string item) {
                string trimmed = String.trim_all_whites(item);
                if (sizeof(trimmed)) clean += ({ trimmed });
            }
            return clean;
        }
        return default_val || ({});
    }

    mapping(string:mixed) to_mapping() {
        return copy_value(values);
    }
}

//! Parse a configuration string into a Config object.
Config parse_string(string content) {
    Config cfg = Config();
    array(string) lines = content / "\n";

    foreach (lines, string line) {
        string trimmed = String.trim_all_whites(line);
        if (sizeof(trimmed) == 0) continue;
        if (has_prefix(trimmed, "#") || has_prefix(trimmed, "//")) continue;

        int eq_pos = search(trimmed, "=");
        if (eq_pos < 0) {
            eq_pos = search(trimmed, ":");
        }
        if (eq_pos < 0) continue;

        string k = String.trim_all_whites(trimmed[0..eq_pos - 1]);
        string v = String.trim_all_whites(trimmed[eq_pos + 1..]);

        // Strip inline comments if not inside quotes
        if (sizeof(v) && v[0] != '"' && v[0] != '\'') {
            int hash_idx = search(v, "#");
            if (hash_idx >= 0) v = String.trim_all_whites(v[0..hash_idx - 1]);
        }

        // Parse quoted string
        if (sizeof(v) >= 2 && ((v[0] == '"' && v[sizeof(v)-1] == '"') || (v[0] == '\'' && v[sizeof(v)-1] == '\''))) {
            cfg->set(k, v[1..sizeof(v)-2]);
        } else if (lower_case(v) == "true" || lower_case(v) == "yes" || lower_case(v) == "on") {
            cfg->set(k, 1);
        } else if (lower_case(v) == "false" || lower_case(v) == "no" || lower_case(v) == "off") {
            cfg->set(k, 0);
        } else {
            // Attempt integer parse
            int int_val;
            if (sscanf(v, "%d", int_val) == 1 && sprintf("%d", int_val) == v) {
                cfg->set(k, int_val);
            } else {
                cfg->set(k, v);
            }
        }
    }

    return cfg;
}

//! Read and parse a configuration file from disk.
Config parse_file(string path) {
    if (!Stdio.exist(path)) {
        Binnacle.Utils.Logger.warn("config", sprintf("Config file %s does not exist, using defaults", path));
        return Config();
    }
    string content = Stdio.read_file(path);
    if (!content) {
        Binnacle.Utils.Logger.error("config", sprintf("Failed to read config file %s", path));
        return Config();
    }
    return parse_string(content);
}
