// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Structured High-Throughput Thread-Safe Logger

constant LEVEL_DEBUG = 0;
constant LEVEL_INFO  = 1;
constant LEVEL_WARN  = 2;
constant LEVEL_ERROR = 3;
constant LEVEL_FATAL = 4;

private int current_level = LEVEL_INFO;
private string output_format = "text"; // "text" or "json"
private Thread.Mutex log_mutex = Thread.Mutex();
private Stdio.File log_file = 0;

//! Initialize logger with target level, format, and optional log file path.
void init(int level, string format, string|void path) {
    current_level = level;
    output_format = format;
    if (path && sizeof(path)) {
        log_file = Stdio.File();
        if (!log_file->open(path, "cwa")) {
            werror("Failed to open log file %s, falling back to stderr.\n", path);
            log_file = 0;
        }
    }
}

//! Parse string log level into integer constant.
int parse_level(string lvl) {
    switch (lower_case(lvl)) {
        case "debug": return LEVEL_DEBUG;
        case "info":  return LEVEL_INFO;
        case "warn":
        case "warning": return LEVEL_WARN;
        case "error": return LEVEL_ERROR;
        case "fatal": return LEVEL_FATAL;
        default:      return LEVEL_INFO;
    }
}

//! Convert integer level constant to uppercase string.
string level_to_string(int lvl) {
    switch (lvl) {
        case LEVEL_DEBUG: return "DEBUG";
        case LEVEL_INFO:  return "INFO";
        case LEVEL_WARN:  return "WARN";
        case LEVEL_ERROR: return "ERROR";
        case LEVEL_FATAL: return "FATAL";
        default:          return "INFO";
    }
}

//! Format current timestamp in ISO 8601 UTC format.
private string get_timestamp() {
    int now = time();
    mapping tm = gmtime(now);
    return sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ",
                   tm->year + 1900, tm->mon + 1, tm->mday,
                   tm->hour, tm->min, tm->sec);
}

//! Core emission routine.
void log(int level, string subsystem, string msg, mapping(string:mixed)|void ctx) {
    if (level < current_level) return;

    string ts = get_timestamp();
    string lvl_str = level_to_string(level);
    string formatted;

    if (output_format == "json") {
        string ctx_str = "";
        if (ctx && sizeof(ctx)) {
            array(string) parts = ({});
            foreach (sort(indices(ctx)), string k) {
                parts += ({ sprintf("\"%s\": \"%O\"", k, ctx[k]) });
            }
            ctx_str = ", \"context\": {" + (parts * ", ") + "}";
        }
        formatted = sprintf("{\"timestamp\":\"%s\",\"level\":\"%s\",\"subsystem\":\"%s\",\"message\":\"%s\"%s}\n",
                            ts, lvl_str, subsystem, msg, ctx_str);
    } else {
        string ctx_suffix = "";
        if (ctx && sizeof(ctx)) {
            array(string) parts = ({});
            foreach (sort(indices(ctx)), string k) {
                parts += ({ sprintf("%s=%O", k, ctx[k]) });
            }
            ctx_suffix = " [" + (parts * " ") + "]";
        }
        formatted = sprintf("[%s] [%-5s] [%s] %s%s\n",
                            ts, lvl_str, subsystem, msg, ctx_suffix);
    }

    Thread.MutexKey key = log_mutex->lock();
    if (log_file) {
        log_file->write(formatted);
    } else {
        if (level >= LEVEL_ERROR) {
            werror("%s", formatted);
        } else {
            write("%s", formatted);
        }
    }
    destruct(key);
}

void debug(string subsystem, string msg, mapping(string:mixed)|void ctx) {
    log(LEVEL_DEBUG, subsystem, msg, ctx);
}

void info(string subsystem, string msg, mapping(string:mixed)|void ctx) {
    log(LEVEL_INFO, subsystem, msg, ctx);
}

void warn(string subsystem, string msg, mapping(string:mixed)|void ctx) {
    log(LEVEL_WARN, subsystem, msg, ctx);
}

void error(string subsystem, string msg, mapping(string:mixed)|void ctx) {
    log(LEVEL_ERROR, subsystem, msg, ctx);
}

void fatal(string subsystem, string msg, mapping(string:mixed)|void ctx) {
    log(LEVEL_FATAL, subsystem, msg, ctx);
}
