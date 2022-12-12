// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Topic Alias Bidirectional Mapping Table (OASIS MQTT 5.0 Section 3.3.2.3.8)

int max_inbound_alias;
int max_outbound_alias;
mapping(int:string) inbound_aliases;  // alias -> topic (Client to Broker)
mapping(string:int) outbound_topics;  // topic -> alias (Broker to Client)
mapping(int:string) outbound_aliases; // alias -> topic
int next_outbound_alias;

void create(int|void _max_in, int|void _max_out) {
    max_inbound_alias = zero_type(_max_in) ? Binnacle.Constants.DEFAULT_TOPIC_ALIAS_MAXIMUM : _max_in;
    max_outbound_alias = zero_type(_max_out) ? 0 : _max_out;
    inbound_aliases = ([]);
    outbound_topics = ([]);
    outbound_aliases = ([]);
    next_outbound_alias = 1;
}

//! Register or update an inbound topic alias from client.
//! Throws MalformedPacketError if alias is 0 or exceeds max_inbound_alias.
void register_inbound_alias(int alias, string topic) {
    if (alias == 0 || alias > max_inbound_alias) {
        throw(Binnacle.Errors.ProtocolError(
            sprintf("Topic alias %d out of range (max %d)", alias, max_inbound_alias),
            Binnacle.Constants.RC_TOPIC_ALIAS_INVALID
        ));
    }
    inbound_aliases[alias] = topic;
}

//! Resolve an inbound topic alias.
//! Throws ProtocolError if alias is not found.
string resolve_inbound_alias(int alias) {
    if (alias == 0 || !has_index(inbound_aliases, alias)) {
        throw(Binnacle.Errors.ProtocolError(
            sprintf("Unknown topic alias: %d", alias),
            Binnacle.Constants.RC_TOPIC_ALIAS_INVALID
        ));
    }
    return inbound_aliases[alias];
}

//! Lookup or allocate an outbound topic alias for outgoing publish to client.
//! Returns ({ int alias, int is_new }) or 0 if outbound aliases disabled.
array(int)|int get_or_create_outbound_alias(string topic) {
    if (max_outbound_alias <= 0) return 0;

    if (has_index(outbound_topics, topic)) {
        return ({ outbound_topics[topic], 0 });
    }

    if (next_outbound_alias <= max_outbound_alias) {
        int alias = next_outbound_alias++;
        outbound_topics[topic] = alias;
        outbound_aliases[alias] = topic;
        return ({ alias, 1 }); // New alias registered
    }

    return 0; // Alias capacity full
}

//! Clear all registered alias mappings on connection reset.
void clear() {
    inbound_aliases = ([]);
    outbound_topics = ([]);
    outbound_aliases = ([]);
    next_outbound_alias = 1;
}
