// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Client Subscription Entity and QoS Resolution Rules

string client_id;
string topic_filter;
int qos;
int no_local;
int retain_as_published;
int retain_handling;
int subscription_identifier;
object connection_actor; // Reference to active connection actor

void create(string _cid, string _filter, int _qos,
            int|void _nl, int|void _rap, int|void _rh,
            int|void _sub_id, object|void _actor) {
    client_id = _cid;
    topic_filter = _filter;
    qos = _qos & 0x03;
    no_local = zero_type(_nl) ? 0 : (_nl ? 1 : 0);
    retain_as_published = zero_type(_rap) ? 0 : (_rap ? 1 : 0);
    retain_handling = zero_type(_rh) ? 0 : (_rh & 0x03);
    subscription_identifier = zero_type(_sub_id) ? 0 : _sub_id;
    connection_actor = _actor || 0;
}

//! Compute effective QoS given the message publication QoS and subscription QoS.
//! Effective QoS is min(pub_qos, sub_qos).
int calculate_effective_qos(int pub_qos) {
    return (pub_qos < qos) ? pub_qos : qos;
}

//! Check if subscriber should be suppressed by NoLocal policy.
int is_suppressed_by_no_local(string publisher_client_id) {
    return no_local && (client_id == publisher_client_id);
}
