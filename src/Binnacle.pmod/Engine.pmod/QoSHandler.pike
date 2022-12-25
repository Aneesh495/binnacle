// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// QoS 0, 1, and 2 Handshake State Machines and Deduplication Controller

multiset(int) incoming_qos2_pending; // Inbound QoS 2 packets awaiting PUBREL
Thread.Mutex qos_mutex;

void create() {
    incoming_qos2_pending = (<>);
    qos_mutex = Thread.Mutex();
}

//! Process incoming PUBLISH packet from client.
//! Returns ({ int should_dispatch, object ack_packet_to_send }).
array(mixed) handle_inbound_publish(object pub_pkt, int proto_ver) {
    int qos = pub_pkt->qos;
    int pid = pub_pkt->packet_id;

    if (qos == Binnacle.Constants.QOS_0) {
        // QoS 0: Dispatch immediately, no acknowledgment sent
        return ({ 1, 0 });
    }

    if (qos == Binnacle.Constants.QOS_1) {
        // QoS 1: Dispatch immediately and reply with PUBACK
        object ack = Binnacle.Protocol.PubAckPacket(pid, Binnacle.Constants.RC_SUCCESS);
        return ({ 1, ack });
    }

    if (qos == Binnacle.Constants.QOS_2) {
        Thread.MutexKey key = qos_mutex->lock();

        if (incoming_qos2_pending[pid]) {
            // Duplicate PUBLISH while handshake is already in flight
            destruct(key);
            object rec = Binnacle.Protocol.PubRecPacket(pid, Binnacle.Constants.RC_SUCCESS);
            return ({ 0, rec }); // Do not dispatch to subscribers again
        }

        incoming_qos2_pending[pid] = 1;
        destruct(key);

        object rec = Binnacle.Protocol.PubRecPacket(pid, Binnacle.Constants.RC_SUCCESS);
        return ({ 1, rec });
    }

    return ({ 0, 0 });
}

//! Process incoming PUBREL packet from client for inbound QoS 2 flow.
//! Returns PUBCOMP packet to send back to client.
object handle_inbound_pubrel(object rel_pkt, int proto_ver) {
    int pid = rel_pkt->packet_id;

    Thread.MutexKey key = qos_mutex->lock();
    int was_pending = incoming_qos2_pending[pid];
    if (was_pending) {
        incoming_qos2_pending[pid] = 0;
    }
    destruct(key);

    int rc = was_pending ? Binnacle.Constants.RC_SUCCESS : Binnacle.Constants.RC_PACKET_IDENTIFIER_NOT_FOUND;
    return Binnacle.Protocol.PubCompPacket(pid, rc);
}

//! Process incoming PUBACK for outgoing QoS 1 flow.
int handle_outbound_puback(object ack_pkt, object inflight_window, object id_pool) {
    int pid = ack_pkt->packet_id;
    object msg = inflight_window->acknowledge(pid);
    if (msg) {
        id_pool->release(pid);
        return 1;
    }
    return 0;
}

//! Process incoming PUBREC for outgoing QoS 2 flow.
//! Transitions inflight state to PUBREL sent, returns PUBREL packet to send to subscriber.
object handle_outbound_pubrec(object rec_pkt, object inflight_window) {
    int pid = rec_pkt->packet_id;
    object msg = inflight_window->get(pid);

    if (!msg) {
        // Packet ID not found in inflight window, send PUBREL with error or ignore
        return Binnacle.Protocol.PubRelPacket(pid, Binnacle.Constants.RC_PACKET_IDENTIFIER_NOT_FOUND);
    }

    inflight_window->update_state(pid, 1); // State 1: PUBREC received, PUBREL pending
    return Binnacle.Protocol.PubRelPacket(pid, Binnacle.Constants.RC_SUCCESS);
}

//! Process incoming PUBCOMP for outgoing QoS 2 flow.
//! Completes the 4-step QoS 2 handshake and releases packet ID.
int handle_outbound_pubcomp(object comp_pkt, object inflight_window, object id_pool) {
    int pid = comp_pkt->packet_id;
    object msg = inflight_window->acknowledge(pid);
    if (msg) {
        id_pool->release(pid);
        return 1;
    }
    return 0;
}
