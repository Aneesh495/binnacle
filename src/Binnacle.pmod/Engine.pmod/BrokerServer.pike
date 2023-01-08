// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Central Broker Server and Engine Orchestrator

object config;
object topic_trie;
object subscription_table;
object retained_store;
object session_manager;
object will_manager;
object auth_provider;
object sys_topics;
object storage_engine;
object rule_pipeline;
object cluster_mesh;
object metrics_registry;

multiset(object) active_actors;
int messages_received_count;
int messages_sent_count;
int bytes_received_count;
int bytes_sent_count;
mixed ticker_callout_id;
Thread.Mutex server_mutex;

void create(object|void _cfg) {
    config = _cfg || Binnacle.Utils.ConfigParser.Config();
    topic_trie = Binnacle.Router.TopicTrie();
    subscription_table = Binnacle.Router.SubscriptionTable();
    retained_store = Binnacle.Router.RetainedStore();
    session_manager = Binnacle.Session.SessionManager(this);
    will_manager = Binnacle.Session.WillManager(this);
    auth_provider = .AuthProvider(config->get_bool("allow_anonymous", 1));
    sys_topics = Binnacle.Router.SystemTopicProvider(this, 10);
    storage_engine = 0;
    rule_pipeline = 0;
    cluster_mesh = 0;
    metrics_registry = 0;

    active_actors = (<>);
    messages_received_count = 0;
    messages_sent_count = 0;
    bytes_received_count = 0;
    bytes_sent_count = 0;
    server_mutex = Thread.Mutex();
    ticker_callout_id = 0;
}

//! Start the broker engine and background maintenance tasks.
void start() {
    Binnacle.Utils.Logger.info("broker", sprintf("Starting %s", Binnacle.banner()));
    sys_topics->start();
    _schedule_ticker();
}

//! Stop the broker engine.
void stop() {
    sys_topics->stop();
    if (ticker_callout_id) {
        remove_call_out(ticker_callout_id);
        ticker_callout_id = 0;
    }
    Thread.MutexKey key = server_mutex->lock();
    foreach (indices(active_actors), object actor) {
        actor->disconnect(Binnacle.Constants.RC_SERVER_SHUTTING_DOWN, "Server shutting down");
    }
    active_actors = (<>);
    destruct(key);
}

private void _schedule_ticker() {
    ticker_callout_id = call_out(_on_tick, 1);
}

private void _on_tick() {
    int now = time();

    // Check client keep-alives
    Thread.MutexKey key = server_mutex->lock();
    array(object) actors_snapshot = indices(active_actors);
    destruct(key);

    foreach (actors_snapshot, object actor) {
        actor->check_keep_alive(now);
    }

    // Check delayed wills and expired sessions
    will_manager->check_delayed_wills(now);
    session_manager->reap_expired_sessions(now);

    _schedule_ticker();
}

//! Register a new active client connection actor.
void register_actor(object actor) {
    Thread.MutexKey key = server_mutex->lock();
    active_actors[actor] = 1;
    destruct(key);
}

//! Unregister a disconnected client connection actor.
void unregister_actor(object actor) {
    Thread.MutexKey key = server_mutex->lock();
    if (active_actors[actor]) {
        active_actors[actor] = 0;
    }
    destruct(key);
}

// ---------------------------------------------------------------------------
// Protocol Handlers
// ---------------------------------------------------------------------------

//! Handle incoming CONNECT packet.
void handle_connect(object actor, object cp) {
    int ver = cp->protocol_version;
    actor->protocol_version = ver;

    // Validate protocol version
    if (ver != Binnacle.Constants.PROTOCOL_LEVEL_3_1_1 && ver != Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        actor->send_packet(Binnacle.Protocol.ConnAckPacket(0, Binnacle.Constants.RC_UNSUPPORTED_PROTOCOL_VERSION));
        actor->disconnect(Binnacle.Constants.RC_UNSUPPORTED_PROTOCOL_VERSION, "Unsupported protocol version");
        return;
    }

    string cid = cp->client_id;
    mapping(int:mixed) connack_props = ([]);

    // Auto-generate Client ID if empty
    if (!cid || sizeof(cid) == 0) {
        if (!cp->clean_start && ver == Binnacle.Constants.PROTOCOL_LEVEL_3_1_1) {
            actor->send_packet(Binnacle.Protocol.ConnAckPacket(0, Binnacle.Constants.RC_CLIENT_IDENTIFIER_NOT_VALID));
            actor->disconnect(Binnacle.Constants.RC_CLIENT_IDENTIFIER_NOT_VALID, "Client ID must not be empty with clean_session=0");
            return;
        }
        cid = sprintf("binnacle-%08x-%04x", time(), random(65535));
        cp->client_id = cid;
        if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
            connack_props[Binnacle.Constants.PROP_ASSIGNED_CLIENT_IDENTIFIER] = cid;
        }
    }

    actor->client_id = cid;
    actor->keep_alive = cp->keep_alive;

    // Authenticate
    int auth_rc = auth_provider->authenticate(cid, cp->username, cp->password);
    if (auth_rc != Binnacle.Constants.RC_SUCCESS) {
        actor->send_packet(Binnacle.Protocol.ConnAckPacket(0, auth_rc));
        actor->disconnect(auth_rc, "Authentication failed");
        return;
    }

    // Session Expiry Interval
    int expiry = config->get_int("session_expiry_interval", Binnacle.Constants.DEFAULT_SESSION_EXPIRY);
    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && has_index(cp->properties, Binnacle.Constants.PROP_SESSION_EXPIRY_INTERVAL)) {
        expiry = cp->properties[Binnacle.Constants.PROP_SESSION_EXPIRY_INTERVAL];
    } else if (ver == Binnacle.Constants.PROTOCOL_LEVEL_3_1_1) {
        expiry = cp->clean_start ? 0 : 0xFFFFFFFF;
    }

    // Inflight receive maximum
    int client_rx_max = Binnacle.Constants.DEFAULT_RECEIVE_MAXIMUM;
    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && has_index(cp->properties, Binnacle.Constants.PROP_RECEIVE_MAXIMUM)) {
        client_rx_max = cp->properties[Binnacle.Constants.PROP_RECEIVE_MAXIMUM];
    }

    // Fetch or create session
    array(mixed) sess_res = session_manager->get_or_create_session(cid, cp->clean_start, expiry, client_rx_max);
    object sess = sess_res[0];
    int session_present = sess_res[1];

    sess->attach_connection(actor);
    actor->session = sess;

    // Register Last Will if provided
    if (cp->will_flag) {
        int delay = 0;
        if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && has_index(cp->will_properties, Binnacle.Constants.PROP_WILL_DELAY_INTERVAL)) {
            delay = cp->will_properties[Binnacle.Constants.PROP_WILL_DELAY_INTERVAL];
        }
        object will = Binnacle.Types.WillMessage(cp->will_topic, cp->will_payload, cp->will_qos, cp->will_retain, delay, cp->will_properties);
        will_manager->register_will(cid, will);
    } else {
        will_manager->on_client_reconnected(cid);
    }

    // Configure MQTT 5.0 CONNACK properties
    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        connack_props[Binnacle.Constants.PROP_MAXIMUM_QOS] = config->get_int("maximum_qos", 2);
        connack_props[Binnacle.Constants.PROP_RETAIN_AVAILABLE] = config->get_bool("retain_available", 1);
        connack_props[Binnacle.Constants.PROP_WILDCARD_SUBSCRIPTION_AVAILABLE] = config->get_bool("wildcard_subscription_available", 1);
        connack_props[Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE] = config->get_bool("subscription_identifiers_available", 1);
        connack_props[Binnacle.Constants.PROP_SHARED_SUBSCRIPTION_AVAILABLE] = config->get_bool("shared_subscription_available", 1);
        connack_props[Binnacle.Constants.PROP_RECEIVE_MAXIMUM] = config->get_int("receive_maximum", Binnacle.Constants.DEFAULT_RECEIVE_MAXIMUM);
        connack_props[Binnacle.Constants.PROP_TOPIC_ALIAS_MAXIMUM] = config->get_int("topic_alias_maximum", Binnacle.Constants.DEFAULT_TOPIC_ALIAS_MAXIMUM);
        connack_props[Binnacle.Constants.PROP_SERVER_KEEP_ALIVE] = actor->keep_alive;
    }

    actor->is_connected = 1;
    register_actor(actor);

    // Send CONNACK
    actor->send_packet(Binnacle.Protocol.ConnAckPacket(session_present, Binnacle.Constants.RC_SUCCESS, connack_props));
    Binnacle.Utils.Logger.info("broker", sprintf("Client %s connected successfully (v%d, clean=%d, present=%d)",
                               cid, ver, cp->clean_start, session_present));

    // Drain offline queue
    sess->drain_offline_queue();
}

//! Handle incoming PUBLISH packet.
void handle_publish(object actor, object pub_pkt) {
    string topic = pub_pkt->topic;
    int ver = actor->protocol_version;

    // Topic alias resolution (MQTT 5.0)
    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        if (has_index(pub_pkt->properties, Binnacle.Constants.PROP_TOPIC_ALIAS)) {
            int alias = pub_pkt->properties[Binnacle.Constants.PROP_TOPIC_ALIAS];
            if (sizeof(topic) > 0) {
                // Register/update alias
                catch {
                    actor->topic_alias_map->register_inbound_alias(alias, topic);
                };
            } else {
                // Resolve cached alias
                catch {
                    topic = actor->topic_alias_map->resolve_inbound_alias(alias);
                    pub_pkt->topic = topic;
                };
            }
        }
    }

    if (!topic || sizeof(topic) == 0 || !Binnacle.Utils.StringUtil.is_valid_topic_name(topic)) {
        actor->disconnect(Binnacle.Constants.RC_TOPIC_NAME_INVALID, "Malformed topic name");
        return;
    }

    // ACL check
    if (!auth_provider->can_publish(actor->client_id, "", topic)) {
        if (pub_pkt->qos == Binnacle.Constants.QOS_1) {
            actor->send_packet(Binnacle.Protocol.PubAckPacket(pub_pkt->packet_id, Binnacle.Constants.RC_NOT_AUTHORIZED));
        } else if (pub_pkt->qos == Binnacle.Constants.QOS_2) {
            actor->send_packet(Binnacle.Protocol.PubRecPacket(pub_pkt->packet_id, Binnacle.Constants.RC_NOT_AUTHORIZED));
        }
        return;
    }

    // QoS handshake
    array(mixed) qos_res = actor->qos_handler->handle_inbound_publish(pub_pkt, ver);
    int should_dispatch = qos_res[0];
    object ack_pkt = qos_res[1];

    if (ack_pkt) {
        actor->send_packet(ack_pkt);
    }

    if (!should_dispatch) return;

    // Retained message update
    if (pub_pkt->retain) {
        retained_store->put(topic, pub_pkt->payload, pub_pkt->qos, pub_pkt->properties);
        if (storage_engine) {
            storage_engine->append_retained(topic, pub_pkt->payload, pub_pkt->qos, pub_pkt->properties);
        }
    }

    // Stream rule engine evaluation
    if (rule_pipeline) {
        rule_pipeline->process_message(topic, pub_pkt->payload, pub_pkt->properties);
    }

    // Dispatch to subscribers
    _dispatch_to_subscribers(pub_pkt, actor->client_id);
}

//! Internal message dispatch into broker routing tree.
void internal_publish(string topic, string payload, int qos, int retain, mapping(int:mixed)|void props) {
    object pub = Binnacle.Protocol.PublishPacket(topic, payload, qos, retain, 0, 0, props);
    if (retain) {
        retained_store->put(topic, payload, qos, props);
    }
    _dispatch_to_subscribers(pub, "$broker");
}

//! Route published message to all matching subscribers.
private void _dispatch_to_subscribers(object pub_pkt, string publisher_cid) {
    array(object) matched = topic_trie->match(pub_pkt->topic, publisher_cid);

    foreach (matched, object sub) {
        object sess = session_manager->get_session(sub->client_id);
        if (!sess) continue;

        int eff_qos = sub->calculate_effective_qos(pub_pkt->qos);

        // Construct outgoing publish packet
        mapping(int:mixed) out_props = copy_value(pub_pkt->properties);
        if (sub->subscription_identifier > 0) {
            out_props[Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER] = sub->subscription_identifier;
        }

        int out_retain = sub->retain_as_published ? pub_pkt->retain : 0;
        object out_pkt = Binnacle.Protocol.PublishPacket(
            pub_pkt->topic, pub_pkt->payload, eff_qos, out_retain, 0, 0, out_props
        );

        sess->dispatch_or_queue(out_pkt);
    }
}

//! Handle incoming SUBSCRIBE packet.
void handle_subscribe(object actor, object sub_pkt) {
    int pid = sub_pkt->packet_id;
    int ver = actor->protocol_version;
    array(int) reason_codes = ({});
    object sess = actor->session;

    int sub_id = 0;
    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && has_index(sub_pkt->properties, Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER)) {
        sub_id = sub_pkt->properties[Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER];
    }

    foreach (sub_pkt->subscriptions, array sub_item) {
        string filter = sub_item[0];
        int opt_byte = sub_item[1];

        if (!Binnacle.Utils.StringUtil.is_valid_topic_filter(filter)) {
            reason_codes += ({ Binnacle.Constants.RC_TOPIC_FILTER_INVALID });
            continue;
        }

        if (!auth_provider->can_subscribe(actor->client_id, "", filter)) {
            reason_codes += ({ Binnacle.Constants.RC_NOT_AUTHORIZED });
            continue;
        }

        int req_qos = opt_byte & 0x03;
        int no_local = (opt_byte & 0x04) ? 1 : 0;
        int rap = (opt_byte & 0x08) ? 1 : 0;
        int rh = (opt_byte >> 4) & 0x03;

        int is_new_sub = !has_index(sess->subscriptions, filter);

        object sub = Binnacle.Router.Subscription(
            actor->client_id, filter, req_qos, no_local, rap, rh, sub_id, actor
        );

        topic_trie->subscribe(sub);
        subscription_table->add(sub);
        sess->subscriptions[filter] = sub;

        reason_codes += ({ req_qos }); // Granted QoS

        // Retain handling
        int should_send_retained = 0;
        if (rh == Binnacle.Constants.RETAIN_SEND_ON_SUBSCRIBE) {
            should_send_retained = 1;
        } else if (rh == Binnacle.Constants.RETAIN_SEND_IF_SUBSCRIPTION_NEW && is_new_sub) {
            should_send_retained = 1;
        }

        if (should_send_retained) {
            array(object) retained = retained_store->match(filter);
            foreach (retained, object r_msg) {
                int r_eff_qos = sub->calculate_effective_qos(r_msg->qos);
                mapping(int:mixed) r_props = copy_value(r_msg->properties);
                if (sub_id > 0) {
                    r_props[Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER] = sub_id;
                }
                object out_r = Binnacle.Protocol.PublishPacket(
                    r_msg->topic, r_msg->payload, r_eff_qos, 1, 0, 0, r_props
                );
                sess->dispatch_or_queue(out_r);
            }
        }
    }

    actor->send_packet(Binnacle.Protocol.SubAckPacket(pid, reason_codes));
}

//! Handle incoming UNSUBSCRIBE packet.
void handle_unsubscribe(object actor, object unsub_pkt) {
    int pid = unsub_pkt->packet_id;
    int ver = actor->protocol_version;
    array(int) reason_codes = ({});
    object sess = actor->session;

    foreach (unsub_pkt->topic_filters, string filter) {
        int removed = topic_trie->unsubscribe(actor->client_id, filter);
        subscription_table->remove(actor->client_id, filter);
        if (sess && has_index(sess->subscriptions, filter)) {
            m_delete(sess->subscriptions, filter);
        }

        if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
            reason_codes += ({ removed ? Binnacle.Constants.RC_SUCCESS : Binnacle.Constants.RC_NO_SUBSCRIPTION_EXISTED });
        }
    }

    actor->send_packet(Binnacle.Protocol.UnsubAckPacket(pid, reason_codes));
}

//! Handle client disconnection.
void handle_client_disconnected(object actor, int unexpected) {
    unregister_actor(actor);
    string cid = actor->client_id;
    if (cid && sizeof(cid)) {
        if (actor->session) {
            actor->session->detach_connection();
        }
        will_manager->on_client_disconnected(cid, unexpected);
        Binnacle.Utils.Logger.info("broker", sprintf("Client %s disconnected (unexpected=%d)", cid, unexpected));
    }
}

// ---------------------------------------------------------------------------
// Telemetry Metrics Getters
// ---------------------------------------------------------------------------

void record_message_received() { messages_received_count++; }
void record_message_sent()     { messages_sent_count++; }
void record_bytes_received(int b) { bytes_received_count += b; }
void record_bytes_sent(int b)     { bytes_sent_count += b; }

int get_connected_clients_count() { return sizeof(active_actors); }
int get_total_clients_count()     { return session_manager->total_count(); }
int get_subscriptions_count()     { return subscription_table->total_subscriptions(); }
int get_retained_count()          { return retained_store->count(); }
int get_messages_received_count() { return messages_received_count; }
int get_messages_sent_count()     { return messages_sent_count; }
int get_bytes_received_count()    { return bytes_received_count; }
int get_bytes_sent_count()        { return bytes_sent_count; }
