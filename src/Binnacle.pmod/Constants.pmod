// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Protocol Constants and Reason Codes

// Protocol Levels
constant PROTOCOL_LEVEL_3_1_1 = 4;
constant PROTOCOL_LEVEL_5_0   = 5;
constant PROTOCOL_NAME_MQTT   = "MQTT";

// MQTT Fixed Header Packet Types
constant PACKET_CONNECT     = 1;
constant PACKET_CONNACK     = 2;
constant PACKET_PUBLISH     = 3;
constant PACKET_PUBACK      = 4;
constant PACKET_PUBREC      = 5;
constant PACKET_PUBREL      = 6;
constant PACKET_PUBCOMP     = 7;
constant PACKET_SUBSCRIBE   = 8;
constant PACKET_SUBACK      = 9;
constant PACKET_UNSUBSCRIBE = 10;
constant PACKET_UNSUBACK    = 11;
constant PACKET_PINGREQ     = 12;
constant PACKET_PINGRESP    = 13;
constant PACKET_DISCONNECT  = 14;
constant PACKET_AUTH        = 15;

// Quality of Service Levels
constant QOS_0 = 0; // At most once delivery
constant QOS_1 = 1; // At least once delivery
constant QOS_2 = 2; // Exactly once delivery

// Connect Flags Bitmasks
constant CONNECT_FLAG_RESERVED      = 0x01;
constant CONNECT_FLAG_CLEAN_START   = 0x02;
constant CONNECT_FLAG_WILL          = 0x04;
constant CONNECT_FLAG_WILL_QOS_1    = 0x08;
constant CONNECT_FLAG_WILL_QOS_2    = 0x10;
constant CONNECT_FLAG_WILL_RETAIN   = 0x20;
constant CONNECT_FLAG_PASSWORD      = 0x40;
constant CONNECT_FLAG_USERNAME      = 0x80;

// Subscribe Options Bitmasks
constant SUB_OPT_QOS_MASK           = 0x03;
constant SUB_OPT_NO_LOCAL           = 0x04;
constant SUB_OPT_RETAIN_AS_PUBLISHED= 0x08;
constant SUB_OPT_RETAIN_HANDLING    = 0x30;

// Retain Handling Directive Values
constant RETAIN_SEND_ON_SUBSCRIBE            = 0;
constant RETAIN_SEND_IF_SUBSCRIPTION_NEW    = 1;
constant RETAIN_DO_NOT_SEND                  = 2;

// MQTT 5.0 Reason Codes
constant RC_SUCCESS                         = 0x00;
constant RC_NORMAL_DISCONNECTION            = 0x00;
constant RC_GRANTED_QOS_0                   = 0x00;
constant RC_GRANTED_QOS_1                   = 0x01;
constant RC_GRANTED_QOS_2                   = 0x02;
constant RC_DISCONNECT_WITH_WILL            = 0x04;
constant RC_NO_MATCHING_SUBSCRIBERS         = 0x10;
constant RC_NO_SUBSCRIPTION_EXISTED         = 0x11;
constant RC_CONTINUE_AUTHENTICATION         = 0x18;
constant RC_RE_AUTHENTICATE                 = 0x19;
constant RC_UNSPECIFIED_ERROR               = 0x80;
constant RC_MALFORMED_PACKET                = 0x81;
constant RC_PROTOCOL_ERROR                  = 0x82;
constant RC_IMPLEMENTATION_SPECIFIC_ERROR   = 0x83;
constant RC_UNSUPPORTED_PROTOCOL_VERSION    = 0x84;
constant RC_CLIENT_IDENTIFIER_NOT_VALID     = 0x85;
constant RC_BAD_USER_NAME_OR_PASSWORD       = 0x86;
constant RC_NOT_AUTHORIZED                  = 0x87;
constant RC_SERVER_UNAVAILABLE              = 0x88;
constant RC_SERVER_BUSY                     = 0x89;
constant RC_BANNED                          = 0x8A;
constant RC_SERVER_SHUTTING_DOWN            = 0x8B;
constant RC_BAD_AUTHENTICATION_METHOD       = 0x8C;
constant RC_KEEP_ALIVE_TIMEOUT              = 0x8D;
constant RC_SESSION_TAKEN_OVER              = 0x8E;
constant RC_TOPIC_FILTER_INVALID            = 0x8F;
constant RC_TOPIC_NAME_INVALID              = 0x90;
constant RC_PACKET_IDENTIFIER_IN_USE        = 0x91;
constant RC_PACKET_IDENTIFIER_NOT_FOUND     = 0x92;
constant RC_RECEIVE_MAXIMUM_EXCEEDED        = 0x93;
constant RC_TOPIC_ALIAS_INVALID             = 0x94;
constant RC_PACKET_TOO_LARGE                = 0x95;
constant RC_MESSAGE_RATE_TOO_HIGH           = 0x96;
constant RC_QUOTA_EXCEEDED                  = 0x97;
constant RC_ADMINISTRATIVE_ACTION           = 0x98;
constant RC_PAYLOAD_FORMAT_INVALID          = 0x99;
constant RC_RETAIN_NOT_SUPPORTED            = 0x9A;
constant RC_QOS_NOT_SUPPORTED               = 0x9B;
constant RC_USE_ANOTHER_SERVER              = 0x9C;
constant RC_SERVER_MOVED                    = 0x9D;
constant RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 0x9E;
constant RC_CONNECTION_RATE_EXCEEDED        = 0x9F;
constant RC_MAXIMUM_CONNECT_TIME            = 0xA0;
constant RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 0xA1;
constant RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED   = 0xA2;

// MQTT 5.0 Property Identifiers
constant PROP_PAYLOAD_FORMAT_INDICATOR          = 0x01;
constant PROP_MESSAGE_EXPIRY_INTERVAL           = 0x02;
constant PROP_CONTENT_TYPE                      = 0x03;
constant PROP_RESPONSE_TOPIC                    = 0x08;
constant PROP_CORRELATION_DATA                  = 0x09;
constant PROP_SUBSCRIPTION_IDENTIFIER           = 0x0B;
constant PROP_SESSION_EXPIRY_INTERVAL           = 0x11;
constant PROP_ASSIGNED_CLIENT_IDENTIFIER        = 0x12;
constant PROP_SERVER_KEEP_ALIVE                 = 0x13;
constant PROP_AUTHENTICATION_METHOD             = 0x15;
constant PROP_AUTHENTICATION_DATA               = 0x16;
constant PROP_REQUEST_PROBLEM_INFORMATION       = 0x17;
constant PROP_WILL_DELAY_INTERVAL               = 0x18;
constant PROP_REQUEST_RESPONSE_INFORMATION      = 0x19;
constant PROP_RESPONSE_INFORMATION              = 0x1A;
constant PROP_SERVER_REFERENCE                  = 0x1C;
constant PROP_REASON_STRING                     = 0x1F;
constant PROP_RECEIVE_MAXIMUM                   = 0x21;
constant PROP_TOPIC_ALIAS_MAXIMUM               = 0x22;
constant PROP_TOPIC_ALIAS                       = 0x23;
constant PROP_MAXIMUM_QOS                       = 0x24;
constant PROP_RETAIN_AVAILABLE                  = 0x25;
constant PROP_USER_PROPERTY                     = 0x26;
constant PROP_MAXIMUM_PACKET_SIZE               = 0x27;
constant PROP_WILDCARD_SUBSCRIPTION_AVAILABLE   = 0x28;
constant PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0x29;
constant PROP_SHARED_SUBSCRIPTION_AVAILABLE     = 0x2A;

// Default System Limits and Sizing Constants
constant DEFAULT_PORT                           = 1883;
constant DEFAULT_WEBSOCKET_PORT                 = 8083;
constant DEFAULT_HTTP_PORT                      = 8080;
constant DEFAULT_TLS_PORT                       = 8883;
constant DEFAULT_MAX_PACKET_SIZE                = 10485760; // 10 MB
constant DEFAULT_RECEIVE_MAXIMUM                = 1024;
constant DEFAULT_TOPIC_ALIAS_MAXIMUM            = 64;
constant DEFAULT_KEEP_ALIVE                     = 60;
constant DEFAULT_SESSION_EXPIRY                 = 7200;
constant DEFAULT_MAX_OFFLINE_MESSAGES           = 10000;
constant MAX_VARINT_VALUE                       = 268435455; // 256 MB minus 1 byte
constant MAX_PACKET_ID                          = 65535;

// Packet Type Name Mapper
string packet_type_name(int type) {
    switch (type) {
        case PACKET_CONNECT:     return "CONNECT";
        case PACKET_CONNACK:     return "CONNACK";
        case PACKET_PUBLISH:     return "PUBLISH";
        case PACKET_PUBACK:      return "PUBACK";
        case PACKET_PUBREC:      return "PUBREC";
        case PACKET_PUBREL:      return "PUBREL";
        case PACKET_PUBCOMP:     return "PUBCOMP";
        case PACKET_SUBSCRIBE:   return "SUBSCRIBE";
        case PACKET_SUBACK:      return "SUBACK";
        case PACKET_UNSUBSCRIBE: return "UNSUBSCRIBE";
        case PACKET_UNSUBACK:    return "UNSUBACK";
        case PACKET_PINGREQ:     return "PINGREQ";
        case PACKET_PINGRESP:    return "PINGRESP";
        case PACKET_DISCONNECT:  return "DISCONNECT";
        case PACKET_AUTH:        return "AUTH";
        default:                 return sprintf("UNKNOWN(%d)", type);
    }
}
