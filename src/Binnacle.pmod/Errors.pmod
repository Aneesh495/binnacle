// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Error Hierarchy and Exception Types

class BinnacleError {
    string message;
    int error_code;
    int reason_code;

    void create(string _msg, int _err = 1, int _rc = 0x80) {
        message = _msg;
        error_code = _err;
        reason_code = _rc;
    }

    string description() {
        return sprintf("[BinnacleError %d] %s (ReasonCode: 0x%02x)",
                       error_code, message, reason_code);
    }
}

class ProtocolError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x82) {
        ::create(_msg, 100, _rc);
    }
}

class MalformedPacketError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x81) {
        ::create(_msg, 101, _rc);
    }
}

class AuthenticationError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x86) {
        ::create(_msg, 102, _rc);
    }
}

class AuthorizationError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x87) {
        ::create(_msg, 103, _rc);
    }
}

class QuotaExceededError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x97) {
        ::create(_msg, 104, _rc);
    }
}

class TopicFilterError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x8F) {
        ::create(_msg, 105, _rc);
    }
}

class StorageError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x80) {
        ::create(_msg, 106, _rc);
    }
}

class ClusterError {
    inherit BinnacleError;

    void create(string _msg, int _rc = 0x80) {
        ::create(_msg, 107, _rc);
    }
}
