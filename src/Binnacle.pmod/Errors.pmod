// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Error Hierarchy and Exception Types

class BinnacleError {
    string message;
    int error_code;
    int reason_code;

    void create(string _msg, int|void _err, int|void _rc) {
        message = _msg;
        error_code = zero_type(_err) ? 1 : _err;
        reason_code = zero_type(_rc) ? 0x80 : _rc;
    }

    string description() {
        return sprintf("[BinnacleError %d] %s (ReasonCode: 0x%02x)",
                       error_code, message, reason_code);
    }
}

class ProtocolError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 100, zero_type(_rc) ? 0x82 : _rc);
    }
}

class MalformedPacketError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 101, zero_type(_rc) ? 0x81 : _rc);
    }
}

class AuthenticationError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 102, zero_type(_rc) ? 0x86 : _rc);
    }
}

class AuthorizationError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 103, zero_type(_rc) ? 0x87 : _rc);
    }
}

class QuotaExceededError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 104, zero_type(_rc) ? 0x97 : _rc);
    }
}

class TopicFilterError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 105, zero_type(_rc) ? 0x8F : _rc);
    }
}

class StorageError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 106, zero_type(_rc) ? 0x80 : _rc);
    }
}

class ClusterError {
    inherit BinnacleError;

    void create(string _msg, int|void _rc) {
        ::create(_msg, 107, zero_type(_rc) ? 0x80 : _rc);
    }
}
