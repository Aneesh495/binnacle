// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Core Module Definition

#define BINNACLE_VERSION "1.0.0"
#define BINNACLE_RELEASE_NAME "Centurion"

//! Return the human-readable release version of Binnacle.
string version() {
    return BINNACLE_VERSION;
}

//! Return the code release name.
string release_name() {
    return BINNACLE_RELEASE_NAME;
}

//! Return system banner string.
string banner() {
    return sprintf("Binnacle Telemetry Broker v%s (%s) [Pike %s]",
                   BINNACLE_VERSION, BINNACLE_RELEASE_NAME, version());
}
