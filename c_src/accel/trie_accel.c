/*
 * Binnacle: Distributed MQTT 5.0 Telemetry Broker
 * File: c_src/accel/trie_accel.c
 * Description: Fast C topic filter matching engine with MQTT 5.0 wildcard semantics.
 */

#include "binnacle_accel.h"
#include <string.h>

bool binnacle_topic_matches(const char *topic, const char *filter) {
    if (!topic || !filter) {
        return false;
    }

    /* System topics starting with $ cannot match wildcards at root level */
    if (topic[0] == '$' && (filter[0] == '+' || filter[0] == '#')) {
        return false;
    }

    const char *t = topic;
    const char *f = filter;

    while (*t != '\0' && *f != '\0') {
        if (*f == '#') {
            /* '#' must be the last character in the filter */
            return true;
        }

        if (*f == '+') {
            /* Consume topic characters until next '/' or end of string */
            while (*t != '\0' && *t != '/') {
                t++;
            }
            f++;

            /* Both must either reach '/' or both terminate */
            if (*t == '/' && *f == '/') {
                t++;
                f++;
                continue;
            } else if (*t == '\0' && *f == '\0') {
                return true;
            } else if (*t == '\0' && *f == '/' && *(f + 1) == '#') {
                /* e.g. topic "sport" matching "sport/#" is false, but "sport/" matching "sport/+" */
                return false;
            } else {
                return false;
            }
        }

        if (*t != *f) {
            return false;
        }

        t++;
        f++;
    }

    if (*t == '\0' && *f == '\0') {
        return true;
    }

    /* Edge case: filter ends with "/#" while topic is without trailing slash */
    if (*t == '\0' && strcmp(f, "/#") == 0) {
        return false;
    }

    if (*f == '#' && *(f + 1) == '\0') {
        return true;
    }

    return false;
}
