/**
 * @file stream_query_client.h
 * @brief Routine for C-SPARQL executor thread
 */

#pragma once

#include <vector>
#include "stream_basic_types.h"
#include "stream_coordinator.h"
#include "csparql_manager.h"

class stream_query_client{
 private:
    thread_cfg* cfg;
    stream_coordinator* coordinator;

    void send(request_or_reply& r);
    request_or_reply recv();
    int stream_query_execution(request_or_reply& r);
    int throughput_mode(request_or_reply& r);
    void local_metadata_lookup(request_or_reply& r);

 public:
    stream_query_client(thread_cfg* _cfg, stream_coordinator* _coordinator):
        cfg(_cfg),
        coordinator(_coordinator) {}

    void master();

    /**
     * @depricated slave
     */
    /* void slave(); */
};

extern stream_query_client **stream_query_masters, **stream_query_slaves;
