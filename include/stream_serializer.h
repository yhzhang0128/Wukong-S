/**
 * @file stream_serializer.h
 * @brief Triple packet receiver and ingestion coordinator
 */


#pragma once

#include "thread_cfg.h"
#include "stream_basic_types.h"
#include "stream_coordinator.h"

/**
 * @brief Stream Serializer (not thread-safe)
 *
 * Local ingestor ask serializer for next packet to insert.
 */
class stream_serializer{
    thread_cfg* cfg;
    int polling_world;

 public:
 stream_serializer(thread_cfg* _cfg): 
    cfg(_cfg), polling_world(0) {}

    /**
     * @breif Get the next packet for ingestion
     */
    void get_next_packet(stream_dispatch_packet& packet){
        while(true){
            // coordinate for snapshot isolation
            // TODO: skip certain polling world

            // receive packet from dispatcher
            bool success = TryRecvT(cfg, polling_world, packet);
            polling_world = (polling_world + 1) % cfg->m_num;
            if (success) break;
        }
    }
};

