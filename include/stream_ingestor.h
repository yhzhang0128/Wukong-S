/**
 * @file stream_ingestor.h
 * @brief Ingest triples into local kv-store
 */


#pragma once

#include "thread_cfg.h"
#include "message_wrap.h"
#include "graph_storage.h"
#include "stream_serializer.h"
#include "stream_coordinator.h"

/**
 * @brief Stream Ingestor
 */
class stream_ingestor{
private:
    stream_serializer *serializer; /** packet receiver  */
    graph_storage *graph; /** local graph */
    int world_me; /** local machine id */
    stream_coordinator *coordinator; /** vector timestamp maintainer */

public:
    /**
     * @brief Constructor
     */
 stream_ingestor(int world, 
                 stream_serializer* _serializer, 
                 stream_coordinator* _coordinator,
                 graph_storage* _graph):
    world_me(world), serializer(_serializer), 
    coordinator(_coordinator), graph(_graph){}

    /**
     * @brief Ingestion routine invoked at the beginning.
     *
     * Cooperating with metadata_manager and coordinator
     */
    void work(){
        stream_dispatch_packet packet;
        while(true){
            packet.packet.clear();
            serializer->get_next_packet(packet);

            // insert a whole packet of triples
            int nstream = packet.packet.size(), total_recv = 0;
            for(int i = 0; i < nstream; i++){
                stream_timestamp t;
                t = packet.packet[i].time;
                int nitems = packet.packet[i].items.size();
                total_recv += nitems;

                // tell metadata manager about a new stream packet
                metadata_manager->start_batch(t);

                for(int j = 0; j < nitems; j++){
                    uint32_t s, p, o;
                    s = packet.packet[i].items[j].s;
                    p = packet.packet[i].items[j].p;
                    o = packet.packet[i].items[j].o;
                    // metadata update is in stream_insert_*** routine
                    if (world_me == packet.packet[i].items[j].m_id_s)
                        graph->stream_insert_spo(
                            edge_triple(s, p, o), t);
                    if (world_me == packet.packet[i].items[j].m_id_o)
                        graph->stream_insert_ops(
                            edge_triple(s, p, o), t);
                }

                // prepare metadata rdma hashmap
                metadata_manager->finish_batch();
                // update local steady timestamp
                coordinator->update_local_steady_clock(t);
            }

        }
    }
};
