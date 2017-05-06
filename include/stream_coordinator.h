/**
 * @file stream_coordinator.h
 * @brief Maintain insertion state and snapshot mechanism
 * @notice stable DON'T mean finish insertion, insead, means that all metadata is ready
 *
 * There is no coordinator thread. The methods are invoked periodically
 * by stream_dispatcher. The coordinate steps are:
 *    1. ingestor update local METADATA(not insert) state
 *    2. dispatcher send local METADATA(not insert) state to all machines
 *    3. dispatcher receive these states and update global steady insert state
 *    4. stream_query_client read global state to decide query execution
 * Step3 and Step4 are actually dispatcher synchronization.
 */

#pragma once

#include "thread_cfg.h"
#include "global_cfg.h"
#include "stream_basic_types.h"
#include <boost/mpi.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>


/**
 * @brief Coordinator for Query, Graph Storage
 */
class stream_coordinator{
    thread_cfg* cfg;
    boost::mpi::communicator& world;

    stream_vector_clock global_steady_clock;  /** global insert state */
    stream_vector_clock *local_steady_clocks;  /** local insert state of all machines */
    stream_vector_clock last_send_clock;

    /**
     * @brief Receive steady VTS from all machines and update global steady VTS
     */
    void update_global_steady_clock();
    /**
     * @brief Receive metadata_cache_packet from all machines and update global steady VTS
     */
    void update_steady_clock_with_metadata();
    
 public:
 stream_coordinator(thread_cfg* _cfg, boost::mpi::communicator& _world):
    cfg(_cfg), world(_world){
        // initialize steady clocks
        global_steady_clock.reset();
        last_send_clock.reset();
        local_steady_clocks = new stream_vector_clock[cfg->m_num];
        for(int i = 0; i < cfg->m_num; i++){
            local_steady_clocks[i].reset();
        }
    }

    /**
     * @brief Ingestor update local insert state
     */
    inline void update_local_steady_clock(const stream_timestamp &t){
        local_steady_clocks[world.rank()].clock[t.stream_id].batch_id = t.batch_id;
    }

    /**
     * @brief Dispatcher send local insert state periodically
     */
    void send_local_steady_clock();
    /**
     * @brief Dispatcher recv coordinate packet/vector clock
     */
    void recv_global_steady_clock();

    /**
     * @brief CSPARQL executor query global insert state of stream
     */
    inline int get_steady_batchid(int stream_id){
        return global_steady_clock.clock[stream_id].batch_id;
    }


    void print_local_steady_clock();
    void print_global_steady_clock();
};
