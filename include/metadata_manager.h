/**
 * @file metadata_manager.h
 * @brief Manage time information(metadata) for stream batches
 *
 * The time information(metadata) can help the C-SPARQL engine identify
 * memroy location of triples in a given stream window. 
 * There are 2 ways to fetch them:
 *     1. location cache(prefered)
 *     2. one-sided RDMA
 * Specified by config variable global_metadata_location_cache
 * @bug
 * The unordered_map may cause *rehash* during lookup and cause sigsegv.
 * Currently it is reserved large enough size
 */

#pragma once

#include "rdma_resource.h"
#include "graph_basic_types.h"
#include "query_basic_types.h"
#include "stream_basic_types.h"
#include "csparql_manager.h"

#include <tbb/concurrent_vector.h>

#include <map>
#include <fstream>
#include <algorithm>

/**
 * @breif Manage time-info metadata for stream batches
 */
class metadataManager{
 private:    
    boost::mpi::communicator& world; /** mpi world information */
    
    int resource_redundant_factor; /** redundant multiple for memory resource */
    stream_timestamp current_batch_time; /** batch_id currently inserting for each stream */
    unordered_map<uint64_t, memarea_slot>* current_batch;
    pthread_spinlock_t *stream_lock; /** locks for accessing per-stream data structures */
    
    /**
     * @brief Whether a stream has C-SPARQL window on it
     */
    vector<bool> enable_metadata;

    /**
     * @brief Memory efficiency recording
     * @depricated will be moved to stream_result_recorder
     */
    struct memusage{
        int query_id, ninterval, ndata;
        memusage()  {}
        memusage(int q, int i, int d):
            query_id(q), ninterval(i), ndata(d) {}
    };
    tbb::concurrent_vector<memusage> *usage;

    /// added for RDMA lookup
	RdmaResource* rdma;  /** rdma resource manager  */
    int slot_num, bucket_num, bucket_size;  /** parameters of hash table  */
    int total_indirect, next_indirect;  /** parameters of hash table */
    memarea_slot**** metadata_hashmap; /** pointer to metadata_hashmap[machine_id][stream_id][batch_id] */
    memarea_slot* metadata_indirect; /** pointer to aggregated indirect slots */
    
    /**
     * @brief Initialize metadata hashmap for RDMA
     * @param rdma_ptr start pointer of RDMA memory area of this hashmap
     */
    void metadata_hashmap_init(char* rdma_ptr);

    /**
     * @brief Insert slot into metadata hashmap for RDMA
     */
    void metadata_hashmap_insert(memarea_slot* current_hashmap, const memarea_slot& slot);
    
    /**
     * @brief Maintain metadata hashmap in RDMA area
     */
    void prepare_rdma_metadata();
    
 public:
    /**
     * @brief Constructor
     * @param rdma_ptr start pointer for RDMA hashmap
     * @param _rdma RDMA resource handler
     */
    metadataManager(char* rdma_ptr, RdmaResource* _rdma, boost::mpi::communicator& world);

    /**
     * @brief Return bucket size of metadata hashmap
     * 
     * In query executor thread, a buffer is split for multiple purpose
     * so that it needs to know the bucket size to arrange that buffer.
     */
    inline int bucket_size_byte(){return bucket_size * sizeof(memarea_slot);}

    /**
     * @brief Notify metadata manager that a stream batch is inserting
     */
    void start_batch(const stream_timestamp &t);
    
    /**
     * @brief Notify metadata manager that a stream batch is finished
     */
    void finish_batch();

    /**
     * @brief Parse metadata cache packet from other machine
     */
    void prepare_cache_metadata(int target, const metadata_cache_packet &packet,
                                stream_vector_clock* local_steady_clocks);

    /**
     * @brief Update metadata by new key and value area
     *
     * Invoked by graph_storage when inserting triples
     */
    void update_metadata(const local_key& raw_key, 
                         const edge_memarea& area, 
                         const stream_timestamp& timestamp);

    /**
     * @brief Fetch metadata through one-sided RDMA
     */
    memarea_slot metadata_hashmap_lookup(int mid, int tid, local_key key,
                                         int target, int stream_id, int batch_id);

    /**
     * @brief Prepare metadata cache for single query
     * @param r target request
     * @param cache_packet return packet
     * @depricated
     * This method is invoked by stream_query_client slave only when not using
     * metadata RDMA hashmap. The cache packet is sent back by the slave.
     */
    /* void prepare_metadata_cache(const request_or_reply& r, metadata_cache_packet* cache_packet); */

    /**
     * @brief Print memory usage
     * @depricated will be moved to stream_result_recorder
     */
    void print_memory_usage();
};

extern metadataManager *metadata_manager;
