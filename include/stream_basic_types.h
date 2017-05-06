/**
 * @file stream_basic_types.h
 * @brief Define the data structures used in streaming.
 *
 * This file contains three kinds of definitions:
 *   Basic data structures(timestamp, dispatch packet, etc.)
 *   Data structures to represent time metadata for stream window
 *   Stream related information in request_or_reply
 */

#pragma once

#include <vector>
#include <algorithm>
#include <unordered_map>
#include "global_cfg.h"
#include "graph_basic_types.h"

// Boost Binary Archive for Serialization
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>

/**************** data structures for storage logic ****************/

/**
 * @brief stream ID and discrete batch number
 *
 * Mapping from stream name to ID is maintained in csparql_manager
 */
struct stream_timestamp{
    int stream_id;
    int batch_id;
    stream_timestamp(): stream_id(0), batch_id(0) {}
    stream_timestamp(int _stream_id, int _batch_id): stream_id(_stream_id), batch_id(_batch_id) {}
    

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & stream_id;
        ar & batch_id;
    }
};

/**
 * @brief RDF triple before being distributed
 *
 * The target machine is based on the hash of SUBJECT and OBJECT
 */
struct stream_batch_item{
    uint64_t s, p, o;
    uint32_t m_id_s, m_id_o;

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & m_id_s;
        ar & m_id_o;
        ar & s;
        ar & p;
        ar & o;
    }
};

/**
 * @brief Batch prepared by Adapter
 */
struct stream_batch{
    stream_timestamp time;
    vector<stream_batch_item> items;

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & time;
        ar & items;
    }    
};

/**
 * @brief Packet sent by Dispatcher
 */
struct stream_dispatch_packet{
    vector<stream_batch> packet;
    
    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & packet;
    }  
};

/**
 * @brief VTS(vector timestamp) for consistency
 */
struct stream_vector_clock{
    vector<stream_timestamp> clock;

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & clock;
    }

    void reset(){
        clock.clear();

        for(int i = 0; i < global_stream_source_num; i++){
            clock.push_back(stream_timestamp());
            clock[i].stream_id = i;
            clock[i].batch_id = 0;
        }
    }
};

/**************** data structures for metadata ****************/

/**
 * @brief Metadata of a key in a stream window
 * @depricated The flattened data structures are more efficient.
 *
 * A stream window is a multiple of batches. Each batch may insert
 * a consecutive area into the value of a key, specified by paired
 * element in start_off and end_off.
 */
/* struct memarea_archive{ */
/*     uint64_t start_ptr, max_off; */
/*     vector<uint32_t> start_off; */
/*     vector<uint32_t> end_off; */

/*     template <typename Archive> */
/*     void serialize(Archive &ar, const unsigned int version) { */
/*         ar & start_ptr; */
/*         ar & max_off; */
/*         ar & start_off; */
/*         ar & end_off; */
/*     } */

/*     memarea_archive(): start_ptr(0), max_off(0) {} */
/* }; */

/**
 * @brief Metadata of a window
 * @depricated The flattened data structures are more efficient.
 *
 * All information needed to extract a window from the local kv-store
 */
/* struct eager_metadata_packet{ */
/*     map<uint64_t, memarea_archive> metadata; */

/*     inline int size()  {return metadata.size();} */
/*     template <typename Archive> */
/*     void serialize(Archive &ar, const unsigned int version) { */
/*         ar & metadata; */
/*     } */
/* }; */
/* // local_eager_metadata[mid][qid] is the metadata currently needed by query qid on machine mid */
/* extern eager_metadata_packet** local_eager_metadata; */


/**
 * @brief Flattened memarea_archive for faster serialization
 */
/* struct flat_memarea_archive{ */
/*     uint32_t start_ptr, start_off, end_off; */
    
/*     template <typename Archive> */
/*     void serialize(Archive &ar, const unsigned int version) { */
/*         ar & start_ptr; */
/*         ar & start_off; */
/*         ar & end_off; */
/*     } */
/* }; */

/**
 * @brief Flattened metadata of a window
 */
/* struct forward_star_packet{ */
/*     struct index{ */
/*         uint64_t key; */
/*         flat_memarea_archive area; */
/*         template <typename Archive> */
/*         void serialize(Archive &ar, const unsigned int version) { */
/*             ar & key; */
/*             ar & area; */
/*         } */

/*         // for sorting */
/*         bool operator < (const index& oth) const{ */
/*             return key < oth.key; */
/*         } */
/*     }; */
/*     vector<index> indexes; */

/*     template <typename Archive> */
/*     void serialize(Archive &ar, const unsigned int version) { */
/*         ar & indexes; */
/*     } */

/*     void add(uint64_t key, */
/*              uint32_t start_ptr, uint32_t start_off, uint32_t end_off){ */
/*         index idx; */
/*         idx.key = key; */
/*         idx.area.start_ptr = start_ptr; */
/*         idx.area.start_off = start_off; */
/*         idx.area.end_off = end_off; */
/*         indexes.push_back(idx); */
/*     } */

/*     void clear(){ */
/*         indexes.clear(); */
/*     } */

/*     void finish(){ */
/*         sort(indexes.begin(), indexes.end()); */
/*     } */
/* }; */
/* extern forward_star_packet** local_optimized_metadata; */

/**
 * @brief Slot of metadata hash table bucket
 */
struct memarea_slot{
    uint64_t key;
    edge_memarea area;
    memarea_slot(): key(0){}
    memarea_slot(const memarea_slot& oth){
        key = oth.key;
        area.start_ptr = oth.area.start_ptr;
        area.offset = oth.area.offset;
    }
    void operator = (const memarea_slot& oth){
        key = oth.key;
        area.start_ptr = oth.area.start_ptr;
        area.offset = oth.area.offset;
    }
};

/**
 * @brief Used by metadata_manager to maintain metadata internally
 *
 * Notice: should reserve size before use
 */
struct metadata_batch{
    int batch_id;
    vector<memarea_slot> records;
};


/**
 * @brief Archive struct for memarea_slot
 *
 * Element in memarea_archive is either a key/value pair or a timestamp
 *    key == 0: area encodes a vector_clock
 *    key != 0: area encodes the key's area
 * Encoding is for faster serialization
 */
struct memarea_archive{
    uint64_t key;  /** same as memarea_slot */
    uint64_t area; /** sizeof(edge_memarea) = 8byte */
    memarea_archive(){}
    memarea_archive(const memarea_slot& slot){
        key = slot.key;
        area = ((uint64_t)slot.area.start_ptr << nbit_memarea_offset) + slot.area.offset;
    }
    memarea_archive(const stream_timestamp& time){
        key = 0;
        area = ((uint64_t)time.stream_id << 32) + time.batch_id;
    }

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & key;
        ar & area;
    }

    inline uint64_t start_ptr() const {return (area >> nbit_memarea_offset) & ((1LL << nbit_memarea_pointer) - 1);}
    inline uint64_t offset() const {return area & ((1LL << nbit_memarea_offset) - 1);}
    inline int stream_id() const {return (area >> 32) & (~0);}
    inline int batch_id() const {return area & (~0);} // ~0 is 32 ones
};

/**
 * @brief Metadata cache for single C-SPARQL execution
 */
struct metadata_cache_packet{
    vector<memarea_archive> archive;

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & archive;
    }

    void append(const vector<memarea_slot>& batch){
        int sz = batch.size();
        for(int i = 0; i < sz; i++){
            archive.push_back( memarea_archive(batch[i]) );
        }
    }

    void append(const stream_timestamp& time){
        archive.push_back( memarea_archive(time) );
    }

    void clear(){
        archive.clear();
    }
};

extern int batch_num_reserved; /** number of metadata batches reserved for each stream */
extern int batch_size_reserved; /** number of entries reserved in each batch */
extern vector<metadata_batch> *stream_metadata;
// metadata_local_cache[machine_id][stream_id][batch_id] = metadata needed
// extern unordered_map<uint64_t, memarea_slot>*** metadata_local_cache;

/**************** data structures for query logic ****************/

// definitions for garbage collection
//gc[stream_id][query_id] = batch_id
/* depricated gc style */
/* typedef map<int, int> query_to_batchid; */
/* typedef vector<query_to_batchid> stream_to_border; */


/**
 * @brief Window information in a certain query
 */
struct windowinfo{
    int stream_id;
    uint64_t start_batch_id, end_batch_id;
    uint64_t window_size, window_step;

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & stream_id;
        ar & start_batch_id;
        ar & end_batch_id;
        ar & window_size;
        ar & window_step;
    }

    inline int window_size_nbatch(){
        return window_size / global_dispatch_max_interval;
    }
    inline int window_step_nbatch(){
        return window_step / global_dispatch_max_interval;
    }

    windowinfo()    {}
    windowinfo(int id, int size, int step):
        stream_id(id),
        start_batch_id(0), end_batch_id(0),
        window_size(size), window_step(step)    {}
};


/**
 * @brief Stream information used in request_or_reply
 */
struct streaminfo{
    bool is_stream_query;
    bool is_query_partition;
    int query_id;
    string query_name;
    vector<bool> timeless;
    // a C-SPARQL query may specify multiple windows
    vector<windowinfo> window_info;

    vector<int> atomic_result_table;

    streaminfo():
      is_stream_query(false), 
      is_query_partition(false),
      query_name("not initialized") {}

    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & is_stream_query;
        ar & is_query_partition;
        ar & query_id;
        ar & query_name;
        ar & timeless;
        ar & window_info;
        ar & atomic_result_table;
    }


    /**
     * @brief Special function for CityBench
     *
     * Some queries in CityBench contain seperate parts.
     * atomic_result_table is used to record the result of each part.
     */ 
    void add_atomic_result(
        int col_num, vector<int>& result_talbe){
        // Currently only record the result size
        if (col_num == 0)
            atomic_result_table.push_back(0);
        else
            atomic_result_table.push_back(result_talbe.size()/col_num);

        result_talbe.clear();
    }
};
