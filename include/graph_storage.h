/**
 * @file graph_storage.h
 * @brief Local graph storage class
 */

#pragma once
#include <stdint.h>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <boost/unordered_set.hpp>
#include <tbb/concurrent_hash_map.h>

#include "rdma_resource.h"
#include "graph_basic_types.h"
#include "global_cfg.h"
#include "thread_cfg.h"
#include "utils.h"

#include "malloc_naive.h"
#include "malloc_buddysystem.h"

#include "metadata_manager.h"
#include "stream_basic_types.h"
#include "stream_query_client.h"

class graph_storage{
    class rdma_cache{
        struct cache_item{
            pthread_spinlock_t lock;
            vertex v;
            cache_item(){
                pthread_spin_init(&lock,0);
            }
        };
        static const int num_cache=100000;
        cache_item array[num_cache];
    public:
        bool lookup(local_key key,vertex& ret){
            if(!global_use_loc_cache){
                return false;
            }
            int idx=key.hash()%num_cache;
            bool found=false;
            pthread_spin_lock(&(array[idx].lock));
            if(array[idx].v.key==key){
                ret=array[idx].v;
                found=true;
            }
            pthread_spin_unlock(&(array[idx].lock));
            return found;
        }
        void insert(vertex& v){
            if(!global_use_loc_cache){
                return ;
            }
            int idx=v.key.hash()%num_cache;
            pthread_spin_lock(&(array[idx].lock));
            array[idx].v=v;
            pthread_spin_unlock(&(array[idx].lock));
        }
    };
    static const int num_locks=1024;
    static const int indirect_ratio=7; // 	1/indirect_ratio  of buckets are used as indirect buckets
    static const int cluster_size=8;   //	each bucket has cluster_size slots

    pthread_spinlock_t allocation_lock;

	pthread_spinlock_t fine_grain_locks[num_locks];

    rdma_cache rdmacache;

    vertex* vertex_addr;
	RdmaResource* rdma;

	uint64_t slot_num;
	uint64_t m_num;
	uint64_t m_id;

	uint64_t header_num;
	uint64_t indirect_num;

    uint64_t used_indirect_num;
    uint64_t type_index_edge_num; // used to print memory usage
    uint64_t predict_index_edge_num; // used to print memory usage

    // manage edge
    malloc_interface *edge_manager;

    uint64_t insertKey(local_key key, bool check_dup);
    bool insertKeyValue(local_key key, uint64_t value, stream_timestamp timestamp);
    bool init_mem_for_key(uint64_t vertex_ptr, uint64_t size);
    // this method is obsolete and replaced by edge_manager
    //uint64_t atomic_alloc_edges(uint64_t num_edge);

    vertex get_vertex_local(local_key key);
    vertex get_vertex_remote(int tid,local_key key);

    // added for streaming
    // @debug
    uint64_t slot_used, malloc_mem_used;
    edge* get_edges_global_batch(thread_cfg *_cfg, uint64_t id, int direction, int predict, int* size, edge* result_ptr, int stream_id, int batch_id);

public:
  	edge* edge_addr;

    graph_storage();
    void init(RdmaResource* _rdma,uint64_t machine_num,uint64_t machine_id);
    void stream_insert_spo(const edge_triple &triple, stream_timestamp timestamp);
    void stream_insert_ops(const edge_triple &triple, stream_timestamp timestamp);
    void atomic_batch_insert(vector<edge_triple>& vec_spo,vector<edge_triple>& vec_ops);
    void print_memory_usage();
    edge* get_edges_global(int tid,uint64_t id,int direction,int predict,int* size);
    edge* get_edges_local(int tid,uint64_t id,int direction,int predict,int* size);

    // added for streaming
    edge* get_edges_global_stream(thread_cfg* _cfg, const streaminfo& stream_info, uint64_t id,int direction,int predict,int* size);

//define as public
//should be refined
    typedef tbb::concurrent_hash_map<uint64_t,vector<uint64_t> > tbb_vector_table;
    void insert_vector(tbb_vector_table& table,uint64_t index_id,uint64_t value_id);
    void init_index_table();
    tbb_vector_table src_predict_table;
    tbb_vector_table dst_predict_table;
    tbb_vector_table type_table;

    edge* get_index_edges_local(int tid,uint64_t index_id,int direction,int* size);

    // only be used in eager stream query, not lazy ones
    /* edge* get_index_edges_global(int tid,uint64_t index_id,int direction,int* size); */
    /* vertex get_index_vertex_remote(int tid, local_key key, int mid); */
};
