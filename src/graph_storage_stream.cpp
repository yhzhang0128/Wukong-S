/**
 * @file graph_storage_stream.cpp
 * @brief Methods in graph_storage class for C-SPARQL
 */

#include "graph_storage.h"
#include "message_wrap.h"
#include "kv_fetch_rpc.h"

// get_edges_glboal_stream: decide the method to use by global settings
// With RDMA:
//   get_edges_global_stream_rdma
// Without RDMA:
//   get_edges_global_cache: use vector and binary search

edge* graph_storage::get_edges_global_stream(
    thread_cfg *_cfg, const streaminfo& stream_info, uint64_t id, int direction, int predict, int* size){

    int tid = _cfg->t_id;
    int qid = stream_info.query_id;

    char *local_buffer = rdma->GetMsgAddr(tid);
    edge* result_ptr = (edge*)local_buffer;
    edge* next_ptr = result_ptr;
    *size = 0;

    // With RDMA
    // 2 time for protection
    result_ptr = (edge*)(local_buffer + 2 * metadata_manager->bucket_size_byte());
    next_ptr = result_ptr;
        
    for(auto window : stream_info.window_info){
        for(int i = window.start_batch_id; i <= window.end_batch_id; i++){
            int current_size = 0;
                
            get_edges_global_batch(_cfg, id, direction, predict, &current_size, 
                                   next_ptr, window.stream_id, i);
                    
            next_ptr += current_size;
            *size += current_size; 
        }
    }
    goto Finish;

    // Without RDMA
    // @depricated
    // if (global_enable_csparql_fork){
    //     // csparql fork means that id is in local kv-store
    //     //TODO move id judge into get_edges_global_cache
    //     if (id < (1 << nbit_predict)){
    //         get_edges_global_cache(
    //                                tid, qid, id, direction, predict, size, 
    //                                result_ptr, m_id
    //                                );
    //     } else{
    //         get_edges_global_cache(
    //                                tid, qid, id, direction, predict, size, 
    //                                result_ptr, mymath::hash_mod(id, m_num)
    //                                );
    //     }
    // } else if (id < (1 << nbit_predict)){
    //     for(int i = 0; i < m_num; i++){
    //         int size_per_world = 0;
    //         get_edges_global_cache(
    //                                tid, qid, id, direction, predict, &size_per_world, 
    //                                next_ptr, i
    //                                );
    //         next_ptr += size_per_world;
    //         *size += size_per_world;
    //     }
    // } else{
    //     get_edges_global_cache(
    //                            tid, qid, id, direction, predict, size, 
    //                            result_ptr, mymath::hash_mod(id, m_num)
    //                            );
    // }

 Finish:
    if (*size == 0)
        return NULL;
    else
        return result_ptr;
}

edge* graph_storage::get_edges_global_batch(thread_cfg *_cfg,uint64_t id,int direction,int predict,
 int* size, edge* result_ptr, int stream_id, int batch_id){
    //if( mymath::hash_mod(id,m_num) ==m_id){
    //return get_edges_local(tid,id,direction,predict,size);
    //}
    local_key key=local_key(id,direction,predict);
    int target, tid = _cfg->t_id;
    // if start from index, return LOCAl result
    if (id >= 0 && id < (1<<nbit_predict))
        target = m_id;
    else
        target = mymath::hash_mod(id,m_num);

    // use RPC to fetch data
    if (global_stream_TCP_test && target != m_id)
        return kv_fetch_thread->get_edges_rpc(_cfg, target, stream_id, batch_id, key, result_ptr, size);
    
    //vertex v=get_vertex_remote(tid,key);
    memarea_slot v;
    v = metadata_manager->metadata_hashmap_lookup(
                                                  m_id, tid, key, target, stream_id, batch_id
                                                  );

    
    if(v.area.offset == 0){
        *size=0;
        return NULL;
    }

    // use new interface of ptr+sizen
    if (target == m_id){
        // memcpy
        // memcpy(next_machine_ptr, &(edge_addr[ptr]), sizeof(edge) * area.offset);
        memcpy(result_ptr, &edge_addr[v.area.start_ptr], sizeof(edge) * v.area.offset);
    } else{
        uint64_t start_addr  = sizeof(vertex)*slot_num + sizeof(edge)*(v.area.start_ptr);
        uint64_t read_length = sizeof(edge)*v.area.offset;
        rdma->RdmaRead(tid, target, (char *)result_ptr, read_length, start_addr);
    }

    *size = v.area.offset;
    return result_ptr;
}
