/**
 * @file kv_fetch_rpc.h
 * @brief RPC thread for key-value fetching
 *
 * Listening thread for kv-fetch RPC. 
 * Only enabled when global_stream_TCP_test is true
 */
#pragma once

#include "thread_cfg.h"
#include "global_cfg.h"
#include "graph_storage.h"

/**
 * @brief key-value fetch RPC
 * @bug not thead-safe
 */
class kv_fetch_rpc{
    thread_cfg* cfg;
    graph_storage* graph;
    std::vector<uint32_t> recv_buffer, send_buffer;
 public:
 kv_fetch_rpc(thread_cfg *_cfg, graph_storage *_graph): cfg(_cfg), graph(_graph){
        assert(global_stream_TCP_test);
    }

    /**
     * @brief call by main to start thread
     */
    void run(){
        //edge* get_edges_local(int tid,uint64_t id,int direction,int predict,int* size)
        /* cout << "########\n\n"; */
        /* cout << "key-value fetch RPC is running\n\n"; */
        /* cout << "########\n"; */

        while(true){
            string request = cfg->node->Recv();
            //string request = cfg->rdma->rbfRecv(global_num_thread - 1);

            uint64_t id;
            int direction, predict, target, tid, stream_id, batch_id;
            std::stringstream s;
            s << request;
            boost::archive::binary_iarchive ia(s);
            recv_buffer.clear();
            ia >> id >> direction >> predict >> stream_id >> batch_id >> target >> tid;
            local_key key;
            key.id = id; key.dir = direction; key.predict = predict;

            int size;
            /* edge* local_ptr = graph->get_edges_local(cfg->t_id, id, direction, predict, &size); */

            memarea_slot v;
            v = metadata_manager->metadata_hashmap_lookup(
                                                          cfg->m_id, cfg->t_id, key, cfg->m_id, stream_id, batch_id
                                                          );
            
            send_buffer.clear();
            for(int i = 0; i <  v.area.offset; i++)
                send_buffer.push_back(graph->edge_addr[v.area.start_ptr + i].val);

            std::stringstream ss;
            boost::archive::binary_oarchive oa(ss);
            oa << send_buffer;

            cfg->node->Send(cfg->m_id, target, tid, ss.str());
            //cfg->rdma->rbfSend(global_num_thread - 1, target, global_num_thread - 1, ss.str().c_str(), ss.str().size());
        }
    }

    /**
     * @brief call by query thread
     */
    edge* get_edges_rpc(thread_cfg *_cfg, int target, int stream_id, int batch_id,
                        const local_key &key, edge* result, int* size){
        // send local_key
        std::stringstream ss;
        boost::archive::binary_oarchive oa(ss);
        uint64_t id = key.id;
        int direction = key.dir, predict = key.predict;
        oa << id << direction << predict << stream_id << batch_id << _cfg->m_id << _cfg->t_id;

        //cfg->rdma->rbfSend(global_num_thread - 1, target, global_num_thread - 1, ss.str().c_str(), ss.str().size());
        _cfg->node->Send(_cfg->m_id, target, global_num_thread - 1, ss.str());
 
        // receive reply
        std::string reply = _cfg->node->Recv(target);

        if (reply == "")
            return NULL;
        
        std::stringstream s;
        s << reply;
        boost::archive::binary_iarchive ia(s);
        recv_buffer.clear();
        ia >> recv_buffer;

        (*size) = recv_buffer.size();
        if ((*size) == 0)
            return NULL;

        for(int i = 0; i < *size; i++){
            result[i].val = recv_buffer[i];
        }
        return result;
    }
};

extern kv_fetch_rpc *kv_fetch_thread;
