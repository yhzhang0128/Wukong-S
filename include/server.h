/**
 * @file server.h
 * @brief Server class for receiving SPARQL and C-SPARQL 
 *        queries and processing them via graph exploration.
 */

#pragma once
#include "message_wrap.h"
#include "distributed_graph.h"
#include "query_basic_types.h"
#include "global_cfg.h"
#include "thread_cfg.h"
#include "wait_queue.h"

#include <boost/unordered_set.hpp>
#include <boost/unordered_map.hpp>

class server{
    distributed_graph& g;
	thread_cfg* cfg;
    wait_queue wqueue;
    uint64_t last_time;
    pthread_spinlock_t recv_lock;
    pthread_spinlock_t wqueue_lock;
    vector<request_or_reply> msg_fast_path;

    // all of these means const predict
    void const_to_unknown(request_or_reply& req);
    void const_to_known(request_or_reply& req);
    void known_to_unknown(request_or_reply& req);
    void known_to_known(request_or_reply& req);
    void known_to_const(request_or_reply& req);
    void index_to_unknown(request_or_reply& req);

    // unknown_predict
    void const_unknown_unknown(request_or_reply& req);
    void known_unknown_unknown(request_or_reply& req);
    void known_unknown_const(request_or_reply& req);


    vector<request_or_reply> generate_sub_requests(request_or_reply& r);
    vector<request_or_reply> generate_mt_sub_requests(request_or_reply& r);
    
    bool need_sub_requests(request_or_reply& req);
    bool execute_one_step(request_or_reply& req);
    void execute(request_or_reply& req);

    server** s_array;// array of server pointers

    //join related functions
    void handle_join(request_or_reply& req);

    // added for streaming
    inline bool timeless_pattern(const request_or_reply& req, int step){
        if (!req.stream_info.is_stream_query)
            return true;
        if (step > req.stream_info.timeless.size()){
            cout << "stream_pattern matching overflow\n";
            exit(0);
        }
        return req.stream_info.timeless[step];
    }
    inline edge* server_get_edges_global(uint64_t id, int direction, int predict,
                                         int* size, const request_or_reply& req){
        // check if predict is streaming predict
        if (timeless_pattern(req, req.step)){
            // get all the values
            return g.local_storage.get_edges_global(
                       cfg->t_id, id, direction, predict, size);
        } else{
            // get values in the window
            return g.local_storage.get_edges_global_stream(
                       cfg, req.stream_info, id, direction, predict, size);
        }
    }

public:
    server(distributed_graph& _g,thread_cfg* _cfg);
    void set_server_array(server** array){
        s_array=array;
    };
    void run();
};
