/**
 * @file stream_query_client.cpp
 * @brief Methods in stream_query_client class
 */

#include "global_cfg.h"
#include "message_wrap.h"
#include "stream_query_client.h"
#include "stream_result_recorder.h"

void stream_query_client::send(request_or_reply& req){
    if(req.parent_id==-1){
        req.parent_id=cfg->get_inc_id();
    }

    if (req.stream_info.is_stream_query){
        // current stream query is assigned to this machine

        int start = req.cmd_chains[0];

        if (req.use_index_vertex()){
            // start from predicate, fork queries
            req.stream_info.is_query_partition = true;
            // dispatch the query to corresponding servers
            int nthread = global_stream_multithread_factor;
            int tid_base = cfg->client_num +
                global_stream_multithread_factor *
                (cfg->t_id - global_stream_client_tid);
            //cout << req.stream_info.query_name << " send to " << tid_base << " " << nthread << endl;
            for(int i = 0; i < cfg->m_num; i++){
                for(int j = 0; j < nthread; j++){
                    req.first_target = i;
                    req.mt_total_thread = nthread;
                    req.mt_current_thread = j;
                    SendR(cfg, i, tid_base + j, req);
                    //cout << "Send request: " << req.stream_info.query_name << " to" << tid_base+j << endl;
                }
            }
        } else{
            // start from normal vertex
            int tid = cfg->client_num +
                global_stream_multithread_factor *
                (cfg->t_id - global_stream_client_tid);

            assert(cfg->m_id == mymath::hash_mod(start, cfg->m_num));
            req.first_target = cfg->m_id;                
            SendR(cfg,req.first_target,tid,req);
        }

        return ;
    }
}

request_or_reply stream_query_client::recv(){
    request_or_reply r = RecvR(cfg);

    if (r.stream_info.is_stream_query){
        // execute stream query on only one machine

        if (r.stream_info.is_query_partition){
            int nthread= global_stream_multithread_factor;
            for(int i = 0; i < cfg->m_num * nthread - 1; i++){
                request_or_reply r2 = RecvR(cfg);
                // they should execute same steps
                assert(r2.col_num == r.col_num);
                r.silent_row_num += r2.silent_row_num;

                int new_size = r.result_table.size() + r2.result_table.size();
                r.result_table.reserve(new_size);
                r.result_table.insert(r.result_table.end(), r2.result_table.begin(), r2.result_table.end());
            }
        }

        if (!r.is_finished()){
            //cout << " Collected and restart at step" << r.step << endl;
            //cout << r.row_num() << " : " << r.col_num << endl;
            r.stream_info.is_query_partition = false;
            r.first_target = cfg->m_id;
            r.parent_id = cfg->get_inc_id();  // set return target to current client
            r.id = -1;
            //int tid = cfg->client_num + cfg->get_random() % cfg->server_num;
            int tid = cfg->client_num +
                      global_stream_multithread_factor *
                      (cfg->t_id - global_stream_client_tid);
            SendR(cfg, r.first_target, tid, r);

            r = RecvR(cfg);
        }
        return r;
    }
}

void stream_query_client::local_metadata_lookup(request_or_reply& r){
    // @deprecated
    //local_optimized_metadata[i][r.stream_info.query_id]
    // local_optimized_metadata[cfg->m_id][r.stream_info.query_id].clear();

    // uint64_t t1=timer::get_usec();
    // metadata_manager->eager_query(r,  // prepare_metadata_cache
    //     &local_optimized_metadata[cfg->m_id][r.stream_info.query_id]);
    // local_optimized_metadata[cfg->m_id][r.stream_info.query_id].finish();
    // uint64_t t2=timer::get_usec();
    //cout << "  prepare local latency: " << t2 - t1 << endl;
}

int stream_query_client::stream_query_execution(request_or_reply& r){
    // Execution
    r.parent_id = -1;
    r.silent = global_silent;
    send(r);
    request_or_reply reply = recv();
    return reply.stream_row_num();
}

void stream_query_client::master(){
    // initialize all queries assigned to this machine
    int master_id = cfg->t_id - global_stream_client_tid;

    vector<request_or_reply> local_queries;
    csparql_manager->get_local_queries(local_queries);
    //csparql_manager->print();

    if (local_queries.size() == 0){
        // no query assigned to this machine
        return;
    }

    // initialize queries
    vector<request_or_reply>::iterator iter;
    for(iter = local_queries.begin(); iter != local_queries.end(); iter++){
        int nwindows = iter->stream_info.window_info.size();
        for(int i = 0; i < nwindows; i++){
            int window_step_nbatch = 
                iter->stream_info.window_info[i].window_step_nbatch();
            int window_size_nbatch = 
                iter->stream_info.window_info[i].window_size_nbatch();
            iter->stream_info.window_info[i].start_batch_id = 1;
            iter->stream_info.window_info[i].end_batch_id = window_size_nbatch;
        }
    }

    // polling to deal with queries one by one
    int nquery = local_queries.size();
    int query_idx = 0;
    while(true){
        // polling for a ready query
        while(true){
            bool ready = true;
            query_idx = (query_idx + 1) % nquery;

            // doesn't belong to this master
            if (query_idx % global_num_stream_client != master_id)
                continue;

            // check if query is ready
            int nwindows = local_queries[query_idx].stream_info.window_info.size();
            for(int i = 0; i < nwindows; i++){
                int stream_id = local_queries[query_idx].stream_info.window_info[i].stream_id;
                int need_max = local_queries[query_idx].stream_info.window_info[i].end_batch_id;
                int current = coordinator->get_steady_batchid(stream_id);
                if (need_max + 1 > current){
                    ready = false;
                    break;
                }
            }
            if (ready)
                break;
        }

        // execute the query
        if (global_stream_query_throughput_test){
            // throughput mode
            int tpt = throughput_mode(local_queries[query_idx]);
            stream_result_recorder->add_throughput(local_queries[query_idx].stream_info.query_name, tpt);
        } else{
            // normal mode(latency)
            int start_id = local_queries[query_idx].stream_info.window_info[0].start_batch_id;
            int end_id = local_queries[query_idx].stream_info.window_info[0].end_batch_id;

            uint64_t t1=timer::get_usec();
            int sz = stream_query_execution(local_queries[query_idx]);
            uint64_t t2=timer::get_usec();

            // print and record
            if (! global_stream_query_silent){
                cout << "##### Stream Query: " << local_queries[query_idx].stream_info.query_name
                     << " start id:" << start_id << " end id:" << end_id << endl
                     << "result size:" << sz << endl
                     << "total execution latency "<< t2 - t1 <<" us"<<endl;
            }
            stream_result_recorder->add_latency(
                 local_queries[query_idx].stream_info.query_name, sz, t2 - t1);
        }

        // prepare next execution of this query
        int nwindows = local_queries[query_idx].stream_info.window_info.size();
        for(int i = 0; i < nwindows; i++){
            windowinfo *handler =
                &local_queries[query_idx].stream_info.window_info[i];
            int step_nbatch = handler->window_step_nbatch();
            int size_nbatch = handler->window_size_nbatch();
            if (handler->end_batch_id - handler->start_batch_id + 1
                != size_nbatch){
                // less than window size
                handler->end_batch_id += step_nbatch;
            } else{
                handler->start_batch_id += step_nbatch;
                handler->end_batch_id += step_nbatch;
            }
        }
    }
}

int stream_query_client::throughput_mode(request_or_reply& r){
    int count = 0;
    uint64_t start = timer::get_usec();
    while(true){
        request_or_reply tmp_req = r;
        stream_query_execution(tmp_req);
        count++;
        uint64_t end = timer::get_usec();
        if (end - start > 1000000)
            break;
    }
    return count;
}

