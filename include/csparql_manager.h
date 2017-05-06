/**
 * @file csparql_manager.h
 * @brief Manager of C-SPARQL register, deregister and other issues.
 */

#pragma once

#include "query_basic_types.h"
#include "string_server.h"
#include "sparql_parser.h"
#include "stream_adaptor_readfile.h"


/**
 * @brief Manage all registered C-SPARQL queries
 *
 * This class provides register, deregister and some other functions 
 * related with the current C-SPARQL queries. 
 */
class csparqlManager{
private:
    string_server* str_server;
    sparql_parser parser;
    map<string, int> stream_to_id;
    vector<string> id_to_stream;
    vector<int> stream_to_world;
    vector<int> stream_rates;
    vector<request_or_reply> all_queries, local_queries;
    
public:
    /**
     * @brief Test if a key might be used in one query.
     *
     * For most cases, this can be determined and can help avoid storing unnecessary metadata.
     */
    bool key_match_query(const local_key& key,
                     const request_or_reply& req) const{
        int sz = req.cmd_chains.size();
        int step = 0;
        for(int i = 0; i < sz; i += 4){
            if (!req.stream_info.timeless[step]){
                int start = req.cmd_chains[i];
                int predict = req.cmd_chains[i + 1];
                int direction = req.cmd_chains[i + 2];

                if (start >= 0 && start != key.id) continue;
                if (predict >= 0 && predict != key.predict) continue;
                if (direction >= 0 && direction != key.dir) continue;

                return true;
            }
            step++;
        }
        return false;
    }
    
    /**
     * @brief Test if a key might be used in any query.
     *
     * if return true, the time metadata of *key* should be maintained
     */
    bool key_match_any_query(const local_key& key) const{
        int sz = all_queries.size();
        for(int j = 0; j < sz; j++){
            if (key_match_query(key, all_queries[j]))
                return true;
        }
        return false;
    }

    /**
     * @brief Give the local executer a list of queries.
     */
    void get_local_queries(vector<request_or_reply> &queries){
        queries = local_queries;
    }

    /**
     * @brief Determine whether to maintain time metadata for each stream
     * @param enable_metadata the length of this vector is number of streams
     */
    void enable_metadata(vector<bool>& enable_metadata){
        for(auto iter : all_queries){
            int nwindows = iter.stream_info.window_info.size();
            for(int i = 0; i < nwindows; i++)
                enable_metadata[
                    iter.stream_info.window_info[i].stream_id
                ] = true;
        }
    }


    /**
     * @brief Give metadata manager a hint for memory allocation
     * @return number of batches for largest window in all queries
     */
    int nbatch_max_window(){
        int ret = 0;
        for(auto iter : all_queries){
            for(auto window_iter : iter.stream_info.window_info){
                int nbatch = window_iter.window_size_nbatch();
                ret = ret > nbatch? ret : nbatch;
            }
        }
        return ret;
    }

    /**
     * @brief Initialize streams and C-SPARQL queries
     *
     * Currently read from config files
     */
    csparqlManager(string_server* _str_server, 
                   boost::mpi::communicator& world,
                   stream_adaptor_interface** adaptors,
                   int& nadaptors):
        str_server(_str_server), parser(_str_server){

        // prepare streams and adaptors
        initialize_streams(world, adaptors, nadaptors);

        // prepare and parse queries
        initialize_queries(world);
    }

    /**
     * @brief Print details of registered C-SPARQL queries
     */
    void print(){
        int local_query_num = local_queries.size();
        for(int i = 0; i < local_query_num; i++){
            cout << "##### CSPARQL: " << local_queries[i].stream_info.query_name << endl;
            int sz = local_queries[i].stream_info.window_info.size();
            for(int j = 0; j < sz; j++){
                int id = local_queries[i].stream_info.window_info[j].stream_id;
                cout << id_to_stream[id] << " ";
                cout << local_queries[i].stream_info.window_info[j].window_size << " ";
                cout << local_queries[i].stream_info.window_info[j].window_step << " ";            
                cout << endl;
            }

            int cmd_sz = local_queries[i].cmd_chains.size();
            cout << "start\tp\tdirect\tend\n";
            for(int j = 0; j < cmd_sz; j+=4){
                cout << local_queries[i].cmd_chains[j] << "\t";
                cout << local_queries[i].cmd_chains[j + 1] << "\t";
                cout << local_queries[i].cmd_chains[j + 2] << "\t";
                cout << local_queries[i].cmd_chains[j + 3] << "\n";
            }
        }
    }

 private:
    /**
     * @brief Initialize streams in constructor
     */
    void initialize_streams(boost::mpi::communicator& world,
                           stream_adaptor_interface** adaptors,
                           int& nadaptors){
        // check config file
        ifstream source_config(global_input_folder + global_stream_source_config);
        if (source_config.good() == false){
            cout << "Stream source config file not available!" << endl;
            assert(false);
        }

        // parse config file
        int target, rate;
        string name, filename;
        for(int i = 0; i < global_stream_source_num; i++){
            source_config >> target >> rate >> name >> filename;

            stream_to_id[name] = i;
            id_to_stream.push_back(name);
            stream_to_world.push_back(target);
            stream_rates.push_back(rate);

            if (source_config.good()){
                if (target == world.rank())
                    adaptors[nadaptors++] = new stream_adaptor_readfile(i, filename, rate, world.size());
            } else{
                cout << "global_stream_source_num is too big, check stream config file" << endl;
                assert(false);
            }
        }
        cout << nadaptors << " adaptors in world" << world.rank() << endl;
    }

    /**
     * @brief Initialize queries in constructor
     */
    void initialize_queries(boost::mpi::communicator& world){
        int target;
        string name, filename;

        // check config file
        ifstream config(global_csparql_folder + global_stream_query_config);
        if (config.good() == false){
            cout << "Stream query config file not available!" << endl;
            assert(false);
        }

        // parse config file
        for(int i = 0 ; i < global_stream_query_num; i++){
            config >> target >> filename;

            if (!config.good()){
                cout << "global_stream_query_num is too big, check query config" << endl;
                assert(false);
            } else{
                request_or_reply request;
                bool success = parser.parse_stream(global_csparql_folder + filename, request, &stream_to_id);

                if (!success){
                    // syntax error
                    cout << "csparql syntax error " + filename << endl;
                    assert(false);
                } else{
                    // normal check
                    if (request.cmd_chains.size() == 0){
                        cout << "cmd_chains length error" << endl;
                        assert(false);
                    }
                    if (request.use_index_vertex() &&
                        request.cmd_chains[0] == request.cmd_chains[1])
                        request.cmd_chains[1] = 0;  // usually __PREDICT__

                    // Initialize stream_info of request
                    request.stream_info.is_stream_query = true;
                    request.stream_info.query_id = i;

                    // If starting vertex is fixed, better move to its target
                    if (request.cmd_chains[0] > (1 << nbit_predict)){
                        if (global_stream_query_throughput_test)
                            assert(target == mymath::hash_mod(request.cmd_chains[0], world.size()));
                        target = mymath::hash_mod(request.cmd_chains[0], world.size());
                        cout << "Move Query " << request.stream_info.query_name << " to world " << target << endl;
                    }

                    // check window size/step and determine dispatch frequency
                    int sz = request.stream_info.window_info.size();
                    for(int j = 0; j < sz; j++){
                        uint64_t window_size = request.stream_info.window_info[j].window_size;
                        uint64_t window_step = request.stream_info.window_info[j].window_step;
                        if (window_size == 0 || window_step == 0){
                            cout << "window definition error for "
                                 << request.stream_info.window_info[j].stream_id;
                            assert(false);
                        }

                        global_dispatch_max_interval =
                            mymath::gcd(window_size, global_dispatch_max_interval);
                        global_dispatch_max_interval =
                            mymath::gcd(window_step, global_dispatch_max_interval);
                    }

                    // add to query list
                    all_queries.push_back(request);
                    if (target == world.rank())
                        local_queries.push_back(request);
                }

            }
        }

    }
};

extern csparqlManager *csparql_manager;
