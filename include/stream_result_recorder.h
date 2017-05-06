/**
 * @file stream_result_recorder.h
 * @brief Recording C-SPARQL query results
 */

#pragma once

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include "csparql_manager.h"

#include <tbb/concurrent_vector.h>

const int LATENCY_RECORD_NUMBER = 100;
const int THROUGHPUT_RECORD_NUMBER = 10;

class stream_resultRecorder{
    struct latency_result{
        int result_size;
        uint64_t latency;
    };

    // @debug
    int tpt_finish, tpt_all;
    map<string, vector<latency_result>> latency_results;
    map<string, tbb::concurrent_vector<int>> throughput_results;

public:
    stream_resultRecorder() {
        // initialize for latency
        vector<request_or_reply> queries;
        csparql_manager->get_local_queries(queries);

        if (global_stream_query_throughput_test){
            for(auto iter : queries){
                throughput_results[iter.stream_info.query_name] = tbb::concurrent_vector<int>();
                throughput_results[iter.stream_info.query_name].reserve(THROUGHPUT_RECORD_NUMBER
                                                                        * global_num_stream_client);
            }
        } else{
            for(auto iter : queries){
                latency_results[iter.stream_info.query_name] = vector<latency_result>();
                latency_results[iter.stream_info.query_name].reserve(LATENCY_RECORD_NUMBER);
            }
        }
            tpt_finish = 0;
            tpt_all = queries.size();

    }

    void add_latency(const string& name, int result_size, int latency){
        latency_result new_result;
        new_result.result_size = result_size;
        new_result.latency = latency;

        if (latency_results[name].size() < LATENCY_RECORD_NUMBER){
            latency_results[name].push_back(new_result);
        }else{
            print_latency(0);
        }
    }

    void add_throughput(const string& name, int throughput){
        // @debug
        // throughput test 1
        // give each thread THROUGHPUT_RECORD_NUMBER chances
        int threshold = THROUGHPUT_RECORD_NUMBER * global_stream_query_num / 6;
        if (throughput_results[name].size() < threshold)
            throughput_results[name].push_back(throughput);
        if (throughput_results[name].size() == threshold){
            tpt_finish++;
            if (tpt_finish == tpt_all){
                cout << "\n\n\nFinish\n\n\n";
            }
        }
    }

    void print_latency(int m_id){
        // for not mixing printing result
        usleep(m_id * 50000);
        
        map<string, vector<latency_result>>::iterator iter;
        for(iter = latency_results.begin();
            iter != latency_results.end(); iter++){
            cout << "##### Stream Query: " << iter->first << " at " << m_id << endl;

            int sz = iter->second.size();
            uint64_t avg_exec = 0;
            for(int i = 0; i < sz; i++){
                avg_exec += iter->second[i].latency;
                cout << iter->second[i].latency << '\t';
            }
            if (sz != 0){
                avg_exec /= sz;
                cout << endl;
                cout << iter->first << "  average executing time: " << avg_exec << " us for " << sz << " records" << endl;
            } else{
                cout << iter->first << " has no record!" << endl;
            }
        }
    }

    void print_throughput(int m_id){
        // for not mixing printing result
        usleep(m_id * 200000);
        
        map<string, tbb::concurrent_vector<int>>::iterator iter;
        for(iter = throughput_results.begin();
            iter != throughput_results.end(); iter++){
            //cout << "##### Stream Query: " << iter->first << " at " << m_id << endl;

            int sz = iter->second.size();
            uint64_t avg_exec = 0;
            for(int i = 0; i < sz; i++){
                avg_exec += iter->second[i];
                //cout << iter->second[i] << '\t';
            }
            if (sz != 0){
                avg_exec /= sz;
                //cout << endl;
                cout << iter->first << "  average throughput: " << avg_exec << "/second" << endl;
            } else{
                cout << iter->first << " has no record!" << endl;
            }
        }
    }

};

extern stream_resultRecorder *stream_result_recorder;
