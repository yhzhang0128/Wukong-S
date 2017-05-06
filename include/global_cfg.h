/**
 * @file global_cfg.h
 * @brief Global config variables
 */

#pragma once

#include <map>
#include <string>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h> //atoi
#include <sstream>

using namespace std;

extern int global_rdftype_id;  // only a global variable, but not configurable
extern int global_num_thread;  //=global_num_server+global_num_client
extern int global_multithread_factor;

extern bool global_use_rbf;
extern bool global_use_rdma;
extern int global_rdma_threshold;
extern int global_num_server;
extern int global_num_client;
extern int global_batch_factor;
extern string global_input_folder;
extern int global_client_mode;
extern bool global_use_loc_cache;
extern bool global_load_minimal_index;
extern bool global_silent;
extern int global_max_print_row;
extern int global_total_memory_gb;
extern int global_perslot_msg_mb;
extern int global_perslot_rdma_mb;
extern int global_hash_header_million;
extern int global_enable_workstealing;
extern int global_enable_index_partition;
extern int global_verbose;

extern bool global_enable_streaming;
extern bool global_remove_dup;
extern string global_csparql_folder;
extern string global_stream_source_config;
extern int global_max_adaptor;
extern int global_dispatch_max_interval;
extern int global_adaptor_max_throughput;
extern int global_adaptor_buffer_ntriple;
extern int global_num_stream_client;
extern string global_stream_query_config;

extern int global_stream_source_num;
extern int global_stream_query_num;
extern int global_stream_multithread_factor;

extern bool global_stream_query_silent;
extern bool global_server_debugging;
extern bool global_dispatch_debugging;

extern bool global_stream_TCP_test;
extern bool global_stream_query_throughput_test;
/** @depricated */
/* extern bool global_enable_metadata_rdma;  */

extern int global_metadata_size_mb;
extern bool global_metadata_location_cache;

// tid for store threads
extern int global_coordinator_tid;
extern int global_dispatcher_tid;
extern int global_stream_serializer_tid;
// tid for query threads
extern int global_stream_client_tid;


void load_changeable_cfg();
void load_global_cfg(char* filename);
