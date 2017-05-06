/**
 * @file global_cfg.cpp
 * @brief Initialize global config variables
 */

#include <string>
#include "utils.h"
#include "global_cfg.h"

int global_rdftype_id;  // only a global variable, but not configurable
int global_num_thread;  //=global_num_server+global_num_client
int global_multithread_factor;

bool global_use_rbf;
bool global_use_rdma;
int global_rdma_threshold;
int global_num_server;
int global_num_client;
int global_batch_factor;
string global_input_folder;
int global_client_mode;
bool global_use_loc_cache;
bool global_load_minimal_index;
bool global_silent;
int global_max_print_row;
int global_total_memory_gb;
int global_perslot_msg_mb;
int global_perslot_rdma_mb;
int global_hash_header_million;
int global_enable_workstealing;
int global_enable_index_partition;
int global_verbose;

bool global_enable_streaming;
bool global_remove_dup;
string global_csparql_folder;
string global_stream_source_config;
int global_max_adaptor;
int global_dispatch_max_interval;
int global_adaptor_max_throughput;
int global_adaptor_buffer_ntriple;
int global_num_stream_client;
string global_stream_query_config;

int global_stream_source_num;
int global_stream_query_num;
int global_stream_multithread_factor;

bool global_stream_query_silent;
bool global_server_debugging;
bool global_dispatch_debugging;
bool global_stream_TCP_test;
bool global_stream_query_throughput_test;
// @depricated bool global_enable_metadata_rdma;
int global_metadata_size_mb;
bool global_metadata_location_cache;

// tid for store threads
int global_coordinator_tid;
int global_dispatcher_tid;
int global_stream_serializer_tid;
// tid for query threads
int global_stream_client_tid;


std::string config_filename;
/* used in client_mode.cpp */
void load_changeable_cfg(){
	ifstream file(config_filename.c_str());
	string row;
	string val;
	if(!file){
		cout<<"Config file "<<config_filename<<" not exist"<<endl;
		exit(0);
	}
	map<string,string> config_map;
	// while(file>>row>>val){
	// 	config_map[row]=val;
	// }
	string line;
	while (std::getline(file, line)) {
	    istringstream iss(line);
		iss >> row>>val;
		config_map[row]=val;
	}

	global_batch_factor=atoi(config_map["global_batch_factor"].c_str());
	global_use_loc_cache=atoi(config_map["global_use_loc_cache"].c_str());
	global_silent=atoi(config_map["global_silent"].c_str());
	global_multithread_factor=atoi(config_map["global_multithread_factor"].c_str());
	global_rdma_threshold=atoi(config_map["global_rdma_threshold"].c_str());

}
/* used in main.cpp */
void load_global_cfg(char* filename){
	config_filename=std::string(filename);
	ifstream file(config_filename.c_str());
	global_rdftype_id=-1;
	string row;
	string val;
	if(!file){
		cout<<"Config file "<<config_filename<<" not exist"<<endl;
		exit(0);
	}
	map<string,string> config_map;
	// while(file>>row>>val){
	// 	config_map[row]=val;
	// }
	string line;
	while (std::getline(file, line)) {
	    istringstream iss(line);
		iss >> row>>val;
		config_map[row]=val;
	}
	global_use_rbf=atoi(config_map["global_use_rbf"].c_str());
	global_use_rdma=atoi(config_map["global_use_rdma"].c_str());
	global_rdma_threshold=atoi(config_map["global_rdma_threshold"].c_str());
	global_num_server=atoi(config_map["global_num_server"].c_str());
	global_num_client=atoi(config_map["global_num_client"].c_str());
	global_batch_factor=atoi(config_map["global_batch_factor"].c_str());
	global_multithread_factor=atoi(config_map["global_multithread_factor"].c_str());
	global_input_folder=config_map["global_input_folder"];
	global_client_mode=atoi(config_map["global_client_mode"].c_str());
	global_use_loc_cache=atoi(config_map["global_use_loc_cache"].c_str());
	global_load_minimal_index=atoi(config_map["global_load_minimal_index"].c_str());
	global_silent=atoi(config_map["global_silent"].c_str());
	global_max_print_row=atoi(config_map["global_max_print_row"].c_str());
	global_total_memory_gb=atoi(config_map["global_total_memory_gb"].c_str());
	global_perslot_msg_mb=atoi(config_map["global_perslot_msg_mb"].c_str());
	global_perslot_rdma_mb=atoi(config_map["global_perslot_rdma_mb"].c_str());
	global_hash_header_million=atoi(config_map["global_hash_header_million"].c_str());
	global_enable_workstealing=atoi(config_map["global_enable_workstealing"].c_str());
	global_enable_index_partition=atoi(config_map["global_enable_index_partition"].c_str());

	global_verbose=atoi(config_map["global_verbose"].c_str());

	// streaming related
    global_enable_streaming = atoi(config_map["global_enable_streaming"].c_str());
    global_remove_dup = atoi(config_map["global_remove_dup"].c_str());
    global_csparql_folder = config_map["global_csparql_folder"].c_str();
    global_stream_source_config = config_map["global_stream_source_config"].c_str();
        
    global_max_adaptor = atoi(config_map["global_max_adaptor"].c_str());
    global_adaptor_max_throughput = atoi(config_map["global_adaptor_max_throughput"].c_str());
    global_dispatch_max_interval = atoi(config_map["global_dispatch_max_interval"].c_str());

    global_num_stream_client = atoi(config_map["global_num_stream_client"].c_str());
    global_stream_query_config = config_map["global_stream_query_config"].c_str();
    global_stream_source_num = atoi(config_map["global_stream_source_num"].c_str());
    global_stream_query_num = atoi(config_map["global_stream_query_num"].c_str());

    global_stream_query_silent = atoi(config_map["global_stream_query_silent"].c_str());
    global_server_debugging = atoi(config_map["global_server_debugging"].c_str());
    global_dispatch_debugging = atoi(config_map["global_dispatch_debugging"].c_str());

    global_stream_TCP_test = atoi(config_map["global_stream_TCP_test"].c_str());
    global_stream_query_throughput_test = atoi(config_map["global_stream_query_throughput_test"].c_str());

    global_stream_multithread_factor = atoi(config_map["global_stream_multithread_factor"].c_str());

    // stream speed control
    global_adaptor_buffer_ntriple
        = (uint64_t)global_adaptor_max_throughput * global_dispatch_max_interval / 1000000 + 10; // 10 is redundancy

    // @depricated
    // global_enable_metadata_rdma = atoi(config_map["global_enable_metadata_rdma"].c_str());
    global_metadata_size_mb = atoi(config_map["global_metadata_size_mb"].c_str());
    global_metadata_location_cache = atoi(config_map["global_metadata_location_cache"].c_str());
}
