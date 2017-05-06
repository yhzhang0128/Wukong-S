#include <boost/mpi.hpp>
#include <boost/serialization/string.hpp>

#include <iostream>
#include "utils.h"
#include "global_cfg.h"
#include "thread_cfg.h"
#include "string_server.h"
#include "distributed_graph.h"
#include "server.h"
#include "client.h"
#include "client_mode.h"

#include "stream_adaptor_interface.h"
#include "stream_adaptor_readfile.h"
#include "stream_dispatcher.h"
#include "stream_coordinator.h"
#include "stream_serializer.h"
#include "stream_ingestor.h"

#include "csparql_manager.h"
#include "metadata_manager.h"
#include "stream_query_client.h"

#include "kv_fetch_rpc.h"
#include "stream_result_recorder.h"
// streaming threads
csparqlManager *csparql_manager;
metadataManager *metadata_manager;
stream_resultRecorder *stream_result_recorder;
stream_query_client **stream_query_clients;
kv_fetch_rpc *kv_fetch_thread;
// metadata maintainer
int batch_num_reserved; 
int batch_size_reserved;
vector<metadata_batch> *stream_metadata;
//unordered_map<uint64_t, memarea_slot>*** metadata_local_cache;

using namespace std;

/* old configuration on cube cluster
int socket_0[] = {
  0,2,4,6,8,10,12,14,16,18
};

int socket_1[] = {
  1,3,5,7,9,11,13,15,17,19,0,2,4,6,8,10,12,14,16,18
};
*/

static int socket_0[] =  {
    0,2,4,6,8,10,12,14,16,18,20,22
};

static int socket_1[] = {
    1,3,5,7,9,11,13,15,17,19,21,23
};

static int socket_all[] = {
    0,2,4,6,8,10,12,14,16,18,20,22,1,3,5,7,9,11,13,15,17,19,21,23
};

static bool coreused[24];
void pin_to_core(size_t core, bool checkdup=true) {
    if (checkdup){
        if (core >= 24 || coreused[core]){
            cout << "Already pined to core" << core << endl;
            assert(false);
        }
        coreused[core] = true;
    }
    
  cpu_set_t  mask;
  CPU_ZERO(&mask);
  CPU_SET(core, &mask);
  int result=sched_setaffinity(0, sizeof(mask), &mask);
}

// start registered threads
void* Run(void *ptr) {
	struct thread_cfg *cfg = (struct thread_cfg*) ptr;

    // old cube cluster configuration
    // socket_1[0]: adaptor + ingestor
    // socket_1[1]: dispatcher
    // socket_1[2..19]: client, server, stream_client

    // new val cluster configuration
    // latency test:
    // socket_0: 12 cores
    // adaptor+dispatcher, ingestor, client, stream_client, server * 8
    // socket_1: 12 cores
    // server * 12

    size_t dispatcher_core = 0;
    size_t executor_base = 2;
    
	if(cfg->t_id < cfg->client_num){
        // client
        pin_to_core( socket_all[executor_base + cfg->t_id] );

        iterative_shell(((client*)(cfg->ptr)));
        assert(global_num_client == 1);
        // example bad
        //storm_execute_sample_query_bad((client*)(cfg->ptr));
        // example good
        //storm_execute_sample_query_good((client*)(cfg->ptr));
	} else if (cfg->t_id < cfg->client_num + cfg->server_num){
        // server
        pin_to_core( socket_all[executor_base + global_num_stream_client + cfg->t_id] );
		((server*)(cfg->ptr))->run();
	} else if (cfg->t_id == global_dispatcher_tid){
        // dispatcher
        pin_to_core( socket_all[dispatcher_core], false );
        ((stream_dispatcher*)cfg->ptr)->work();
    } else if (cfg->t_id < global_stream_client_tid + global_num_stream_client){
        // query client
        pin_to_core( socket_all[executor_base + global_num_client +
                              cfg->t_id - global_stream_client_tid] );

        ((stream_query_client*)cfg->ptr)->master();
    } else{
        // obsolete design, no metadata replication
        assert(false);
        //pin_to_core( socket_1[19] );
        //((kv_fetch_rpc*)cfg->ptr)->run();
    }
}

// start stream adaptor thread
void* adaptor_run(void *ptr){
    stream_adaptor_interface *adaptor = (stream_adaptor_interface*) ptr;
    pin_to_core(socket_all[0], false);
    adaptor->work();
}

// start ingestor thread
void* ingestor_run(void *ptr){
    stream_ingestor *ingestor = (stream_ingestor*) ptr;
    pin_to_core(socket_all[1], false);
    ingestor->work();
}

void thread_allocation(){
    global_num_thread = global_num_server + global_num_client;
    if (global_enable_streaming){
        // thread_id assignment
        global_coordinator_tid = global_num_thread + 0;
        global_dispatcher_tid = global_num_thread + 1;
        global_stream_serializer_tid = global_num_thread + 2;

        global_stream_client_tid = global_num_thread + 3;

        // coordinator * 1
        // dispatcher * 1
        // serializer * 1
        // stream query client * many
        global_num_thread += 3 + global_num_stream_client;

        // 2 cores for adaptor, dispatcher, ingestor
        // reserve 2 cores for adaptor, ingestor, etc.
        int query_worker_cores = 24 - 2;

        /*
        if (global_stream_TCP_test){
            // kv_fetch_rpc thread
            global_num_thread += 1;
            query_worker_cores -= 1;
            // only support single thread for test purpose
            assert(global_num_stream_client == 1);
        }
        */
        
        assert(global_num_server +
               global_num_client +
               global_num_stream_client
               <= query_worker_cores);
    }
}


void test_java_zmq(){
    zmq::context_t context(1);
    zmq::socket_t* receiver;

    receiver = new zmq::socket_t(context, ZMQ_REP);
    receiver->bind("tcp://*:10090");

    cout << "Start!" << endl;
    while(true){
        zmq::message_t request;

        receiver->recv(&request);
        cout << "Recv: " << (char*)request.data() << endl;

        zmq::message_t reply(4);
        memcpy (reply.data(), "Ack!", 4);
        receiver->send(reply);
    }
}

int main(int argc, char * argv[]) {
    uint64_t init_start_time = timer::get_usec();

    if(argc !=3) {
        cout<<"usage:./wukong config_file hostfile"<<endl;
        return -1;
    }

    //test_java_zmq();
    //return 0;

    /****** Load global configurations ******/
    load_global_cfg(argv[1]);

    /****** Initialize world ******/
    boost::mpi::environment env(argc, argv);
    boost::mpi::communicator world;

    /****** Calculate memory size of each area ******/
    thread_allocation();
    
    uint64_t rdma_size = 1024*1024*1024;
    rdma_size = rdma_size*global_total_memory_gb;
    uint64_t msg_slot_per_thread= 1024*1024*global_perslot_msg_mb;
    uint64_t rdma_slot_per_thread =1024*1024* global_perslot_rdma_mb;

    uint64_t metadata_rdma = 0;
    if (!global_metadata_location_cache)
        metadata_rdma = 1024 * 1024 * global_metadata_size_mb;
    
    uint64_t total_size = rdma_size +
        rdma_slot_per_thread * global_num_thread +
        msg_slot_per_thread * global_num_thread +
        metadata_rdma;

    /****** Create network communication node, both zmq & rdma ******/
    Network_Node *node = new Network_Node(world.size(), world.rank(), global_num_thread,string(argv[2]));

    //Network_Node *node = new Network_Node(world.size(), world.rank(),global_num_thread,string(argv[2]));
    char *buffer= (char*) malloc(total_size);
    memset(buffer,0,total_size);
    RdmaResource *rdma=new RdmaResource(world.size(),global_num_thread,
	world.rank(),buffer,total_size,rdma_slot_per_thread,msg_slot_per_thread,rdma_size);
    rdma->node = node;
    rdma->Servicing();
    rdma->Connect();

    uint64_t init_mid_time1 = timer::get_usec();
    cout << "############ Machine" << world.rank() << " takes " 
         << (init_mid_time1 - init_start_time) / 1000000 << " second init rdma\n";
    MPI_Barrier(MPI_COMM_WORLD);

    /****** Initialize str_server and graph ******/
	distributed_graph graph(world, rdma, global_input_folder);
    uint64_t init_mid_time2 = timer::get_usec();
    cout << "############ Machine" << world.rank() << " takes " 
         << (init_mid_time2 - init_mid_time1) / 1000000 << " second init graph\n";
    MPI_Barrier(MPI_COMM_WORLD);

    string_server str_server(global_input_folder);
    uint64_t init_mid_time3 = timer::get_usec();
    cout << "############ Machine" << world.rank() << " takes " 
         << (init_mid_time3 - init_mid_time2) / 1000000 << " second init dictionary\n";
    MPI_Barrier(MPI_COMM_WORLD);

    /****** Initialize thread info ******/
    thread_cfg* cfg_array= new thread_cfg[global_num_thread];
    for(int i=0;i<global_num_thread;i++){
        cfg_array[i].t_id=i;
        cfg_array[i].t_num=global_num_thread;
        cfg_array[i].m_id=world.rank();
        cfg_array[i].m_num=world.size();
        cfg_array[i].client_num=global_num_client;
        cfg_array[i].server_num=global_num_server;
        cfg_array[i].rdma=rdma;
        cfg_array[i].node=new Network_Node(world.size(), cfg_array[i].m_id,cfg_array[i].t_id,string(argv[2]));
        cfg_array[i].init();
    }

    uint64_t init_end_time = timer::get_usec();
    cout << "############ Machine" << world.rank() << " takes " 
         << (init_end_time - init_start_time) / 1000000 << " seconds totally for init\n";
    MPI_Barrier(MPI_COMM_WORLD);

    /****** Prepare objects for each thread ******/
    // client & server
	client** client_array=new client*[global_num_client];
	for(int i=0;i<global_num_client;i++){
		client_array[i]=new client(&cfg_array[i],&str_server);
	}
	server** server_array=new server*[global_num_server];
	for(int i=0;i<global_num_server;i++){
		server_array[i]=new server(graph,&cfg_array[global_num_client+i]);
	}
	for(int i=0;i<global_num_server;i++){
		server_array[i]->set_server_array(server_array);
	}
    // stream related threads
    int nadaptors = 0;
    stream_adaptor_interface** adaptors = new stream_adaptor_interface*[global_max_adaptor];
    stream_dispatcher *dispatcher;
    stream_coordinator *coordinator;
    stream_serializer *serializer;
    stream_ingestor *ingestor;

    if (global_enable_streaming){
        // adaptors created here
        csparql_manager = new csparqlManager(&str_server, world, adaptors, nadaptors);
        
        stream_result_recorder = new stream_resultRecorder();
        
        // check
        if (global_dispatch_max_interval < 1000 ||
            global_dispatch_max_interval % 1000 != 0 ||
            global_adaptor_buffer_ntriple == 0){
            cout << "please check window_size or window_step" << endl;
            return 0;
        }
        
        // metadata_manager uses csparql_manager in construction
        if (global_metadata_location_cache){
            // not in RDMA area
            char *hashmap_ptr = (char*) malloc(1024 * 1024 * global_metadata_size_mb);
            metadata_manager = new metadataManager(hashmap_ptr, rdma, world);
        } else{
            metadata_manager = new metadataManager(buffer + (total_size - metadata_rdma), rdma, world);
        }
        cout << "Adaptor max throughput: " << global_adaptor_max_throughput << endl;
        cout << "Dispatch interval: " << global_dispatch_max_interval << endl;
        cout << "Adaptor buffer size: " << global_adaptor_buffer_ntriple << endl;

        // prepare coordinator
        coordinator = new stream_coordinator(&cfg_array[global_coordinator_tid], world);
        // prepare dispatcher
        dispatcher = new stream_dispatcher(&cfg_array[global_dispatcher_tid], 
                                           coordinator, adaptors, nadaptors);
        // prepare serializer
        serializer = new stream_serializer(&cfg_array[global_stream_serializer_tid]);
        // prepare ingestor
        ingestor = new stream_ingestor(world.rank(), serializer, coordinator, &(graph.local_storage));
        
        // prepare stream query client
        stream_query_clients = new stream_query_client*[global_num_stream_client];

        for(int i = 0; i < global_num_stream_client; i++){
            stream_query_clients[i] = new stream_query_client(&cfg_array[global_stream_client_tid + i], coordinator);
        }

        if (global_stream_TCP_test){
            assert(global_stream_client_tid + global_num_stream_client
                   == global_num_thread - 1);
            kv_fetch_thread = new kv_fetch_rpc(&cfg_array[global_num_thread - 1], &graph.local_storage);
        } else{
            assert(global_stream_client_tid + global_num_stream_client
                   == global_num_thread);
        }

    }

    // @debug
    // cout << "###Cube" << world.rank() << endl;
    // graph.local_storage.print_memory_usage();
    // return 0;

    /****** Launch all threads ******/
    // Unregistered threads
    pthread_t *unregister_thread;
    if (global_enable_streaming){
        unregister_thread = new pthread_t[nadaptors + 1];
        // ingestor
        pthread_create(&(unregister_thread[0]), NULL, ingestor_run, (void *)ingestor);

        // adaptors
        for(int i = 1; i <= nadaptors; i++){
            pthread_create (&(unregister_thread[i]), NULL, adaptor_run, (void *)adaptors[i - 1]);
        }
    }

    // Registered threads
    pthread_t *thread  = new pthread_t[global_num_thread];
	for(size_t id = 0;id < global_num_thread;++id) {
		if(id < global_num_client){
			cfg_array[id].ptr = client_array[id];
		} else if (id < global_num_client + global_num_server) {
			cfg_array[id].ptr = server_array[id - global_num_client];
		} else if (id == global_coordinator_tid) {
            cfg_array[id].ptr = coordinator;
            // no coordinator thread
            continue;
        } else if (id == global_dispatcher_tid) {
            cfg_array[id].ptr = dispatcher;
        } else if (id == global_stream_serializer_tid){
            cfg_array[id].ptr = serializer;
            // no serializer thread
            continue;
        } else if (id < global_stream_client_tid + global_num_stream_client){
            cfg_array[id].ptr = stream_query_clients[id - global_stream_client_tid];
        } else{
            cfg_array[id].ptr = kv_fetch_thread;
        }

		pthread_create (&(thread[id]), NULL, Run, (void *) &(cfg_array[id]));
	}

    // main should not affect performance
    pin_to_core(socket_all[0], false);

	for(size_t t = 0 ; t < global_num_thread; t++) {
		int rc = pthread_join(thread[t], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}

    for(size_t t = 0; t < nadaptors + 1; t++){
        int rc = pthread_join(unregister_thread[t], NULL);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
    }
    
	return 0;
}
