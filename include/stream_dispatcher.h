/**
 * @file stream_dispatcher.h
 * @brief Dispatching time-labled triples from Adaptor to all machines.
 */

#pragma once

#include "global_cfg.h"
#include "thread_cfg.h"
#include "message_wrap.h"
#include "stream_coordinator.h"
#include "stream_adaptor_interface.h"
#include "stream_result_recorder.h"


/**
 * @brief Stream Dispatcher
 * 
 * Dispatcher is in charge of coordinating adaptors and dispatching the triples
 * to target machines by hash of subject and object.
 */
class stream_dispatcher{
 private:
    thread_cfg* cfg;
    int nadaptors, batch_id;
    stream_coordinator* coordinator;
    stream_adaptor_interface** adaptors;
    stream_dispatch_packet* send_buffer;

    /**
     * @brief Default constructor is not exposed
     */
    stream_dispatcher(){}

    /**
     * @brief Prepare buffer for a new round
     */
    void check_and_clear_buffer(int batch_id){
        for(int i = 0; i < cfg->m_num; i++){
            if (send_buffer[i].packet.size() != nadaptors){
                cout << "send_buffer.packet wrong size!" << endl;
                assert(false);
            }

            for(int j = 0; j < nadaptors; j++){
                send_buffer[i].packet[j].time.batch_id = batch_id;
                send_buffer[i].packet[j].items.clear();
            }
        }        
    }

    /**
     * @brief Fill the send buffer with Adaptor's buffer
     */
    int fill_buffer(){
        int total_send = 0;
        for(int i = 0; i < nadaptors; i++){
            uint32_t sz = adaptors[i]->buffer_size;
            total_send += sz;
                
            for(int j = 0; j < sz; j++){
                // target machine is determined by the hash of subject and object
                uint64_t s = adaptors[i]->buffer[j].s;
                uint64_t o = adaptors[i]->buffer[j].o;
                int world_size = cfg->m_num;
                adaptors[i]->buffer[j].m_id_s = mymath::hash_mod(s, world_size);
                adaptors[i]->buffer[j].m_id_o = mymath::hash_mod(o, world_size);

                uint32_t world0 = adaptors[i]->buffer[j].m_id_s;
                uint32_t world1 = adaptors[i]->buffer[j].m_id_o;
                
                send_buffer[world0].packet[i].items.push_back(adaptors[i]->buffer[j]);
                if (world0 != world1)
                    send_buffer[world1].packet[i].items.push_back(adaptors[i]->buffer[j]);
            }

            adaptors[i]->clear();
        }
        return total_send;
    }

 public:
    /**
     * @brief Constructor
     * @param _nadaptors adaptors assigned to this machine
     */
    stream_dispatcher(thread_cfg* _cfg, stream_coordinator* _coordinator,
                      stream_adaptor_interface** _adaptors, int _nadaptors):
    cfg(_cfg), coordinator(_coordinator), nadaptors(_nadaptors)
    {
        batch_id = 0;
        // a object local copy of adaptor information
        adaptors = new stream_adaptor_interface*[_nadaptors];
        memcpy(adaptors, _adaptors, sizeof(stream_adaptor_interface*) * _nadaptors);

        // initialize send buffer
        send_buffer = new stream_dispatch_packet[cfg->m_num];
        for(int i = 0; i < cfg->m_num; i++){
            for(int j = 0; j < nadaptors; j++){
                send_buffer[i].packet.push_back(stream_batch());
                send_buffer[i].packet[j].time.batch_id = 0;
                send_buffer[i].packet[j].time.stream_id = adaptors[j]->stream_id;

                // reserve enough memory
                int max_capacity = global_adaptor_max_throughput * global_stream_source_num;
                send_buffer[i].packet[j].items.reserve(max_capacity);
            }
        }
            
    }

    /**
     * @brief Dispatcher routine invoked at the beginning
     */
    void work(){
        // @debug
        // ensure the buffer is full in first round
        usleep(global_dispatch_max_interval * 2);
        
        while(true){
            // incremental batch number
            uint64_t start = timer::get_usec();
            
            batch_id++;
            check_and_clear_buffer(batch_id);

            // stop all adaptors
            for(int i = 0; i < nadaptors; i++)
                adaptors[i]->pause();

            // fill buffer and send
            int total_send = fill_buffer();
            for(int i = 0; i < cfg->m_num; i++){
                SendT(cfg, i, global_stream_serializer_tid, send_buffer[i]);
            }

            // user feedback
            if (total_send != 0){
                if (global_dispatch_debugging){
                    cout << "## Send batch" << batch_id << " : "  << total_send << " triples\n";
                }
            } else{
                // This might means all adaptors are dead
                // TODO: check and notify user
            }

            // coordinate
            coordinator->recv_global_steady_clock();
            coordinator->send_local_steady_clock();

            // resume all adaptors
            for(int i = 0; i < nadaptors; i++)
                adaptors[i]->resume();

            uint64_t usage = timer::get_usec() - start;
            if (global_dispatch_max_interval > usage)
                usleep(global_dispatch_max_interval - usage);
        }
    }
};
