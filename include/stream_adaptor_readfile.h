/**
 * @file stream_adaptor_readfile.h
 * @brief Adaptor for reading files through Linux file system interface.
 *
 * This adaptor guarantees no malloc after initialization.
 */

#pragma once

#include <fstream>
#include "global_cfg.h"
#include "stream_adaptor_interface.h"

/**
 * @brief Adaptor for reading normal files
 */
class stream_adaptor_readfile : public stream_adaptor_interface{
 private:
    bool pause_cmd; /** receive pause command  */
    bool is_pause; /** pause status */
    bool is_dead;  /** a stream is dead when reaching the end of file */

    /**
     * @brief Parse a file into a stream
     *
     * the throughput is determined by both
     *     global_adaptor_buffer_ntriple
     * and global_dispatch_max_interval
     */
    void prepare_buffer(){
        ifstream input((global_input_folder + stream_name).c_str());

        // check if file exist
        if (input.good() == false){
            cout << "Adaptor" + stream_name + " Fail\n";
            return;
        }
        
        stream_rate = (uint64_t)stream_rate * global_dispatch_max_interval / 1000000;
        cout << "Adaptor " + stream_name + " OK, rate: " << stream_rate << endl;
            
        uint64_t s, p, o;
        while(true){
            if (!input.good()){
                cout << "Adaptor " + stream_name + " finished\n";
                break;
            }

            // wait until not pausing
            if (pause_cmd) is_pause = true;
            while (is_pause)
                asm volatile("pause\n":::"memory");

            if (buffer_size == stream_rate && stream_rate != 0){
                // buffer is full
                is_pause = true;
            } else{
                input >> s >> p >> o;

                if (stream_rate == 0 && s == 0 && p == 0 && o == 0){
                    is_pause = true;
                    continue;
                }

                /* move to dispatcher */
                /* buffer[buffer_size].m_id_s = mymath::hash_mod(s, world_size); */
                /* buffer[buffer_size].m_id_o = mymath::hash_mod(o, world_size); */

                buffer[buffer_size].s = s;
                buffer[buffer_size].p = p;
                buffer[buffer_size].o = o;
                
                buffer_size++;
                assert(buffer_size <= global_adaptor_max_throughput);
            }
        }
        is_dead = true;
    }

    /**
     * @brief Default constructor is not exposed
     */
    stream_adaptor_readfile() {}

 public:

 stream_adaptor_readfile(int _id, string _name,  int _rate, int _world_size):
    pause_cmd(false), is_pause(false), is_dead(false){
        // variables defined in super class
        buffer_size = 0;
        stream_id = _id;
        stream_name = _name;
        stream_rate = _rate;
    }

    /**
     * @brief Pause by dispatcher
     */
    void pause() { 
        if (is_dead) return;
        //pause_cmd = true;

        while(!is_pause)
            asm volatile("pause\n":::"memory");
    }

    /**
     * @brief Resume by dispatcher
     */
    void resume() {
        //cout << stream_name << " resume" << endl;
        pause_cmd = false;
        is_pause = false;
    }

    /**
     * @brief Notified by dispatcher that buffer is cleared
     */
    void clear(){
        //cout << stream_name << " clear\n";
        buffer_size = 0;
        //buffer.clear();
    }

    /**
     * @brief Invoked when a stream is registered.
     *
     * Currently it is at the beginning.
     */
    void work(){
        // reserve enough space for buffer
        if (stream_rate == 0){
            buffer.resize(global_adaptor_max_throughput);
        } else{
            buffer.resize(stream_rate);
        }
        prepare_buffer();
    }

};
