/**
 * @file malloc_naive.h
 * @brief Raw implementation of Wukong
 */

#pragma once
#include "malloc_interface.h"

extern const size_t malloc_unit_byte;

class malloc_naive : public malloc_interface{
private:
    void* start;
    uint64_t max_edge_ptr;
    uint64_t new_edge_ptr;
    pthread_spinlock_t allocation_lock;
    
public:
    void init(void* _start, uint64_t size_in_unit){
        start = _start;
        new_edge_ptr = 0;
        max_edge_ptr = size_in_unit;
        pthread_spin_init(&allocation_lock, 0);
    }

    uint64_t alloc(uint64_t size_in_unit){
        uint64_t curr_edge_ptr;
        
        pthread_spin_lock(&allocation_lock);
        curr_edge_ptr = new_edge_ptr;
        new_edge_ptr += size_in_unit;
        pthread_spin_unlock(&allocation_lock);
    
        if(new_edge_ptr >= max_edge_ptr){
            cout<<"atomic_alloc_edges out of memory !!!! "<<endl;
            exit(-1);
        }
        return curr_edge_ptr;
    }

    int free(uint64_t idx){
        // navie implementation doesn't support free
        // suitable for modification-free senarios
        cout << "naive_alloc doesn't support free";
        exit(-1);
    }

    uint64_t alloc_size(uint64_t index){
        cout << "naive_alloc doesn't support alloc_size";
        exit(-1);
    }

    void copy(uint64_t dst_idx, uint64_t src_idx, uint64_t size){
        cout << "naive_alloc doesn't support copy";
        exit(-1);
    }

    void print_memory_usage(){
        std::cout<<"graph_storage use "<<new_edge_ptr*sizeof(edge)/1048576<<"/"
            <<max_edge_ptr*sizeof(edge)/1048576<<" MB for edge data"<<std::endl;

    }
};
