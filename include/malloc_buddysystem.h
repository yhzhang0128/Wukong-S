/**
 * @file malloc_buddysystem.h
 * @brief Buddy System implementation of malloc_interface
 *
 * ATTENTION! any pointer or counter should be LESS than 2^48 and LARGER than 4
 *
 ## block layout:
 |-----------------------|--------------------------------------|
 |         header        |             data for user            |
 | unit_per_header units | 2^header.level-unit_per_header units |
 |-----------------------|--------------------------------------|
                         ^
                         |----------------
 alloc method should return this Location |

 ## memory layout:
 |----------------|-----------------------|
 | free_list area |      blocks area      |
 |     64 unit    | total_memory-64 units |
 |----------------|-----------------------|
 
 ### Explanation of free_list area:
 free_list is a list of headers to maintain bi-direction link-lists
 for now, each header occupies 2 units which is 16 bytes
 this area is the first block in total_memory
 |-----------------------------------block0------------------------------------|
 | header for block0 | level2 header | level3 header | ...... | level32 header |
 |  2units / 16bytes |    16bytes    |    16bytes    | ...... |     16bytes    |
 |--------------------------------Total 64 bytes-------------------------------|
 After initialization, block0 should have a *buddy* [unit64, unit128)
 this buddy should be in the free_list of level6 since 128-64==1<<6
 this block should never be freed
 */

#pragma once
#include <iomanip>
#include "malloc_interface.h"

// defined in malloc interface
extern const size_t malloc_unit_byte;

class malloc_buddysystem : public malloc_interface{
private:
    // block size >= 2^level_low_bound units
    static const uint64_t level_low_bound = 2;
    // block size <= 2^level_up_bound units
    static const uint64_t level_up_bound = 48;

    // lock for thread-safe
    pthread_spinlock_t allocation_lock;

    // prev_free_idx and next_free_idx are for bidirection link-list
    struct header{
        uint64_t in_use:1;
        uint64_t level:6;
        uint64_t prev_free_idx:48;
        uint64_t next_free_idx:48;
        uint64_t align:25;
    } __attribute__((packed));

    // unit_per_header = sizeof(header) / sizeof(malloc_unit)
    uint64_t unit_per_header;

    malloc_unit* start_ptr;
    // free_list[i] points to the first free block with size 2^i units
    header* free_list[level_up_bound + 1];

    uint64_t total_malloc_unit;
    uint64_t usage_counter[level_up_bound + 1];

    // interface is in unit style, but pointer style is useful
    inline malloc_unit* idx_to_ptr(uint64_t idx){
        return start_ptr + idx;
    }
    inline uint64_t ptr_to_idx(malloc_unit* ptr){
        return (uint64_t)(ptr - start_ptr);
    }
    inline bool is_empty(uint64_t level){
        return free_list[level]->next_free_idx == ptr_to_idx((malloc_unit*) free_list[level]);
    }

    // convert a size into level. useful for alloc
    inline uint64_t truncate_level(uint64_t level);
    inline uint64_t size_to_level(uint64_t size_in_unit);

    // methods useful for alloc and free
    inline void mark_free(header* start, uint64_t level);
    inline void mark_used(header* start, uint64_t level);

public:
    void init(void* start, uint64_t size_in_unit);

    // return value: an index of starting unit
    uint64_t alloc(uint64_t size_in_unit);
    int free(uint64_t idx);
    uint64_t alloc_size(uint64_t index);
    void copy(uint64_t dst_idx, uint64_t src_idx, uint64_t size);

    void print_memory_usage();
};
