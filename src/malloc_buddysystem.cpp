/**
 * @file malloc_buddysystem.cpp
 * @brief Buddy System implementation of malloc_interface
 */

#include "malloc_buddysystem.h"

// convert a size into level. useful for alloc
inline uint64_t malloc_buddysystem::truncate_level(uint64_t level){
    if (level < level_low_bound) 
        return level_low_bound;
    if (level > level_up_bound) 
        assert(false); // should be less than level_up_bound
    return level;
}

inline uint64_t malloc_buddysystem::size_to_level(uint64_t size_in_unit){
    uint64_t level = 0, tmp = 1;
    size_in_unit += unit_per_header;
    while(true){
        tmp <<= 1;
        level++;
        if (tmp >= size_in_unit) break;
    }
    return truncate_level(level);
}

uint64_t malloc_buddysystem::alloc_size(uint64_t index){
    header *h = (header*)idx_to_ptr(index - unit_per_header);
    return (1 << (h->level)) - unit_per_header;
}

void malloc_buddysystem::copy(uint64_t dst_idx, uint64_t src_idx, uint64_t size){
    memcpy(idx_to_ptr(dst_idx), idx_to_ptr(src_idx), size * sizeof(malloc_unit));
}

// methods useful for alloc and free
inline void malloc_buddysystem::mark_free(header* start, uint64_t level){
    start->in_use = 0;
    start->level = level;
    
    // add to free list
    // free_list[level] <--> start <--> free_list[level]->next_free_idx
    header* prev = free_list[level];
    header* next = (header*)idx_to_ptr(free_list[level]->next_free_idx);

    uint64_t prev_idx = ptr_to_idx((malloc_unit*) free_list[level]);
    uint64_t this_idx = ptr_to_idx((malloc_unit*) start);
    uint64_t next_idx = free_list[level]->next_free_idx;

    // maintain bi-direction link list
    prev->next_free_idx = this_idx;
    next->prev_free_idx = this_idx;
    start->prev_free_idx = prev_idx;
    start->next_free_idx = next_idx;
}

inline void malloc_buddysystem::mark_used(header* start, uint64_t level){
    start->in_use = 1;
    start->level = level;

    // remove from free list
    // start->prev_free_idx <--> start <--> start->next_free_idx
    header* prev = (header*)idx_to_ptr(start->prev_free_idx);
    header* next = (header*)idx_to_ptr(start->next_free_idx);
    // maintain bi-direction link list
    prev->next_free_idx = start->next_free_idx;
    next->prev_free_idx = start->prev_free_idx;
}

void malloc_buddysystem::init(void* start, uint64_t size_in_unit){
    assert(level_low_bound >= 2);
    assert(level_up_bound <= 48);
    assert(size_in_unit <= 1LL << 48);

    total_malloc_unit = 0;
    memset(usage_counter, 0, sizeof(usage_counter));
    // alloc enough space for header
    unit_per_header = sizeof(header) % sizeof(malloc_unit)?
                      (sizeof(header) / sizeof(malloc_unit)) + 1 :
                      (sizeof(header) / sizeof(malloc_unit));
    start_ptr = (malloc_unit*) start;
    pthread_spin_init(&allocation_lock, 0);

    // see block0 layout in header file
    for(int i = level_low_bound; i <= level_up_bound; i++){
        uint64_t idx = (i - 1) * unit_per_header;

        free_list[i] = (header*) idx_to_ptr(idx);
        free_list[i]->prev_free_idx = free_list[i]->next_free_idx = idx;
    }

    // header for block0
    uint64_t level = size_to_level(size_in_unit - unit_per_header);
    // size_to_level is upper bound, but here needs lower bound
    if ((1LL << level) > size_in_unit - unit_per_header)
        level--;
    mark_free((header*) start, level);
    
    // the first block(block0) is memory for free_list
    alloc(level_up_bound * unit_per_header - unit_per_header);

    cout << "Actural edge num: " << size_in_unit << endl;
    cout << "Maximum level: " << level << endl;
    cout << "Header size: " << sizeof(header) << endl;
    cout << "Header unit size: " << unit_per_header << endl;
}

// return value: an index of starting unit
uint64_t malloc_buddysystem::alloc(uint64_t size_in_unit){
    uint64_t need_level = size_to_level(size_in_unit);
    uint64_t free_level;
    int64_t free_idx = -1;

    pthread_spin_lock(&allocation_lock);
    // find the smallest available block
    for(free_level = need_level; free_level <= level_up_bound; free_level++){
        if (is_empty(free_level))
            continue;
        free_idx = free_list[free_level]->next_free_idx;
        break;
    }

    if (free_idx == -1){
        // no block big enough
        std::cout << "malloc_buddysystem: memory is full" << std::endl;
        print_memory_usage();
        assert(false);
        //return -1;
    }

    // split larger block
    for(uint64_t i = free_level-1; i >= need_level; i--){
        //std::cout << "block " << free_idx + (1LL<<i) << " is marked free with level " << i << std::endl;
        mark_free((header*)idx_to_ptr(free_idx + (1LL<<i)), i);
    }
    mark_used((header*)idx_to_ptr(free_idx), need_level);
    pthread_spin_unlock(&allocation_lock);

    usage_counter[need_level]++;
    total_malloc_unit += (1 << free_level);

    return free_idx + unit_per_header;
}

int malloc_buddysystem::free(uint64_t idx){
    // currently not need
    assert(false);
    // trying to free free_list which doesn't belong to user
    if (idx == 0)
        return -1;

    // TODO
    return 0;
}

void malloc_buddysystem::print_memory_usage(){
    std::cout<<"graph_storage edge memory status:" << std::endl;
    uint64_t unit_count = 0;

    for(int i = level_low_bound; i <= level_up_bound; i++){
        std::cout << "level" << std::setw(2) << i << ": " << std::setw(10) << usage_counter[i] << "|\t";
        if ((i - level_low_bound + 1)%4 == 0) std::cout << std::endl;

        unit_count += (1LL << i) * usage_counter[i];
    }

    cout << "\nUnit count: " << unit_count << endl; 
    cout << "Memory used: " << total_malloc_unit * sizeof(malloc_unit) / 1024 / 1024 / 1024 << " GB\n";
    std::cout << std::endl;
}
