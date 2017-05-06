/**
 * @file malloc_interface.h
 * @brief General malloc interface
 */

#pragma once

#include <iostream>
#include "graph_basic_types.h"

typedef edge malloc_unit;
const size_t malloc_unit_byte = sizeof(malloc_unit);

// NOTICE: any implentation of this interface should be *tread-safe*
class malloc_interface{
public:
    virtual void init(void* start, uint64_t size_in_unit) = 0;

    // return value: an index of starting unit
    virtual uint64_t alloc(uint64_t size_in_unit) = 0;
    virtual int free(uint64_t idx) = 0;
    virtual uint64_t alloc_size(uint64_t index) = 0;
    virtual void copy(uint64_t dst_idx, uint64_t src_idx, uint64_t size) = 0;

    virtual void print_memory_usage() = 0;
};
