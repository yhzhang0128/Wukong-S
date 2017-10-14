/**
 * @file graph_basic_types.h
 * @brief Data structures for RDF graph in hash table
 */


#pragma once
#include "utils.h"
#include <functional>
#include <iostream>
#include "global_cfg.h"

struct edge_triple{
	uint64_t s;
	uint64_t p;
	uint64_t o;
	edge_triple(uint64_t _s,uint64_t _p, uint64_t _o): s(_s),p(_p),o(_o){

	}
	edge_triple(): s(-1),p(-1),o(-1){

	}
};
struct edge_sort_by_spo {
    inline bool operator() (const edge_triple& struct1, const edge_triple& struct2) {
        if(struct1.s < struct2.s){
			return true;
		} else if(struct1.s == struct2.s) {
			if(struct1.p < struct2.p){
				return true;
			} else if(struct1.p == struct2.p && struct1.o < struct2.o){
				return true;
			}
		}
		//otherwise
		return false;
	}
};
struct edge_sort_by_ops {
    inline bool operator() (const edge_triple& struct1, const edge_triple& struct2) {
        if(struct1.o < struct2.o){
			return true;
		} else if(struct1.o == struct2.o){
			if(struct1.p < struct2.p){
				return true;
			} else if(struct1.p == struct2.p && struct1.s < struct2.s){
				return true;
			}
		}
		//otherwise
		return false;
	}
};

const int nbit_predict=17;
const int nbit_id=63-nbit_predict;
static inline bool is_index_vertex(int id){
	return id< (1<<nbit_predict);
}
struct local_key{
	uint64_t dir:1;
	uint64_t predict: nbit_predict;
	uint64_t id: nbit_id;

	local_key():dir(0),predict(0),id(0){
		dir-=1;
		predict-=1;
		id-=1;
	}
	void print(){
		std::cout<<"("<<id<<","<<dir<<","<<predict<<")"<<std::endl;
	}

    inline uint64_t archive() const{
        uint64_t r=0;
        r+=dir;
        r<<=nbit_predict;
        r+=predict;
        r<<=nbit_id;
        r+=id;
        return r;
    }
    static local_key unarchive(uint64_t key) {
        uint32_t id = key & ((1LL << nbit_id) - 1);
        key >>= nbit_id;
        uint32_t predict = key & ((1LL << nbit_predict) - 1);
        key >>= nbit_predict;
        return local_key(id, key, predict);
    }

	uint64_t hash(){
		uint64_t r = archive();
		//return std::hash<uint64_t>()(r);
		return mymath::hash(r);
	}
	local_key(uint64_t i,uint64_t d,uint64_t p):id(i),dir(d),predict(p){
		if(id!=i || dir!=d || predict !=p){
			std::cout<<"truncated: "<<"("<<i<<","<<d<<","<<p<<")=>"
									<<"("<<id<<","<<dir<<","<<predict<<")"<<std::endl;
		}
	}
	bool operator==(const local_key& another_key){
		if(dir==another_key.dir
            && predict==another_key.predict
            && id==another_key.id){
			    return true;
		}
		return false;
	}

	bool operator!=(const local_key& another_key){
		return !(operator==(another_key));
	}
};

const int nbit_memarea_pointer = 36;
const int nbit_memarea_offset = 28;
/**
 * @brief Specify an area in edge memory
 *
 * sizeof(edge_memarea) == 8byte is important for archive
 * class in stream_basic_types
 */
struct edge_memarea{
	uint64_t start_ptr:nbit_memarea_pointer;
	uint64_t offset:nbit_memarea_offset;
    edge_memarea(): start_ptr(0), offset(0) {}
};

struct local_val{
	struct edge_memarea  obsolete, read, write;

	// control variables
	uint32_t version;
  /*
	inline void add_size(uint64_t delta){
		obsolete.offset+=delta;
		read.offset+=delta;
		write.offset+=delta;
	}
  */
    inline void flush(){
        obsolete = read;
        read = write;
    }

    inline edge_memarea get_readarea(uint32_t query_version = 0){
        flush();
        return read;
	/*
        if (query_version == version)
            return read;
        else
            return obsolete;
	*/
    }
    inline edge_memarea get_writearea(){
        return write;
    }
    inline void set_writearea(uint64_t off, uint64_t ptr){
        write.start_ptr = ptr;
        write.offset = off;
    }
  /*
	inline uint64_t size(uint32_t query_version = 0){
		if (query_version == version)
			return read.offset;
		else
			return obsolete.offset;
	}
	inline uint64_t ptr(uint32_t query_version = 0){
		if (query_version == version)
			return read.start_ptr;
		else
			return obsolete.start_ptr;
	}
  */

	void init(){
		version = 0;
		obsolete.start_ptr = read.start_ptr = write.start_ptr = 0;
		obsolete.offset    = read.offset    = write.offset    = 0;
	}
	local_val(){
		init();
	}
	local_val(uint64_t s,uint64_t p){
		init();
		obsolete.start_ptr = read.start_ptr = write.start_ptr = p;
		obsolete.offset = read.offset = write.offset = s;
	}
  inline bool is_null(){
    return read.offset == 0;
  }

	bool operator==(const local_val& another_val){
		if(read.offset == another_val.read.offset
            &&  read.start_ptr ==another_val.read.start_ptr){
			return true;
		}
		return false;
	}
	bool operator!=(const local_val& another_val){
		return !(operator==(another_val));
	}
};

struct vertex{
	local_key key;
	local_val val;
	pthread_spinlock_t realloc_lock;
    void init(){
      pthread_spin_init(&realloc_lock, 0);
      val = local_val();
    }
};
struct edge{
    //	uint64_t val;
    unsigned int val;
};
enum direction{
	direction_in,
	direction_out,
	join_cmd
};
