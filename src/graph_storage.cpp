/**
 * @file graph_storage.cpp
 * @brief Methods in graph_storage class
 */

#include "graph_storage.h"

graph_storage::graph_storage(){
    // @debug
    slot_used = 0;
    malloc_mem_used = 0;
    
    pthread_spin_init(&allocation_lock,0);
    for(int i=0;i<num_locks;i++){
        pthread_spin_init(&fine_grain_locks[i],0);
    }
};

void graph_storage::init(RdmaResource* _rdma,uint64_t machine_num,uint64_t machine_id){
    rdma        =_rdma;
    m_num       =machine_num;
    m_id        =machine_id;
    slot_num    =1000000*global_hash_header_million;
    header_num	=(slot_num/cluster_size)/indirect_ratio*(indirect_ratio-1);
    indirect_num=(slot_num/cluster_size)/indirect_ratio;

    vertex_addr	=(vertex*)(rdma->get_buffer());
    edge_addr	=(edge*)(rdma->get_buffer()+slot_num*sizeof(vertex));

    if(rdma->get_memorystore_size()<=slot_num*sizeof(vertex)) {
        std::cout<<"No enough memory to store edge"<<std::endl;
        exit(-1);
    }

    uint64_t max_edge_ptr=(rdma->get_memorystore_size()-slot_num*sizeof(vertex))/sizeof(edge);
    edge_manager = new malloc_buddysystem();
    edge_manager->init((void*)edge_addr, max_edge_ptr);

    #pragma omp parallel for num_threads(20)
    for(uint64_t i=0;i<slot_num;i++){
        vertex_addr[i].key=local_key();
    }
    // if(global_use_loc_cache){
    // 	assert(false);
    // }
}

uint64_t graph_storage::insertKey(local_key key, bool check_dup = true){
	uint64_t vertex_ptr;
	uint64_t bucket_id=key.hash()%header_num;
	uint64_t lock_id=bucket_id% num_locks;
	uint64_t slot_id=0;
	bool found=false;
	pthread_spin_lock(&fine_grain_locks[lock_id]);
	//last slot is used as next pointer
	while(!found){
		for(uint64_t i=0;i<cluster_size-1;i++){
			slot_id=bucket_id*cluster_size+i;
			if(vertex_addr[slot_id].key==key){

			  if (check_dup){
				cout<<"inserting duplicate key" <<endl;
				key.print();
				assert(false);
			  } else{
			    found = true;
			    break;
			  }
			}
			if(vertex_addr[slot_id].key==local_key()){
				vertex_addr[slot_id].init();
				vertex_addr[slot_id].key=key;
				//vertex_addr[slot_id].val=local_val();
				found=true;
				break;
			}
		}
		if(found){
			break;
		} else {
		  // The last slot in this bucket, indirect pointer
			slot_id=bucket_id*cluster_size+cluster_size-1;
			if(vertex_addr[slot_id].key!=local_key()){
			  // follow the indirect pointer
				bucket_id=vertex_addr[slot_id].key.id;
				//continue and jump to next bucket
				continue;
			} else {
			  // create a new bucket for this key
				pthread_spin_lock(&allocation_lock);
				if(used_indirect_num>=indirect_num){
					assert(false);
				}
				vertex_addr[slot_id].key.id=header_num+used_indirect_num;
				used_indirect_num++;
				pthread_spin_unlock(&allocation_lock);

				// insert key into this new bucket
				bucket_id=vertex_addr[slot_id].key.id;
				slot_id=bucket_id*cluster_size+0;
				vertex_addr[slot_id].init();
				vertex_addr[slot_id].key=key;
				//vertex_addr[slot_id].val=local_val();
				//break the while loop since we successfully insert
				break;
			}
		}
	}

	pthread_spin_unlock(&fine_grain_locks[lock_id]);
	assert(vertex_addr[slot_id].key==key);
	return slot_id;
}

/* see detail in header file
uint64_t graph_storage::atomic_alloc_edges(uint64_t num_edge){
	uint64_t curr_edge_ptr;
	pthread_spin_lock(&edge_lock);
	curr_edge_ptr=new_edge_ptr;
	new_edge_ptr+=num_edge;
	pthread_spin_unlock(&edge_lock);
	if(new_edge_ptr>=max_edge_ptr){
		cout<<"atomic_alloc_edges out of memory !!!! "<<endl;
        exit(-1);
	}
	return curr_edge_ptr;
}
*/

bool graph_storage::init_mem_for_key(uint64_t vertex_ptr, uint64_t size){
    bool ret = true;
    vertex *v = &vertex_addr[vertex_ptr];
    pthread_spin_lock(&v->realloc_lock);

    // value area has already existed!
    if (!v->val.is_null()){
        ret = false;
        pthread_spin_unlock(&v->realloc_lock);
        return ret;
    }

    // @debug
    slot_used += 1;
    malloc_mem_used += size;

    uint64_t curr_edge_ptr = edge_manager->alloc(size);
    vertex_addr[vertex_ptr].val = local_val(size, curr_edge_ptr);
  
    pthread_spin_unlock(&v->realloc_lock);
    return ret;
}



bool graph_storage::insertKeyValue(local_key key, uint64_t value, stream_timestamp timestamp){
    bool exist;
    uint64_t vertex_ptr = insertKey(key, false), curr_edge_ptr;
    
    if (init_mem_for_key(vertex_ptr, 1)){
      // new key, allocate edge memory
        exist = true;
        curr_edge_ptr = vertex_addr[vertex_ptr].val.get_writearea().start_ptr;
        edge_addr[curr_edge_ptr].val = value;
        // the size pointer has setted in init_mem_for_key

        // maintain metadata for stream queries
        vertex *v = &vertex_addr[vertex_ptr];
        pthread_spin_lock(&v->realloc_lock);
        metadata_manager->update_metadata(key, v->val.get_writearea(), timestamp);
        pthread_spin_unlock(&v->realloc_lock);
    } else{
      // exist key, expand edge memory
        exist = false;
        vertex *v = &vertex_addr[vertex_ptr];
        pthread_spin_lock(&v->realloc_lock);
        edge_memarea area = v->val.get_writearea();

        if(global_remove_dup){
            for(int i=0; i<area.offset; i++){
                if (value == edge_addr[i + area.start_ptr].val){
                    pthread_spin_unlock(&v->realloc_lock);
                    return exist;
                }
            }
        }


        uint32_t alloc_size = edge_manager->alloc_size(area.start_ptr);
        uint32_t need_size = area.offset + 1;
        uint64_t edge_ptr = area.start_ptr;
        if (need_size > alloc_size){
	    // alloc size not enough
        // alloc new area and copy data
            edge_ptr = edge_manager->alloc(need_size);
            edge_manager->copy(edge_ptr, area.start_ptr, area.offset);
	    }
        v->val.set_writearea(need_size, edge_ptr);
        curr_edge_ptr=v->val.get_writearea().start_ptr + v->val.get_writearea().offset - 1;
        edge_addr[curr_edge_ptr].val = value;

        // maintain metadata for stream queries
        metadata_manager->update_metadata(key, v->val.get_writearea(), timestamp);
        pthread_spin_unlock(&v->realloc_lock);
    }

    return exist;
}

void graph_storage::stream_insert_spo(const edge_triple &spo, stream_timestamp timestamp){
    // (s, ->, p) ---> normal vertex(o)
      // (s, ->, 0) ---> predicts
      // (p, <-, 0) ---> normal vertex(s)
    local_key key = local_key(spo.s, direction_out, spo.p);
    if (insertKeyValue(key, spo.o, timestamp)){
      // key doesn't exist before
        key = local_key(spo.s, direction_out, 0);
        insertKeyValue(key, spo.p, timestamp);
        key = local_key(spo.p, direction_in, 0);
        insertKeyValue(key, spo.s, timestamp);
    }
}

void graph_storage::stream_insert_ops(const edge_triple &ops, stream_timestamp timestamp){
    // (o, <-, p) ---> normal vertex(s)
      // (o, <-, 0) ---> predicts
      // (p, ->, 0) ---> normal vertex(o)
    local_key key = local_key(ops.o, direction_in, ops.p);
    if (insertKeyValue(key, ops.s, timestamp)){
      // key doesn't exist before
        key = local_key(ops.o, direction_in, 0);
        insertKeyValue(key, ops.p, timestamp);
        key = local_key(ops.p, direction_out, 0);
        insertKeyValue(key, ops.o, timestamp);
    }
}

void graph_storage::atomic_batch_insert(vector<edge_triple>& vec_spo,vector<edge_triple>& vec_ops){
    uint64_t accum_predict=0;
    uint64_t nedges_to_skip=0;
    while(nedges_to_skip<vec_ops.size()){
        if(is_index_vertex(vec_ops[nedges_to_skip].o)){
            nedges_to_skip++;
        } else {
            break;
        }
    }
    uint64_t curr_edge_ptr;
    uint64_t start = 0;

    // (s, ->, p) ---> normal vertex(o)
	while(start<vec_spo.size()){
		uint64_t end=start+1;
        // [st, ed) of vec_spo have the same s->p
		while(end<vec_spo.size()
				&& vec_spo[start].s==vec_spo[end].s
				&& vec_spo[start].p==vec_spo[end].p){
			end++;
		}

        accum_predict++;
        /*
        if (vec_spo[start].p == global_rdftype_id){
            start=end;
            continue;
        }
        */

        // insert local_key+local_val into hash table
		local_key key= local_key(vec_spo[start].s,direction_out,vec_spo[start].p);
		uint64_t vertex_ptr=insertKey(key);
		init_mem_for_key(vertex_ptr, end - start);
	        curr_edge_ptr = vertex_addr[vertex_ptr].val.get_writearea().start_ptr;

        // insert edges into edge memory
		for(uint64_t i=start;i<end;i++){
			edge_addr[curr_edge_ptr].val=vec_spo[i].o;
			curr_edge_ptr++;
		}
		start=end;
	}

    // (o, <-, p) ---> normal vertex(s)
    // skip triples whose object is index
	start=nedges_to_skip;
	while(start<vec_ops.size()){
		uint64_t end=start+1;
        // [st, ed) of vec_ops have the same o<-p
		while(end<vec_ops.size()
				&& vec_ops[start].o==vec_ops[end].o
				&& vec_ops[start].p==vec_ops[end].p){
			end++;
		}

        /*
        if (vec_ops[start].p == global_rdftype_id){
            start=end;
            continue;
        } 
        */     

        accum_predict++;
        // insert local_key+local_val into hash table
		local_key key= local_key(vec_ops[start].o,direction_in,vec_ops[start].p);
		uint64_t vertex_ptr=insertKey(key);
		init_mem_for_key(vertex_ptr, end - start);
        curr_edge_ptr = vertex_addr[vertex_ptr].val.get_writearea().start_ptr;
		//curr_edge_ptr = edge_manager->alloc(end - start);
		//local_val val= local_val(end-start,curr_edge_ptr);
		//vertex_addr[vertex_ptr].val=val;

        // insert edges into edge memory
		for(uint64_t i=start;i<end;i++){
			edge_addr[curr_edge_ptr].val=vec_ops[i].s;
			curr_edge_ptr++;
		}
		start=end;
	}


    // *accum_predict is obsolete*
    //curr_edge_ptr=atomic_alloc_edges(accum_predict);

    // (s, ->, 0) ---> predicts
    // accum_predict is calculated at previous phase
    // curr_edge_ptr=atomic_alloc_edges(accum_predict);
    start=0;
	while(start<vec_spo.size()){
        uint64_t end = start, npredict = 0;
        // count how many pairs with same subject
        while(end<vec_spo.size() && vec_spo[start].s==vec_spo[end].s){
            if(end==start || vec_spo[end].p!=vec_spo[end-1].p){
                npredict++;
            }
            end++;
        }

        if (vec_spo[start].s < (1 << nbit_predict))
            assert(false);

        // insert local_key+local_val into hash table
        local_key key= local_key(vec_spo[start].s,direction_out,0);
        uint64_t vertex_ptr=insertKey(key);
        init_mem_for_key(vertex_ptr, npredict);
        curr_edge_ptr = vertex_addr[vertex_ptr].val.get_writearea().start_ptr;
        //curr_edge_ptr = edge_manager->alloc(npredict);
        //local_val val= local_val(npredict, curr_edge_ptr);
        //vertex_addr[vertex_ptr].val=val;
        
        // insert edges into edge memory
        for(int idx = start; idx < end; idx++){
            if(idx == start || vec_spo[idx].p!=vec_spo[idx-1].p){
                edge_addr[curr_edge_ptr].val = vec_spo[idx].p;
    			curr_edge_ptr++;
            }
		}
    	start=end;
	}

}
void graph_storage::print_memory_usage(){

    cout << "slot used: " << slot_used << endl;
    cout << "slot size: " << sizeof(vertex) << endl;
    cout << "edge memory used: " << malloc_mem_used << endl;

}

vertex graph_storage::get_vertex_local(local_key key){
	uint64_t bucket_id=key.hash()%header_num;
	while(true){
		for(uint64_t i=0;i<cluster_size;i++){
			uint64_t slot_id=bucket_id*cluster_size+i;
			if(i<cluster_size-1){
				//data part
				if(vertex_addr[slot_id].key==key){
					//we found it
					return vertex_addr[slot_id];
				}
			} else {
				if(vertex_addr[slot_id].key!=local_key()){
					//next pointer
					bucket_id=vertex_addr[slot_id].key.id;
					//break from for loop, will go to next bucket
					break;
				} else {
					return vertex();
				}
			}
		}
	}
}

vertex graph_storage::get_vertex_remote(int tid,local_key key){
	char *local_buffer = rdma->GetMsgAddr(tid);
	uint64_t bucket_id=key.hash()%header_num;
    vertex ret;
    if(rdmacache.lookup(key,ret)){
        return ret;
    }
	while(true){
		uint64_t start_addr=sizeof(vertex) * bucket_id *cluster_size;
		uint64_t read_length=sizeof(vertex) * cluster_size;
		rdma->RdmaRead(tid,mymath::hash_mod(key.id,m_num),(char *)local_buffer,read_length,start_addr);
		vertex* ptr=(vertex*)local_buffer;
		for(uint64_t i=0;i<cluster_size;i++){
			if(i<cluster_size-1){
				if(ptr[i].key==key){
					//we found it
                    rdmacache.insert(ptr[i]);
					return ptr[i];
				}
			} else {
				if(ptr[i].key!=local_key()){
					//next pointer
					bucket_id=ptr[i].key.id;
					//break from for loop, will go to next bucket
					break;
				} else {
					return vertex();
				}
			}
		}
	}
}

edge* graph_storage::get_edges_global(int tid,uint64_t id,int direction,int predict,int* size){
    if( mymath::hash_mod(id,m_num) ==m_id){
        return get_edges_local(tid,id,direction,predict,size);
    }

    local_key key=local_key(id,direction,predict);
    vertex v=get_vertex_remote(tid,key);
    if(v.key==local_key()){
        *size=0;
        return NULL;
    }
    char *local_buffer = rdma->GetMsgAddr(tid);

    // use new interface of ptr+size
    edge_memarea area = v.val.get_readarea();
    uint64_t start_addr  = sizeof(vertex)*slot_num + sizeof(edge)*(area.start_ptr);
    uint64_t read_length = sizeof(edge)*area.offset;
    rdma->RdmaRead(tid,mymath::hash_mod(id,m_num),(char *)local_buffer,read_length,start_addr);
    edge* result_ptr=(edge*)local_buffer;
    *size=area.offset;
    return result_ptr;
}

edge* graph_storage::get_edges_local(int tid,uint64_t id,int direction,int predict,int* size){
    assert(mymath::hash_mod(id,m_num) ==m_id ||  is_index_vertex(id));
    local_key key=local_key(id,direction,predict);
    vertex v=get_vertex_local(key);
    if(v.key==local_key()){
        *size=0;
        return NULL;
    }

    // use new interface of ptr+size
    edge_memarea area = v.val.get_readarea();
    *size=area.offset;
    uint64_t ptr=area.start_ptr;
    return &(edge_addr[ptr]);
}

void graph_storage::insert_vector(tbb_vector_table& table,uint64_t index_id,uint64_t value_id){
	tbb_vector_table::accessor a;
	table.insert(a,index_id);
	a->second.push_back(value_id);
}

void graph_storage::init_index_table(){
    uint64_t t1=timer::get_usec();

	#pragma omp parallel for num_threads(8)
	for(int x=0;x<header_num+indirect_num;x++){
		for(int y=0;y<cluster_size-1;y++){
			uint64_t i=x*cluster_size+y;
			if(vertex_addr[i].key==local_key()){
				//empty slot, skip it
				continue;
			}
			uint64_t vid=vertex_addr[i].key.id;
			uint64_t p=vertex_addr[i].key.predict;
			if(vertex_addr[i].key.dir==direction_in){
				if(p==global_rdftype_id){
					//it means vid is a type vertex
					//we just skip it

                    // temporarily allow
                    //cout<<"[error] type vertices are not skipped"<<endl;
                    //assert(false);
					continue;
				} else {
					//this edge is in-direction, so vid is the dst of predict
					insert_vector(dst_predict_table,p,vid);
				}
			} else {
				if(p==global_rdftype_id){
				  edge_memarea area = vertex_addr[i].val.get_writearea();
					uint64_t degree=area.offset;
					uint64_t edge_ptr=area.start_ptr;
					for(uint64_t j=0;j<degree;j++){
						//src may belongs to multiple types
						insert_vector(type_table,edge_addr[edge_ptr+j].val,vid);
					}
				} else {
					insert_vector(src_predict_table,p,vid);
				}
			}
		}
	}


    uint64_t t2=timer::get_usec();


    // (p, <-, 0) ---> normal vertex
    // use src_predict_table
    for( tbb_vector_table::iterator i=src_predict_table.begin(); i!=src_predict_table.end(); ++i ) {
        // insert local_key+local_val
        local_key key= local_key(i->first,direction_in,0);
        uint64_t vertex_ptr=insertKey(key);
        init_mem_for_key(vertex_ptr, i->second.size());
        uint64_t curr_edge_ptr = vertex_addr[vertex_ptr].val.get_writearea().start_ptr;
        //uint64_t curr_edge_ptr = edge_manager->alloc(i->second.size());
        //local_val val= local_val(i->second.size(),curr_edge_ptr);
        //vertex_addr[vertex_ptr].val=val;
		
        // insert edges into edge memory
        for(uint64_t k=0;k<i->second.size();k++){
			edge_addr[curr_edge_ptr].val=i->second[k];
			curr_edge_ptr++;
            predict_index_edge_num++;
		}
    }

    // (p, ->, 0) ---> normal vertex
    // use dst_predict_table
    for( tbb_vector_table::iterator i=dst_predict_table.begin(); i!=dst_predict_table.end(); ++i ) {
        // insert local_key+local_val
        local_key key= local_key(i->first,direction_out,0);
        uint64_t vertex_ptr=insertKey(key);
        init_mem_for_key(vertex_ptr, i->second.size());
        uint64_t curr_edge_ptr = vertex_addr[vertex_ptr].val.get_writearea().start_ptr;
        //uint64_t curr_edge_ptr = edge_manager->alloc(i->second.size());
        //local_val val= local_val(i->second.size(),curr_edge_ptr);
        //vertex_addr[vertex_ptr].val=val;

        // insert edges into edge memory
		for(uint64_t k=0;k<i->second.size();k++){
			edge_addr[curr_edge_ptr].val=i->second[k];
			curr_edge_ptr++;
            predict_index_edge_num++;
		}
    }
    tbb_vector_table().swap(src_predict_table);
    tbb_vector_table().swap(dst_predict_table);
    uint64_t t3=timer::get_usec();
    cout<<(t2-t1)/1000<<" ms for parallel generate tbb_table "<<endl;
    cout<<(t3-t2)/1000<<" ms for sequence insert tbb_table to graph_storage"<<endl;

}

edge* graph_storage::get_index_edges_local(int tid,uint64_t index_id,int direction,int* size){
    //predict is not important , so we set it 0
    return get_edges_local(tid,index_id,direction,0,size);
};
