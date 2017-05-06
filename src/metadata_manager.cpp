/**
 * @file metadata_manager.cpp
 * @brief Methods in metadata_manager class
 */

#include "metadata_manager.h"

metadataManager::metadataManager(char* rdma_ptr, RdmaResource *_rdma, boost::mpi::communicator& _world)
    :rdma(_rdma), world(_world){
    
    int nstream = global_stream_source_num;
    // reserve several times memory more than usually needed for delay when busy
    batch_num_reserved = csparql_manager->nbatch_max_window() * 3;
    // maximum number of k/v pair send to one machine in each batch
    // one triple may cause 6 kv pairs
    batch_size_reserved = global_adaptor_buffer_ntriple * 6 / world.size() + 1;
    // lookup table for current (maybe unfinished) batch

    cout << "batch_num_reserved: " << batch_num_reserved << endl;
    cout << "batch_size_reserved: " << batch_size_reserved << endl;

    // initialize stream lock
    stream_lock = new pthread_spinlock_t[nstream];
    for(int i = 0; i < nstream; i++)
        pthread_spin_init(&stream_lock[i], 0);

    // initialize metadata data structures in heap(no RDMA area)
    stream_metadata = new vector<metadata_batch>[nstream];
    for(int i = 0; i < nstream; i++){
        for(int j = 0; j < batch_num_reserved; j++){
            stream_metadata[i].push_back(
                metadata_batch()
            );

            stream_metadata[i][j].batch_id = 0;
            stream_metadata[i][j].records.reserve(batch_size_reserved);
        }
    }

    enable_metadata = vector<bool>(nstream, false);
    csparql_manager->enable_metadata(enable_metadata);

    usage = new tbb::concurrent_vector<memusage>[global_stream_source_num];

    current_batch = new unordered_map<uint64_t, memarea_slot>();
    current_batch->reserve(batch_size_reserved);

    metadata_hashmap_init(rdma_ptr);
}

void metadataManager::metadata_hashmap_init(char* rdma_ptr){
    // default settings of metadata hashmap
    bucket_size = 4;
    resource_redundant_factor = 3;
    bucket_num = batch_size_reserved / bucket_size * resource_redundant_factor;
    slot_num = bucket_size * bucket_num;

    // memory usage checking
    uint64_t total_size = (uint64_t)global_metadata_size_mb * 1024 * 1024;
    uint64_t hashmap_size = slot_num * sizeof(memarea_slot);
    uint64_t direct_size;
    if (global_metadata_location_cache){
        // hashmap for all machines
        direct_size = world.size() * hashmap_size * global_stream_source_num * batch_num_reserved;
    } else{
        // hashmap for one machine
        direct_size = hashmap_size * global_stream_source_num * batch_num_reserved;
    }

    // feedback
    cout << "## Metadata Hashmap parameters:" << endl;
    cout << "   total size: " << total_size << endl;
    cout << "   direct size: " << direct_size << endl;

    // indirect pointers
    assert(total_size > direct_size);
    metadata_indirect = (memarea_slot*)(rdma_ptr + direct_size);
    total_indirect = (total_size - direct_size) / sizeof(memarea_slot) / bucket_size;
    next_indirect = 0;    

    // direct pointers
    metadata_hashmap = new memarea_slot***[world.size()];
    if (global_metadata_location_cache){
        // for each machine
        for(int m = 0; m < world.size(); m++){
            metadata_hashmap[m] = new memarea_slot**[global_stream_source_num];
            uint64_t offset = m * hashmap_size * global_stream_source_num * batch_num_reserved;
            // for each stream
            for(int i = 0; i < global_stream_source_num; i++){
                metadata_hashmap[m][i] = new memarea_slot*[batch_num_reserved];
                // for each batch
                for(int j = 0; j < batch_num_reserved; j++){
                    metadata_hashmap[m][i][j] = (memarea_slot*)
                        (rdma_ptr + (offset + hashmap_size * (i * batch_num_reserved + j)));
                }
            }
        }
    } else{
        // for one machine
        int m = world.rank();
        metadata_hashmap[m] = new memarea_slot**[global_stream_source_num];
        // for each stream
        for(int i = 0; i < global_stream_source_num; i++){
            metadata_hashmap[m][i] = new memarea_slot*[batch_num_reserved];
            // for each batch
            for(int j = 0; j < batch_num_reserved; j++){
                metadata_hashmap[m][i][j] = (memarea_slot*)
                    (rdma_ptr + hashmap_size * (i * batch_num_reserved + j));
            }
        }
    }
}

void metadataManager::metadata_hashmap_insert(memarea_slot* current_hashmap, const memarea_slot& slot){
    uint64_t key = slot.key;
    uint64_t start_ptr = slot.area.start_ptr;
    uint64_t offset = slot.area.offset;

    // metadata hashmap insertion
    int bucket_id = local_key::unarchive(key).hash() % bucket_num;
    memarea_slot* current_bucket = &current_hashmap[bucket_id * bucket_size];
    while(true){
        bool success = false;
        for(int i = 0; i < bucket_size; i++){
            if (current_bucket[i].key == 0 &&
                current_bucket[i].area.start_ptr == 0){
                // this is an empty slot
                current_bucket[i].key = key;
                current_bucket[i].area.start_ptr = start_ptr;
                current_bucket[i].area.offset = offset;

                success = true;
                break;
            }
        }

        if (success) break;
        if (current_bucket[bucket_size - 1].area.offset != 0){
            // need allocate new indirect bucket
            int bucket_id = __sync_fetch_and_add( &next_indirect, 1 );
            // currently, indirect bucket should be rare
            assert(next_indirect < total_indirect);

            memarea_slot* new_bucket = &metadata_indirect[bucket_id * bucket_size];
            // normal slots
            new_bucket[0] = current_bucket[bucket_size - 1];
            new_bucket[1].key = key;
            new_bucket[1].area.start_ptr = start_ptr;
            new_bucket[1].area.offset = offset;
            // linked list slot
            current_bucket[bucket_size - 1].key = 0;
            current_bucket[bucket_size - 1].area.start_ptr = bucket_id;
            current_bucket[bucket_size - 1].area.offset = 0;
                
            break;
        } else{
            // search in next bucket
            current_bucket = &metadata_indirect[
                bucket_size * current_bucket[bucket_size - 1].area.start_ptr
            ];
        }
    }

}

memarea_slot metadataManager::metadata_hashmap_lookup(
                  int mid, int tid, local_key key, int target,
                  int stream_id, int batch_id){
    batch_id %= batch_num_reserved;
    memarea_slot *current_hashmap = metadata_hashmap[mid][stream_id][batch_id], *current_bucket;
    int bucket_id = key.hash() % bucket_num;
    char* rdma_start = rdma->get_buffer();
    uint64_t read_length = bucket_size_byte();
    // initial bucket
    if (mid == target || global_metadata_location_cache){
        // local
        current_hashmap = metadata_hashmap[target][stream_id][batch_id];
        current_bucket = &current_hashmap[bucket_id * bucket_size];
    } else{
        // remote
        current_bucket = (memarea_slot*)rdma->GetMsgAddr(tid);
        uint64_t start_addr = (char*)(&current_hashmap[bucket_id * bucket_size]) - rdma_start;
        rdma->RdmaRead(tid, target, (char*)current_bucket, read_length, start_addr);
    }

    uint64_t key_archive = key.archive();
	while(true){
        // look for key in current bucket
        for(int i = 0; i < bucket_size; i++){
            if (current_bucket[i].key == key_archive){
                return current_bucket[i];
            }
        }

        // try next bucket
        if (current_bucket[bucket_size - 1].key == 0 &&
            current_bucket[bucket_size - 1].area.start_ptr != 0 &&
            current_bucket[bucket_size - 1].area.offset == 0){
            // go to next bucket
            int indirect_id = current_bucket[bucket_size - 1].area.start_ptr;
            if (mid == target || global_metadata_location_cache){
                // local
                current_bucket = &metadata_indirect[indirect_id * bucket_size];
            } else{
                // remote
                uint64_t start_addr = (char*)(&metadata_indirect[indirect_id * bucket_size]) - rdma_start;
                rdma->RdmaRead(tid, target, (char*)current_bucket, read_length, start_addr);
            }
        } else{
            // not found
            return memarea_slot();
        }
	}
}


void metadataManager::start_batch(const stream_timestamp &t){
    // indicate the next batch to be inserted
    // wish clear method does not change the capacity
    // or reserve is useless
    int bid_mod = t.batch_id % batch_num_reserved;
    stream_metadata[t.stream_id][bid_mod].records.clear();
    stream_metadata[t.stream_id][bid_mod].batch_id = t.batch_id;

    current_batch_time = t;
    current_batch->clear();
}

void metadataManager::finish_batch(){
    int stream_id = current_batch_time.stream_id;
    int batch_id = current_batch_time.batch_id % batch_num_reserved;
    int sz = stream_metadata[stream_id][batch_id].records.size();

    // flatten metadata entry records
    for(int i = 0; i < sz; i++){
        uint64_t key = stream_metadata[stream_id][batch_id].records[i].key;
        stream_metadata[stream_id][batch_id].records[i] = (*current_batch)[key];
    }

    prepare_rdma_metadata();
}

void metadataManager::prepare_rdma_metadata(){
    int stream_id = current_batch_time.stream_id;
    int batch_id = current_batch_time.batch_id % batch_num_reserved;
    
    // clear hashmap memory in RDMA area
    memarea_slot* current_hashmap = metadata_hashmap[world.rank()][stream_id][batch_id];
    memset((char*)current_hashmap, 0, sizeof(memarea_slot) * bucket_num * bucket_size);

    // insert records into RDMA metadata hashmap
    int sz = stream_metadata[stream_id][batch_id].records.size();
    //cout << "rdma finish, stream" << stream_id << " batch" << batch_id << endl;
    for(int i = 0; i < sz; i++){
        metadata_hashmap_insert(current_hashmap, stream_metadata[stream_id][batch_id].records[i]);
    }
}

void metadataManager::prepare_cache_metadata(int target, const metadata_cache_packet &packet,
                                             stream_vector_clock* local_steady_clocks){
    // key == 0 means that area is a timestamp
    assert(packet.archive[0].key == 0);

    int sz = packet.archive.size(), stream_id, batch_id;
    memarea_slot* current_hashmap;

    vector<stream_timestamp> metadata_finish_VTS;
    for(int j = 0; j < sz; j++){
        if (packet.archive[j].key == 0){
            stream_id = packet.archive[j].stream_id();
            batch_id = packet.archive[j].batch_id();

            metadata_finish_VTS.push_back(stream_timestamp(stream_id, batch_id));

            current_hashmap = metadata_hashmap[target][stream_id][batch_id % batch_num_reserved];
            memset((char*)current_hashmap, 0, sizeof(memarea_slot) * bucket_num * bucket_size);
        } else{
            // update metadata
            // metadata_local_cache[machine_id][stream_id][batch_id][key] = value
            uint64_t key = packet.archive[j].key;
            memarea_slot slot;
            slot.key = key;
            slot.area.start_ptr = packet.archive[j].start_ptr();
            slot.area.offset = packet.archive[j].offset();
            // metadata_local_cache[i][stream_id][batch_id % batch_num_reserved][key] = slot;
            metadata_hashmap_insert(current_hashmap, slot);
        }
    }

    for(auto t : metadata_finish_VTS){
        stream_id = t.stream_id;
        batch_id = t.batch_id;
        
        // update local_steady_clocks
        local_steady_clocks[target].clock[stream_id].batch_id = batch_id;
    }

}


void metadataManager::update_metadata(
    const local_key& raw_key, const edge_memarea& area, 
    const stream_timestamp &timestamp){

    int stream_id = timestamp.stream_id;
    int batch_id = timestamp.batch_id;
    if (!enable_metadata[stream_id]){
        // no query window on this stream
        return;
    }

    // batch timestamp checking
    if (current_batch_time.stream_id != stream_id ||
        current_batch_time.batch_id != batch_id){
        cout << "Updating wrong stream batch in metadata manager:" << stream_id << endl;
        assert(false);
    }

    uint64_t key = raw_key.archive();
    bool found = current_batch->count(key);
    if (found){
        // key modified before in this batch
        memarea_slot slot = (*current_batch)[key];
        uint64_t new_offset = slot.area.offset + 1;
        uint64_t new_start_ptr = area.start_ptr + area.offset - new_offset;
        slot.area.offset = new_offset;
        slot.area.start_ptr = new_start_ptr;
        (*current_batch)[key] = slot;
    } else{
        // new key modified in this batch
        memarea_slot slot;
        slot.key = key;
        slot.area.offset = 1;
        slot.area.start_ptr = area.start_ptr + area.offset - 1;
        (*current_batch)[key] = slot;
        // flattened records
        stream_metadata[stream_id][batch_id % batch_num_reserved].records.push_back(slot);
    }
}

// query and garbage collection
// void metadataManager::prepare_metadata_cache(
//     const request_or_reply& r,
//     metadata_cache_packet* cache_packet){
    
//     int nwindows = r.stream_info.window_info.size();
//     // prepare metadata for each window (on different stream)
//     for(int i = 0; i < nwindows; i++){
//         int start_batch_id = r.stream_info.window_info[i].start_batch_id;
//         int end_batch_id = r.stream_info.window_info[i].end_batch_id;
//         int stream_id = r.stream_info.window_info[i].stream_id;

//         for(int bid = start_batch_id; bid <= end_batch_id; bid++){
//             // ring buffer
//             int bid_mod = bid % batch_num_reserved;

//             // TODO: query filter
//             cache_packet->append(stream_metadata[stream_id][bid_mod].records);
//         }
//     }
// }


void metadataManager::print_memory_usage(){
    assert(false);
    
//     int nstream = global_stream_source_num;
//     for(int i = 0; i < nstream; i++){
//         pthread_spin_lock(&stream_lock[i]);
//         cout << "#######Stream" << i << endl;

//         int total_interval = 0, total_data = 0;
//         int nrecord = usage[i].size();
//         for(auto iter : usage[i]){
//             total_interval += iter.ninterval;
//             total_data += iter.ndata;
// //            if (iter.ninterval != 0)
// //            cout << iter.ninterval << "\t" << iter.ndata << endl;
//         }
//         cout << "metadata: " << total_interval / nrecord;
//         cout << "data: " << total_data / nrecord;
//         cout << endl;
//         pthread_spin_unlock(&stream_lock[i]);
//     }
}
