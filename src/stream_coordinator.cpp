/**
 * @file stream_coordinator.cpp
 * @brief Methods in stream_coordinator class
 */

#include "stream_coordinator.h"
#include "message_wrap.h"

void stream_coordinator::send_local_steady_clock(){
    if (global_metadata_location_cache){
        // send clock and metadata
        stream_vector_clock tmp_clock = local_steady_clocks[world.rank()];
        metadata_cache_packet metadata_packet;

        // prepare metadata packet
        // TODO should move to metadata_manager
        for(int i = 0; i < global_stream_source_num; i++){
            for(int bid = last_send_clock.clock[i].batch_id + 1;
                    bid <= tmp_clock.clock[i].batch_id; bid++){
                stream_timestamp t;
                t.stream_id = i;
                t.batch_id = bid;
                metadata_packet.append(t);  // timestamp
                int ring_size = batch_num_reserved;
                assert(stream_metadata[i][bid % ring_size].batch_id == bid);
                metadata_packet.append(stream_metadata[i][bid % ring_size].records);  // metadata
            }

        }

        for(int i = 0; i < world.size(); i++)
            if (i != world.rank())
                SendM(cfg, i, global_coordinator_tid, metadata_packet);
        last_send_clock = tmp_clock;
    } else{
        // only send the clocks
        for(int i = 0; i < world.size(); i++)
            if (i != world.rank())
                SendC(cfg, i, global_coordinator_tid, local_steady_clocks[world.rank()]);
    }
}

void stream_coordinator::update_global_steady_clock(){
    stream_vector_clock tmp_clock;
    tmp_clock.clock.clear();

    // recv steady clock packets
    for(int i = 0; i < world.size(); i++){
        if (i == world.rank())
            continue;

        // synchronization
        while(true){
            tmp_clock.clock.clear();
            bool success = TryRecvC(cfg, i, tmp_clock);
            if (!success)
                break;
            local_steady_clocks[i].clock.swap(tmp_clock.clock);
        }

        // check size
        assert(local_steady_clocks[i].clock.size() == global_stream_source_num);
    }
}

void stream_coordinator::update_steady_clock_with_metadata(){
    // recv steady clock packets
    metadata_cache_packet packet;
    for(int i = 0; i < world.size(); i++){
        if (i == world.rank())
            continue;

        // synchronization
        packet.archive.clear();
        while(true){
            bool success = TryRecvM(cfg, i, packet);
            if (!success)
                break;
            //local_steady_clocks[i].clock.swap(tmp_clock.clock);
            if (packet.archive.size() == 0)
                break;

            metadata_manager->prepare_cache_metadata(i, packet, local_steady_clocks);
        }
    }

}

void stream_coordinator::recv_global_steady_clock(){
    if (global_metadata_location_cache){
        update_steady_clock_with_metadata();
    } else{
        update_global_steady_clock();
    }

    // maintain global steady clock
    for(int i = 0; i < global_stream_source_num; i++){
        int smallest_batch_id = 0x7fffffff;
        for(int j = 0; j < world.size(); j++)
            if (smallest_batch_id > local_steady_clocks[j].clock[i].batch_id)
                smallest_batch_id = local_steady_clocks[j].clock[i].batch_id;
        global_steady_clock.clock[i].batch_id = smallest_batch_id;
    }
}


void stream_coordinator::print_global_steady_clock(){
    for(int i = 0; i < global_stream_source_num; i++){
        cout << i << ":" << global_steady_clock.clock[i].batch_id << " | ";
    }
    cout << endl;
}

void stream_coordinator::print_local_steady_clock(){
    for(int j = 0; j < world.size(); j++){
        for(int i = 0; i < global_stream_source_num; i++){
            cout << i << ":" << local_steady_clocks[j].clock[i].batch_id << " | ";
        }
        cout << endl;
    }
}
