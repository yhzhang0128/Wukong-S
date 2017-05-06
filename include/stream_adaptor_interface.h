/**
 * @file stream_adaptor_interface.h
 * @brief All stream adaptors should implement this interface.
 */

#pragma once

#include "global_cfg.h"
#include "stream_basic_types.h"


/**
 * @brief Adaptor Interface
 *
 * Several adaptors can be implemented from this interface
 * such as HDFS, Kafka, Twitter, etc.
 */
class stream_adaptor_interface{
 public:
    int stream_id;  /** unique stream ID  */
    string stream_name;  /** resource identifier, such as "hdfs://..." */
    int stream_rate;
    int buffer_size;  /** number of elements in buffer */
    vector<stream_batch_item> buffer;  /** buffer for time-labled triples */

    virtual void work() = 0;
    virtual void pause() = 0;
    virtual void resume() = 0;
    virtual void clear() = 0;
};
