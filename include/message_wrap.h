/**
 * @file message_wrap.h
 * @brief Wrapped send/recv functions(through thread_id)
 */

#pragma once

#include "query_basic_types.h"
#include "network_node.h"
#include "rdma_resource.h"
#include "thread_cfg.h"
#include "global_cfg.h"

#include "stream_adaptor_interface.h"
#include "stream_query_client.h"
#include "stream_coordinator.h"
#include "metadata_manager.h"

// request or reply
void SendR(thread_cfg* cfg,int r_mid,int r_tid,request_or_reply& r);
request_or_reply RecvR(thread_cfg* cfg);
bool TryRecvR(thread_cfg* cfg,request_or_reply& r);
bool TryTargetRecvR(thread_cfg* cfg, int r_mid, request_or_reply& r);

// dispatch_packet
void SendT(thread_cfg* cfg, int r_mid, int r_tid, stream_dispatch_packet &packet);
//void RecvT(thread_cfg* cfg, int r_mid, stream_dispatch_packet &packet);
bool TryRecvT(thread_cfg* cfg, int r_mid, stream_dispatch_packet &packet);

// vector timestamp
void SendC(thread_cfg* cfg, int r_mid, int r_tid, stream_vector_clock& clock);
bool TryRecvC(thread_cfg* cfg, int r_mid, stream_vector_clock& clock);

// metadata cache packet
void SendM(thread_cfg* cfg, int r_mid, int r_tid, metadata_cache_packet &packet);
bool TryRecvM(thread_cfg* cfg, int r_mid, metadata_cache_packet &packet);

// optimized metadata packet
/* void SendMOpt(thread_cfg* cfg, int r_mid, int r_tid, forward_star_packet &packet); */
/* void RecvMOpt(thread_cfg* cfg, int r_mid, forward_star_packet &packet); */

template<typename T>
void SendObject(thread_cfg* cfg,int r_mid,int r_tid,T& r){
    std::stringstream ss;
    boost::archive::binary_oarchive oa(ss);
    oa << r;
    cfg->node->Send(cfg->m_id, r_mid,r_tid,ss.str());
}

template<typename T>
T RecvObject(thread_cfg* cfg){
    std::string str;
    str=cfg->node->Recv();
    std::stringstream s;
    s << str;
    boost::archive::binary_iarchive ia(s);
    T r;
    ia >> r;
    return r;
}
