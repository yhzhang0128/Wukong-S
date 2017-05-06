/**
 * @file message_wrap.cpp
 * @brief Wrapped send/recv functions(through thread_id)
 */


#include "message_wrap.h"

// Send dispatch_packet
void SendT(thread_cfg* cfg, int r_mid, int r_tid, 
           stream_dispatch_packet &packet){
    std::stringstream ss;
    boost::archive::binary_oarchive oa(ss);
    oa << packet;
    //cout << "Send " << packet.buffer.size() << endl;
    if (global_use_rbf){
        cfg->rdma->rbfSend(cfg->t_id, r_mid, r_tid, ss.str().c_str(), ss.str().size());
    } else{
        cfg->node->Send(cfg->m_id, r_mid, r_tid, ss.str());
    }
}
// Recv dispatch_packet
bool TryRecvT(thread_cfg* cfg, int r_mid, 
              stream_dispatch_packet &packet){
    std::string str;

    if(global_use_rbf){
        bool success = cfg->rdma->rbfTargetTryRecv(cfg->t_id, r_mid, str);
        if (!success)
            return false;
    } else {
        str = cfg->node->tryRecv(r_mid);
        if(str == "")
            return false;
    }

    std::stringstream s;
    s << str;
    boost::archive::binary_iarchive ia(s);
    ia >> packet;

    //cout << "Recv " << packet.buffer.size() << endl;
    return true;
}

void SendC(thread_cfg* cfg, int r_mid, int r_tid, 
           stream_vector_clock& clock){
    std::stringstream ss;
    boost::archive::binary_oarchive oa(ss);
    oa << clock;
    
    if (global_use_rbf){
        cfg->rdma->rbfSend(cfg->t_id, r_mid, r_tid, ss.str().c_str(), ss.str().size());
    } else{
        cfg->node->Send(cfg->m_id, r_mid, r_tid, ss.str());
    }
}
bool TryRecvC(thread_cfg* cfg, int r_mid, 
              stream_vector_clock& clock){
    std::string str;

    if(global_use_rbf){
        bool success = cfg->rdma->rbfTargetTryRecv(cfg->t_id, r_mid, str);
        if (!success)
            return false;
    } else {
        str = cfg->node->tryRecv(r_mid);
        if(str == "")
            return false;
    }

    std::stringstream s;
    s << str;
    boost::archive::binary_iarchive ia(s);
    ia >> clock;

    return true;
}

// metadata cache packet
void SendM(thread_cfg* cfg, int r_mid, int r_tid, metadata_cache_packet &packet){
    std::stringstream ss;
    boost::archive::binary_oarchive oa(ss);
    oa << packet;

    if (global_use_rbf){
        cfg->rdma->rbfSend(cfg->t_id, r_mid, r_tid, ss.str().c_str(), ss.str().size());
    } else{
        cfg->node->Send(cfg->m_id, r_mid, r_tid, ss.str());
    }
}
bool TryRecvM(thread_cfg* cfg, int r_mid, metadata_cache_packet &packet){
    std::string str;
    if(global_use_rbf){
        bool success = cfg->rdma->rbfTargetTryRecv(cfg->t_id, r_mid, str);
        if (!success)
            return false;
    } else {
        str=cfg->node->Recv(r_mid);
        if (str == "")
            return false;
    }

    std::stringstream s;
    s << str;
    boost::archive::binary_iarchive ia(s);
    ia >> packet;

    return true;
}

// forward star metadata packet
// void SendMOpt(thread_cfg* cfg, int r_mid, int r_tid, forward_star_packet &packet){
//     uint64_t t1=timer::get_usec();    
//     std::stringstream ss;
//     boost::archive::binary_oarchive oa(ss);
//     oa << packet;
//     uint64_t t2=timer::get_usec();
//     //cout << "  Send archive latency" << t2 - t1 << endl;
//     //cout << "Send " << packet.indexes.size() << " "
//     //                << ss.str().length() << endl;

//     if (global_use_rbf){
//         cfg->rdma->rbfSend(cfg->t_id, r_mid, r_tid, ss.str().c_str(), ss.str().size());
//     } else{
//         cfg->node->Send(cfg->m_id, r_mid, r_tid, ss.str());
//     }
// }


// void RecvMOpt(thread_cfg* cfg, int r_mid, forward_star_packet &packet){
//     std::string str;
//     if(global_use_rbf){
//         str=cfg->rdma->rbfTargetRecv(cfg->t_id, r_mid);
//     } else {
//         str=cfg->node->Recv(r_mid);
//     }

//     uint64_t t1=timer::get_usec();
//     std::stringstream s;
//     s << str;
//     boost::archive::binary_iarchive ia(s);
//     ia >> packet;

//     uint64_t t2=timer::get_usec();
//     //cout << "  Recv archive latency" << t2 - t1 << endl;
// }

void SendR(thread_cfg* cfg,int r_mid,int r_tid,request_or_reply& r){
    std::stringstream ss;
    boost::archive::binary_oarchive oa(ss);    
    oa << r;
    if(global_use_rbf){
        cfg->rdma->rbfSend(cfg->t_id,r_mid, r_tid, ss.str().c_str(),ss.str().size());
    } else {
        cfg->node->Send(cfg->m_id, r_mid,r_tid,ss.str());
    }
}

request_or_reply RecvR(thread_cfg* cfg){
    std::string str;
    if(global_use_rbf){
        str=cfg->rdma->rbfRecv(cfg->t_id);
    } else {
        str=cfg->node->Recv();
    }
    std::stringstream s;
    s << str;
    boost::archive::binary_iarchive ia(s);
    request_or_reply r;
    ia >> r;
    return r;
}

bool TryRecvR(thread_cfg* cfg,request_or_reply& r){
    std::string str;
    if(global_use_rbf){
        bool ret=cfg->rdma->rbfTryRecv(cfg->t_id,str);
        if(!ret) {
            return false;
        }
    } else {
        str=cfg->node->tryRecv();
        if(str==""){
            return false;
        }
    }
    std::stringstream s;
    s << str;
    boost::archive::binary_iarchive ia(s);
    ia >> r;
    return true;
};

bool TryTargetRecvR(thread_cfg* cfg, int r_mid, 
                    request_or_reply& r){
    std::string str;
    if(global_use_rbf){
        bool success = cfg->rdma->rbfTargetTryRecv(cfg->t_id, r_mid, str);
        if(!success) {
            return false;
        }
    } else {
        str=cfg->node->tryRecv(r_mid);
        if(str==""){
            return false;
        }
    }
    std::stringstream s;
    s << str;
    boost::archive::binary_iarchive ia(s);
    ia >> r;
    return true;
};
