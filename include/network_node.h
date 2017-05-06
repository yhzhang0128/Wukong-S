/**
 * @file network_node.h
 * @brief send/recv functions by zeromq
 */

#pragma once

#include <zmq.hpp>
#include <string>
#include <iostream>
#include <unistd.h>
#include <unordered_map>
#include <fstream>
#include <errno.h>

#include <sstream>
class Network_Node {

public:
  int pid;
  int nid;
  int m_num;
  int port_base;
  zmq::context_t context;
  zmq::socket_t** receiver;

  std::vector<std::string> net_def;
  std::unordered_map<int,zmq::socket_t*> socket_map;
  inline int hash(int _srcWorld, int _dstWorld, int _dstThread){
    return _srcWorld * 600 + _dstWorld * 30 + _dstThread;
  }

 Network_Node(int mnum, int _pid,int _nid,std::string hostname):
  m_num(mnum),nid(_nid),pid(_pid),context(1){
    std::ifstream file(hostname);
    std::string ip;
    while(file>>ip){
        net_def.push_back(ip);
    }

    port_base = 12000;
    char address[30]="";
    receiver = new zmq::socket_t*[m_num];
    for(int i = 0; i < m_num; i++){
      receiver[i] = new zmq::socket_t(context, ZMQ_PULL);
      sprintf(address,"tcp://*:%d", port_base+hash(i, pid, nid));
      //fprintf(stdout,"tcp binding address %s\n",address);
      //cout <<5500+hash(i, pid, nid) << endl;
      try{
          receiver[i]->bind (address);
      } catch(...){
          std::cout << address << std::endl;
          assert(false);
      }
    }
  }

  ~Network_Node(){
    for(auto iter:socket_map){
        if(iter.second!=NULL){
                delete iter.second;
                iter.second=NULL;
        }
    }
    delete receiver;
  }

  void Send(int _sid, int _pid,int _nid,std::string msg){
    int id=hash(_sid, _pid,_nid);

    if(socket_map.find(id)== socket_map.end()){
      socket_map[id] = new zmq::socket_t(context, ZMQ_PUSH);
      char address[30]="";

      snprintf(address,30,"tcp://%s:%d",net_def[_pid].c_str(),port_base + id);
      //fprintf(stdout,"mul estalabish %s\n",address);

      socket_map[id]->connect (address);
    }
    zmq::message_t request(msg.length());
    memcpy ((void *) request.data(), msg.c_str(), msg.length());
    socket_map[id]->send(request);
  }

  std::string Recv(){
    zmq::message_t reply;

    while(true){
      std::string ret;
      ret = tryRecv();
      if (ret != "")
        return ret;
    }
  }

  std::string Recv(int mid){
    zmq::message_t reply;

    if(receiver[mid]->recv(&reply) < 0) {
      fprintf(stderr,"recv with error %s\n",strerror(errno));
      exit(-1);
    }
    return std::string((char *)reply.data(),reply.size());
  }

  std::string tryRecv(){
    for(int i = 0; i < m_num; i++){
      std::string ret = tryRecv(i);
      if (ret != "")
        return ret;
    }
    return "";
  }

  std::string tryRecv(int mid){
    zmq::message_t reply;
    if (receiver[mid]->recv(&reply, ZMQ_NOBLOCK))
      return std::string((char *)reply.data(),reply.size());
    else
      return "";
  }
};
