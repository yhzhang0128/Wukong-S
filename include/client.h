/**
 * @file client.h
 * @brief Routines for client communicating with server
 */

#pragma once

#include "query_basic_types.h"
#include "global_cfg.h"
#include "thread_cfg.h"

#include "sparql_parser.h"
#include "string_server.h"

#include <boost/unordered_set.hpp>
#include <boost/unordered_map.hpp>


/**
 * @brief Client functions
 */
class client{
public:
	thread_cfg* cfg;
	string_server* str_server;
	sparql_parser parser;
    client(thread_cfg* _cfg,string_server* str_server);
    

    /**
     * @brief Send request to server
     */
    void Send(request_or_reply& req);
    /**
     * @brief Receive reply from server
     */
	request_or_reply Recv();

    /* accessory functions */
	void GetId(request_or_reply& req);
	void print_result(request_or_reply& reply,int row_to_print);
};
