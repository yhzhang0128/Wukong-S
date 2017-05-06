/**
 * @file client.h
 * @brief Routines for client iterative shell
 */

#include <iostream>
#include "utils.h"
#include "global_cfg.h"
#include "thread_cfg.h"
#include "client.h"
#include "batch_logger.h"

#include <boost/unordered_map.hpp>
#include <set>

using namespace std;

/**
 * Shell is currently running by one thread on master node.
 * Later will be moved to seperate client side.
 */
void iterative_shell(client* clnt);

void storm_execute_sample_query_bad(client* clnt);
void storm_execute_sample_query_good(client* clnt);

/**
 * Different execution modes used in iterative shell
 */
void single_execute(client* clnt,string filename,int execute_count);
void batch_execute(client* clnt,string mix_config,batch_logger& logger);
void nonblocking_execute(client* clnt,string mix_config,batch_logger& logger);


