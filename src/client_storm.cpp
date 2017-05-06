#include "client_mode.h"
#include "message_wrap.h"

static void copy_raw_request(const request_or_reply& raw_request,
                        request_or_reply& request){
    request.first_target = raw_request.first_target;
    request.id = raw_request.id;
    request.parent_id = raw_request.parent_id;
    request.local_var = raw_request.local_var;
    request.cmd_chains = raw_request.cmd_chains; //n*(start,p,direction,end)
    request.result_table.clear();
    // do not be silent!
    request.silent=0;
}

static void parallize_request(const request_or_reply& request,
                              vector<request_or_reply>& requests,
                              int step, int request_lines, int col_num, int m_num){
    int lines_per_machine = request_lines / m_num;

    for(int i = 0; i < m_num; i++){
        request_or_reply req_single_machine;
        copy_raw_request(request, req_single_machine);
        req_single_machine.step = step;
        req_single_machine.col_num = col_num;

        int base = lines_per_machine * i;
        int end = (i == m_num - 1)? request_lines : base + lines_per_machine;
        for(int j = base; j < end; j++){
            // single line
            for(int k = 0; k < col_num; k++)
                req_single_machine.result_table.push_back(request.result_table[col_num * j + k]);
        }
        requests.push_back(req_single_machine);
    }
}

static string storm_serialize(const request_or_reply& reply){
    assert(reply.result_table.size() == reply.silent_row_num * reply.col_num);
    stringstream ss;

    int idx = 0;
    for(int i = 0; i < reply.silent_row_num; i++){
        for(int j = 0; j < reply.col_num; j++){
            ss << (reply.result_table[idx++]) << "\t";
        }
        ss << "\n";
    }
    
    return ss.str();
}

void storm_send(client* clnt, vector<request_or_reply>& reqs){
    // normal SPARQL
    int tid_base = clnt->cfg->client_num + 1;

    for(int i=0; i<clnt->cfg->m_num; i++){
        if(reqs[i].parent_id==-1){
            clnt->GetId(reqs[i]);
        }

        // C-SPARQL should be handled in stream_query_client
        assert(!reqs[i].stream_info.is_stream_query);

        SendR(clnt->cfg, i, tid_base, reqs[i]);
    }
}

request_or_reply storm_recv(client* clnt){
    request_or_reply r = RecvR(clnt->cfg);
    assert(!r.stream_info.is_stream_query);

    for(int count=0; count<clnt->cfg->m_num-1 ;count++){
        request_or_reply r2=RecvR(clnt->cfg);

        r.silent_row_num +=r2.silent_row_num;
        int new_size=r.result_table.size()+r2.result_table.size();
        r.result_table.reserve(new_size);
        r.result_table.insert( r.result_table.end(), r2.result_table.begin(), r2.result_table.end());
    }
    return r;
}

void storm_execute_sample_query_good(client* clnt){
    string filename = global_csparql_folder + "good_static";

    zmq::context_t context(1);
    zmq::socket_t* receiver;

    receiver = new zmq::socket_t(context, ZMQ_REP);
    receiver->bind("tcp://*:10090");

    cout << "Start!" << endl;
    request_or_reply raw_request;
    bool success = clnt->parser.parse(filename,raw_request);
    if(!success){
        cout<<"sparql parse error"<<endl;
        assert(false);
    }

    while(true){
        zmq::message_t storm_request;

        receiver->recv(&storm_request);
        //cout << "Recv: " << (char*)request.data() << endl;

        uint64_t t0=timer::get_usec();

        string title[2];
        stringstream ss((char*)storm_request.data());
        ss >> title[0] >> title[1];

        // execute query
        request_or_reply request;
        copy_raw_request(raw_request, request);

        // put table from storm tsv style serialization
        request.step = 2;
        request.col_num = 2;
        uint64_t col[2];
        int request_lines = 0;
        while(ss >> col[0] >> col[1]){
            request_lines++;
            for(int i = 0; i < 2; i++)
                if (title[i] == "post")
                    request.result_table.push_back(col[i]);
            for(int i = 0; i < 2; i++)
                if (title[i] == "user")
                    request.result_table.push_back(col[i]);
        }
        cout << "recieve: " << request.result_table.size() << endl;

        // parallize the query to each machine
        int m_num = clnt->cfg->m_num;
        // prepare request for each machine
        vector<request_or_reply> requests;
        parallize_request(request, requests, 2, request_lines, 2, m_num);

        request_or_reply reply;
        uint64_t t1=timer::get_usec();
        storm_send(clnt, requests);
        reply = storm_recv(clnt);//clnt->Recv();
        uint64_t t2=timer::get_usec();

        string reply_str = storm_serialize(reply);

        uint64_t t3=timer::get_usec();
        
        cout << "result lines: " << reply.silent_row_num << ", latency: " << t2 - t1 << "us"
             << ", overhead: " << (t1 - t0) + (t3 - t2) << "us" << endl;


        zmq::message_t reply_to_storm(reply_str.length());
        memcpy (reply_to_storm.data(), reply_str.c_str(), reply_str.length());
        receiver->send(reply_to_storm);
    }
}

void storm_execute_sample_query_bad(client* clnt){
    string filename = global_csparql_folder + "bad_static";

    zmq::context_t context(1);
    zmq::socket_t* receiver;

    receiver = new zmq::socket_t(context, ZMQ_REP);
    receiver->bind("tcp://*:10090");

    cout << "Start!" << endl;
    request_or_reply raw_request;
    bool success=clnt->parser.parse(filename,raw_request);
    if(!success){
        cout<<"sparql parse error"<<endl;
        assert(false);
    }

    while(true){
        zmq::message_t storm_request;

        receiver->recv(&storm_request);
        //cout << "Recv: " << (char*)request.data() << endl;

        uint64_t t0=timer::get_usec();

        string title[3];
        stringstream ss((char*)storm_request.data());
        ss >> title[0] >> title[1] >> title[2];

        // execute query
        request_or_reply request;
        copy_raw_request(raw_request, request);

        // put table from storm tsv style serialization
        request.step = 3;
        request.col_num = 3;
        uint64_t col[3];
        int request_lines = 0;
        while(ss >> col[0] >> col[1] >> col[2]){
            request_lines++;
            for(int i = 0; i < 3; i++)
                if (title[i] == "post")
                    request.result_table.push_back(col[i]);
            for(int i = 0; i < 3; i++)
                if (title[i] == "user")
                    request.result_table.push_back(col[i]);
            for(int i = 0; i < 3; i++)
                if (title[i] == "friend")
                    request.result_table.push_back(col[i]);
        }
        cout << "recieve: " << request.result_table.size() << endl;

        int m_num = clnt->cfg->m_num;
        // prepare request for each machine
        vector<request_or_reply> requests;
        parallize_request(request, requests, 3, request_lines, 3, m_num);

        request_or_reply reply;
        uint64_t t1=timer::get_usec();
        storm_send(clnt, requests);
        reply = storm_recv(clnt);//clnt->Recv();
        uint64_t t2=timer::get_usec();

        string reply_str = storm_serialize(reply);

        uint64_t t3=timer::get_usec();

        cout << "result lines: " << reply.silent_row_num << ", latency: " << t2 - t1 << "us"
             << ", overhead: " << (t1 - t0) + (t3 - t2) << "us" << endl;


        zmq::message_t reply_to_storm(reply_str.length());
        memcpy (reply_to_storm.data(), reply_str.c_str(), reply_str.length());
        receiver->send(reply_to_storm);
    }
}
