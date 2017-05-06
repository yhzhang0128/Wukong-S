/**
 * @file sparql_parser.cpp
 * @brief Methods in sparql_parser class
 * @bug see header files
 */

#include "sparql_parser.h"

inline static bool is_upper(string str1,string str2){
    return boost::to_upper_copy<std::string>(str1)==str2;
}

sparql_parser::sparql_parser(string_server* _str_server):str_server(_str_server){
    valid=true;
};


void sparql_parser::clear(){
    prefix_map.clear();
    variable_map.clear();
    req_template =  request_template();
    valid=true;
    join_step=-1;
    fork_step=-1;
};

vector<string> sparql_parser::get_token_vec(string filename){
    ifstream file(filename);
    vector<string> token_vec;
    if(!file){
        cout<<"[file not found] "<<filename<<endl;
        valid=false;
        return token_vec;
    }
    string cmd;
    while(file>>cmd){
        token_vec.push_back(cmd);
        if(cmd=="}"){
            break;
        }
	}
    file.close();
    return token_vec;
}

uint64_t sparql_parser::parse_time(string& time_spec){
    stringstream ss;
    uint64_t t;
    string unit;
    ss << time_spec;
    ss >> t >> unit;
    if (unit == "min")
        return t * 60 * 1000000;
    if (unit == "s")
        return t * 1000000;
    if (unit == "ms")
        return t * 1000;

    // invalid time_spec
    valid = false;
}

void sparql_parser::remove_header(vector<string>& token_vec){
    vector<string> new_vec;
    int iter=0;
    while(token_vec.size()>iter && token_vec[iter]=="PREFIX"){
        if(token_vec.size()>iter+2){
            prefix_map[token_vec[iter+1]]=token_vec[iter+2];
            iter+=3;
        } else {
            valid=false;
            return ;
        }
    }
    /// TODO More Checking!
    string prev1="", prev2="";
    while(token_vec[iter]!="{"){
        if (prev1 == "REGISTER" && prev2 == "QUERY"){
            stream_query->stream_info.query_name = token_vec[iter];
        }
        if (prev1 == "FROM"){
            if (stream_to_id->count(prev2) == 0){
                cout << prev2 << " not found when parsing csparql"
                     << stream_query->stream_info.query_name;
                assert(false);
            }
            // step and size will be filled in later
            stream_query->stream_info.window_info.push_back(
                windowinfo((*stream_to_id)[prev2], 0, 0)
            );
        }

        if (prev2 == "STEP"){
            stream_query->stream_info.window_info.back().window_size = parse_time(prev1);
            stream_query->stream_info.window_info.back().window_step = parse_time(token_vec[iter]);
            if (valid == false) return;
        }
        prev1 = prev2;
        prev2 = token_vec[iter];
        iter++;
    }
    iter++;

    while(token_vec[iter]!="}"){
        if(token_vec[iter]=="join"){
            join_step=new_vec.size()/4;
            iter++;
            continue;
        }
        if(token_vec[iter]=="fork"){
            fork_step=new_vec.size()/4;
            iter++;
            continue;
        }
        new_vec.push_back(token_vec[iter]);
        iter++;
    }
    token_vec.swap(new_vec);
}

void sparql_parser::replace_prefix(vector<string>& token_vec){
    for(int i=0;i<token_vec.size();i++){
        for(auto iter: prefix_map){
            if(token_vec[i].find(iter.first)==0){
                string new_str=iter.second;
                new_str.pop_back();
                new_str+=token_vec[i].substr(iter.first.size());
                new_str+=">";
                token_vec[i]=new_str;
            } else if(token_vec[i][0]=='%' && token_vec[i].find(iter.first)==1 ){
                string new_str="%";
                new_str+=iter.second;
                new_str.pop_back();
                new_str+=token_vec[i].substr(iter.first.size()+1);
                new_str+=">";
                token_vec[i]=new_str;
            }
        }
    }
}
int sparql_parser::str2id(string& str){
    if(str==""){
        cout<<"[empty string] "<<str<<endl;
        valid=false;
        return 0;
    }
    if(str[0]=='?'){
        if(variable_map.find(str)==variable_map.end()){
            int new_id=-1-variable_map.size();
            variable_map[str]=new_id;
        }
        return variable_map[str];
    } else if(str[0]=='%'){
        req_template.place_holder_str.push_back(str.substr(1));
        //valid=false;
        return place_holder;
    } else {
        if (str == "a"){
            // means type index
            return str_server->str2id[
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            ];
        }

        if(str_server->str2id.find(str) ==str_server->str2id.end()){
            cout<<"unknown str "<<str<<endl;
            valid=false;
            return 0;
        }
        return str_server->str2id[str];
    }
}
void sparql_parser::do_parse(vector<string>& token_vec){
    if(!valid) return ;

    remove_header(token_vec);
    if(!valid) return ;

    replace_prefix(token_vec);
    if(!valid) return ;

    if(token_vec.size()%4!=0){
        cout<<"[error token number] "<<endl;
        valid=false;
        return ;
    }

    int iter=0;
    while(iter<token_vec.size()){
        string strs[3]={token_vec[iter+0],token_vec[iter+1],token_vec[iter+2]};
        int ids[3];

        // add for stream
        // not suitable for csparql
        if (token_vec[iter+3].back() == 'S'){
            stream_query->stream_info.timeless.push_back(false);
            token_vec[iter+3].pop_back();
        } 
        if (token_vec[iter+3].back() == 'T'){
            stream_query->stream_info.timeless.push_back(true);
            token_vec[iter+3].pop_back();
        }

        if(token_vec[iter+3]=="<-"){
            swap(strs[0],strs[2]);
        }
        for(int i=0;i<3;i++){
            ids[i]=str2id(strs[i]);
        }
        if(token_vec[iter+3]=="." || token_vec[iter+3]=="->"){
            req_template.cmd_chains.push_back(ids[0]);
            req_template.cmd_chains.push_back(ids[1]);
            req_template.cmd_chains.push_back(direction_out);
            req_template.cmd_chains.push_back(ids[2]);
            iter+=4;
        } else if(token_vec[iter+3]=="<-"){
            req_template.cmd_chains.push_back(ids[0]);
            req_template.cmd_chains.push_back(ids[1]);
            req_template.cmd_chains.push_back(direction_in);
            req_template.cmd_chains.push_back(ids[2]);
            iter+=4;
        } else {
            cout<<"[error seperator] "<<endl;
            valid=false;
            return ;
        }
    }
    for(int i=0;i<req_template.cmd_chains.size();i++){
        if(req_template.cmd_chains[i]==place_holder){
            req_template.place_holder_position.push_back(i);
            return ;
        }
    }
}

bool sparql_parser::parse_stream(string filename,
                                 request_or_reply& r,
                                 map<string, int> *_stream_to_id = NULL){
    stream_to_id = _stream_to_id;
    return parse(filename, r);
}

bool sparql_parser::parse(string filename, request_or_reply& r){
    clear();
    vector<string> token_vec=get_token_vec(filename);

    r = request_or_reply();
    stream_query = &r;

    do_parse(token_vec);
    if(!valid){
        return false;
    }
    if(req_template.place_holder_position.size()!=0){
        cout<<"[error] request with place_holder"<<endl;
        return false;
    }
    
    if(join_step>=0){
        vector<int> join_vec;
        join_vec.push_back(0);
        join_vec.push_back(0);
        join_vec.push_back(join_cmd); //means join
        join_vec.push_back(join_step+1); // because we insert a new cmd in the middle
        req_template.cmd_chains.insert(req_template.cmd_chains.begin()+fork_step*4,
                                                join_vec.begin(),join_vec.end());
    }
    r.cmd_chains=req_template.cmd_chains;

    return true;
};
bool sparql_parser::parse_string(string input_str,request_or_reply& r){
    clear();
    std::stringstream ss(input_str);
    string str;
    vector<string> token_vec;
    while(ss>>str){
        token_vec.push_back(str);
        if(str=="}"){
            break;
        }
    }
    do_parse(token_vec);
    if(!valid){
        return false;
    }
    if(req_template.place_holder_position.size()!=0){
        cout<<"[error] request with place_holder"<<endl;
        return false;
    }
    r=request_or_reply();
    if(join_step>=0){
        vector<int> join_vec;
        join_vec.push_back(0);
        join_vec.push_back(0);
        join_vec.push_back(join_cmd); //means join
        join_vec.push_back(join_step+1); // because we insert a new cmd in the middle
        req_template.cmd_chains.insert(req_template.cmd_chains.begin()+fork_step*4,
                                                join_vec.begin(),join_vec.end());
    }
    r.cmd_chains=req_template.cmd_chains;
    return true;
};
bool sparql_parser::parse_template(string filename,request_template& r){
    clear();
    vector<string> token_vec=get_token_vec(filename);
    do_parse(token_vec);
    if(!valid){
        return false;
    }
    if(req_template.place_holder_position.size()==0){
        cout<<"[error] request_template without place_holder"<<endl;
        return false;
    }
    r=req_template;
    return true;
};

bool sparql_parser::find_type_of(string type,request_or_reply& r){
    clear();
    r=request_or_reply();
    r.cmd_chains.push_back(str_server->str2id[type]);
    r.cmd_chains.push_back(global_rdftype_id);
    r.cmd_chains.push_back(direction_in);
    r.cmd_chains.push_back(-1);
    return true;
};
