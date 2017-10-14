#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <dirent.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <assert.h>

using namespace std;

void truncate_line(char *str, string& subject, string& predict, string& object){
  // static(sibp_p941, foaf_based_near, 'United_States').                                                               
  // -----subject----- ----predict----- -----object------                                                               
  int idx = 0, start = 0;

  // clean subject
  for(; str[idx] != ' '; idx++);
  
  subject = string(str, start, idx - start);
  start = ++idx;

  // clean predict
  for(; str[idx] != ' '; idx++);
  predict = string(str, start, idx - start);
  start = ++idx;
  
  // clean object
  for(; str[idx] != '\0'; idx++)
    if (str[idx] == ' ' || str[idx] == '\t') str[idx] = '+';
  object = string(str, start, idx - start - 2);
  
}

int main(int argc, char** argv){
  unordered_map<string, int> str_to_id;
  vector<string> normal_str;
  // index node include: predict, type
  vector<string> index_str;

  struct dirent *ptr;
  DIR *dir;
  
  if (argc <= 2){
    printf("Usage: ./generate_lsbench_data src_dir dst_dir\n");
    return 0;
  }
  dir = opendir(argv[1]);

  str_to_id["__PREDICT__"] = 0;
  index_str.push_back("__PREDICT__");
  
  // LSBench doesn't have rdf-type natively
  str_to_id["TYPE"] = 1;
  index_str.push_back("TYPE");

  int nbit_predict = 17;
  size_t next_index_id = 2;
  size_t next_normal_id = 1 << nbit_predict;

  int nfiles = 0;
  while((ptr = readdir(dir)) != NULL){
    if (ptr->d_name[0] == '.')
      continue;
    
    ifstream file((string(argv[1]) + "/" + string(ptr->d_name)).c_str());
    ofstream output((string(argv[2]) + "/id_" + string(ptr->d_name)).c_str());

    printf("No.%d, loading %s ...\n", ++nfiles, ptr->d_name);

    string subject, predict, object;
    char buffer[1024];
    while(file.getline(buffer, 1024)){
      if (buffer[0] == '@') continue;  // prefix definition
      truncate_line(buffer, subject, predict, object);

      //cout << subject << " " << predict << " " << object << endl;
      int id[3];

      // give subject an ID
      if (str_to_id.find(subject) == str_to_id.end()){
        str_to_id[subject] = next_normal_id;
        next_normal_id++;
        normal_str.push_back(subject);
      }

      // give predict an ID
      if (str_to_id.find(predict) == str_to_id.end()){
        str_to_id[predict] = next_index_id;
        next_index_id++;
        index_str.push_back(predict);
      }

      // give object an ID
      // No concept of type in LSBench
      if (str_to_id.find(object) == str_to_id.end()){
        str_to_id[object] = next_normal_id;
        next_normal_id++;
        normal_str.push_back(object);
      }


      id[0] = str_to_id[subject];
      id[1] = str_to_id[predict];
      id[2] = str_to_id[object];
   
      output << id[0] << "\t" << id[1] << "\t" << id[2] << endl;
    }
  }


  // str_normal
  {
    ofstream f_normal((string(argv[2]) + "/str_normal").c_str());
    for(int i = 0; i < normal_str.size(); i++){
      f_normal << normal_str[i] << "\t" << str_to_id[normal_str[i]] << endl;
    }
  }

  // str_index
  {
    ofstream f_index((string(argv[2]) + "/str_index").c_str());
    for(int i = 0; i < index_str.size(); i++){
      f_index << index_str[i] << "\t" << str_to_id[index_str[i]] << endl;
    }
  }

  cout << "sizeof str_to_id=" << str_to_id.size() << endl;
  cout << "sizeof normal_str=" << normal_str.size() << endl;
  cout << "sizeof index_str=" << index_str.size() << endl;
  return 0;
}
