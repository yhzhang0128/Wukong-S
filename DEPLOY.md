# Deployment Instructions

For compilation and usage, please follow the instructions step by step.

# Dependence

**NOTICE:**  before the last two step, you should only work on master machine.

First, notify the location of the code base, for example:

    export WUKONG_ROOT=$HOME/graph-query

## Boost+mpi

    cd  deps/
    tar jxvf  boost_1_58_0.tar.bz2  
    mkdir boost_1_58_0-install
    ./bootstrap.sh --prefix=../boost_1_58_0-install  

add following lines in project-config.jam  

    using mpi ;  

install

    ./b2 install  


## Intel-TBB

    cd deps/
    tar -xzf tbb44_20151115oss_src.tgz  
    cd tbb44_20151115oss;  
    make;  

## ZeroMQ+cpp-binding

http://zeromq.org/

    cd  deps/
    tar -zxvf zeromq-4.0.5.tar.gz
    mkdir zeromq-4.0.5-install
    cd zeromq-4.0.5
    ./configure --prefix=${WUKONG_ROOT}/deps/zeromq-4.0.5-install/
    make
    make install
    cd ..
    cp zmq.hpp  zeromq-4.0.5-install/include/

## Without RDMA

modify CMakeLists.txt, only compile wukong_zmq

## Shell Environment
add following lines in $HOME/.bashrc or the config file of other shell you're using

    export WUKONG_ROOT=$HOME/graph-query
    source ${WUKONG_ROOT}/deps/tbb44_20151115oss/build/linux_intel64_gcc_cc4.8_libc2.19_kernel3.18.24+_release/tbbvars.sh
    export CPATH=${WUKONG_ROOT}/deps/zeromq-4.0.5-install/include:$CPATH
    export LIBRARY_PATH=${WUKONG_ROOT}/deps/zeromq-4.0.5-install/lib:$LIBRARY_PATH
    export LD_LIBRARY_PATH=${WUKONG_ROOT}/deps/zeromq-4.0.5-install/lib:$LD_LIBRARY_PATH

dictionary of tbb may need to be changed, due to different kernel version.

## Slave Configuration

copy zeromq-4.0.5-install/ and tbb44_20151115oss/ to deps/ at other machines

# Compile

1. mkdir wukong/ at all slaves
2. modify tools/mpd.hosts to set ip addresses of slaves
3. modify CMakeLists.txt to set CMAKE_CXX_COMPILER
4. compile and sync  

    cd tools;
    ./make.sh
    ./sync.sh
5. copy zeromq-4.0.5-install/ and tbb44_20151115oss/ to deps/ at slaves

    scp -r ${WUKONG_ROOT}/deps/zeromq-4.0.5-install/ slave1:${WUKONG_ROOT}/deps/
    scp -r ${WUKONG_ROOT}/deps/tbb44_20151115oss/ slave1:${WUKONG_ROOT}/deps/

# Execution

    ./run.sh nmachine

# Input Data

1. If there is space at the raw_data, convert it to underline first

        cat raw_file | sed -e 's/ /_/g’ > convert_file
2. use generate_data to convert raw_data into id_data .

        ./generate_data lubm_raw_40/ id_lubm_40/
3. put id_data to NFS , and set the global_input_folder at config file

4. use str_normal_minimal if loading str_normal causes too much time
    grep "<http://www.Department0.University0.edu>" str_normal >> str_normal_minimal
