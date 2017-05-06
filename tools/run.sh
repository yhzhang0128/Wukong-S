#!/bin/sh

#../build/wukong config
#/usr/bin/mpiexec -x LD_LIBRARY_PATH -hostfile mpd.hosts -n $1 ../build/wukong config mpd.hosts
/home/yhzhang/install/openmpi-1.6.5-install/bin/mpiexec -x LD_LIBRARY_PATH -hostfile mpd.hosts -n $1 ../build/wukong config mpd.hosts
