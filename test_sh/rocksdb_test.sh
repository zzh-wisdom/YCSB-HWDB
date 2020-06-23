#/bin/bash

workload="workloads/workloada.spec"
zipfian_workload="workloads/test_zipfian_load.spec"
dbpath="/tmp/rocksdb-test"
moreworkloads="workloads/workloada.spec:workloads/workloadb.spec:workloads/workloadc.spec:workloads/workloadd.spec:workloads/workloade.spec:workloads/workloadf.spec"

#./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -load true -run true -dbstatistics true

#./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -load true -morerun $moreworkloads -dbstatistics true

if [ -n "$dbpath" ];then
    rm -f $dbpath/*
fi

#cmd="./ycsbc -db rocksdb -dbpath $dbpath -threads 4 -P $workload -load true -morerun $moreworkloads -dbstatistics true "
#cmd="./ycsbc -db basic -dbpath $dbpath -threads 1 -P $workload -load true -morerun $moreworkloads -dbstatistics true "

#cmd="./ycsbc -db rocksdb -dbpath $dbpath -threads 4 -P $zipfian_workload -run true -dbstatistics false "
cmd="./ycsbc -db rocksdb -dbpath $dbpath -threads 4 -P $workload -load true -run true -morerun $moreworkloads -dbstatistics false "

echo $cmd
eval $cmd

# ./ycsbc -db rocksdb -dbpath /tmp/rocksdb-test -threads 4 -P "workloads/workloada.spec" -load true -dbstatistics true