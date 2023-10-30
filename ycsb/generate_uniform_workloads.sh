#!/bin/bash

start_time=$(date +%s)

for WORKLOAD_TYPE in a b c d; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} randint uniform
  python3 gen_workload.py workload${WORKLOAD_TYPE} email uniform
done

for WORKLOAD_TYPE in e; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} randint uniform
done


end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"
