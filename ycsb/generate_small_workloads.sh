#!/bin/bash

start_time=$(date +%s)

for WORKLOAD_TYPE in la a b c d; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} randint small
  python3 gen_workload.py workload${WORKLOAD_TYPE} email small
done

for WORKLOAD_TYPE in e; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} randint small
done

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"