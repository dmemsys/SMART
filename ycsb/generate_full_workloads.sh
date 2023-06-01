#!/bin/bash

start_time=$(date +%s)

for WORKLOAD_TYPE in la a b c d; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} randint full
  python3 gen_workload.py workload${WORKLOAD_TYPE} email full
done

for WORKLOAD_TYPE in e; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} randint full
done

for WORKLOAD_TYPE in 0- 10- 20- 30- 40- 50- 60- 70- 80- 90- 100-; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} email full
done

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"
