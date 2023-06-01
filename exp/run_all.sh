#!/bin/bash

start_time=$(date +%s)

for FIG_NUM in 3a 3b 3c 3d 4a 4b 4c 4d 11 12 13 14 15 16 17 18a 18b 18c; do
  python3 fig_${FIG_NUM}.py
  sleep 1m
done

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"
