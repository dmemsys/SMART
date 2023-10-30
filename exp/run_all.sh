#!/bin/bash

start_time=$(date +%s)

for FIG_NUM in 3a 3b 4a 4b 4c 4d 5a 5b 5c 5d 13 14 15 16 17 18 19 20 21a 21b 21c 21d 21e 21f; do
  python3 fig_${FIG_NUM}.py
  sleep 1m
done

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"
