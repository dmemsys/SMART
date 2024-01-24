#!/bin/bash

start_time=$(date +%s)

for FIG_NUM in 3a 3b 4a 4b 4c 4d 4ef 5a 5b 5c 5d 14 15 16 17 18 19 20 21 22a 22b 22c 22d 22e 22f 22g 22h; do
  python3 fig_${FIG_NUM}.py
  sleep 1m
done

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"
