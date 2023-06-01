#!/bin/bash

# use `sudo fdisk /dev/sda` to first delete all the partition
# then carefully recreate it with a larger size at the same position
echo "p
d

d

d

d

d

n
p
1



No
p
w
" | sudo fdisk /dev/sda
