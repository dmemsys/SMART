# Reproduce All Experiment Results

In this folder, we provide code and scripts for reproducing figures in [our paper](https://www.usenix.org/system/files/osdi23-luo.pdf). A 16-node r650 cluster is needed to reproduce all the results.

The name of each script corresponds to the number of each figure in our paper.
Since some of the scripts in this directory take a long computing time (as we mark below), we strongly recommend you create a ***tmux session*** on each node to avoid script interruption due to network instability.


## Full YCSB workloads

In our paper, we use YCSB workloads, each of which includes 60 million entries and operations.

Using the following commands to generate all the workloads:
```shell
sudo su
cd SMART/ycsb
rm -rf workloads
# This takes about 4 hours and 15 minutes. Let it run and you can do your own things.
sh generate_full_workloads.sh
```

## Source Code of Baselines
* For most figures, there are three lines: **SMART**, **Sherman**, and **ART**.
SMART stands for our system. Sherman is [a recent system](https://github.com/thustorage/Sherman) published in *SIGMOG'22*; we use it as a baseline to show the performance bottleneck of the B+ tree on disaggregated memory (DM). ART is a naive adaptive radix tree (ART) design that we port to DM.

* SMART has been cloned in the home directory of each node. The source codes of ART and any other SMART variants are included inside SMART (*i.e.*, this repo).

* We have forked Sherman and modified it to fit our reproduction scripts. Use the following command to clone the [forked Sherman](https://github.com/River861/Sherman) in the same path of SMART (*i.e.*, the home directory) in each node:
    ```shell
    git clone https://github.com/River861/Sherman.git
    ```

## Additional Setup

* Change the `home_dir` value in `./params/common.json` to your actual home directory path (*i.e.*, /users/XXX).
    ```json
    "home_dir" : "/your/home/directory"
    ```

* Change the `master_ip` value in `./params/common.json` to the IP address of a master node of the r650 cluster. In the r650 cluster,  we define a node which can directly establish SSH connections to other nodes as a **master** node.
    ```json
    "master_ip": "10.10.1.3"
    ```


## Start to Run

* All the scripts are only need to run on a master node (with IP `master_ip`).

    You can run all the scripts with a single batch script using the following command:
    ```shell
    sudo su
    cd SMART/exp
    # This takes about 17 hours and 25 minutes. Let it run and just check the results in the next day.
    sh run_all.sh
    ```
    Or, you can run the scripts one by one, or run some specific scripts if some figures are skipped or show unexpected results during `run_all.sh` (due to network instability, which happens sometimes):
    ```shell
    sudo su
    cd SMART/exp
    # This takes about 1 hour and 41 minutes
    python3 fig_3a.py
    # This takes about 34 minutes
    python3 fig_3b.py
    # This takes about 33 minutes
    python3 fig_3c.py
    # This takes about 1 hour and 2 minutes
    python3 fig_3d.py
    # This takes about 1 hour and 19 minutes
    python3 fig_4a.py
    # This takes about 5 minutes
    python3 fig_4b.py
    # This takes about 7 minutes
    python3 fig_4c.py
    # This takes about 6 minutes
    python3 fig_4d.py
    # This takes about 4 hours and 20 minutes
    python3 fig_11.py
    # This takes about 4 hours and 24 minutes
    python3 fig_12.py
    # This takes about 27 minutes
    python3 fig_13.py
    # This takes about 17 minutes
    python3 fig_14.py
    # This takes about 49 minutes
    python3 fig_15.py
    # This takes about 5 minutes
    python3 fig_16.py
    # This takes about 20 minutes
    python3 fig_17.py
    # This takes about 13 minutes
    python3 fig_18a.py
    # This takes about 26 minutes
    python3 fig_18b.py
    # This takes about 36 minutes
    python3 fig_18c.py
    ```

* The json results and PDF figures of each scirpt will be stored inside a new directoty `./results`.

    The results you get may not be exactly the same as the ones shown in the paper due to changes of physical machines.
    And some curves (*e.g.*, cache hit ratio, p99 latency) may fluctuate due to the instability of RNICs in the cluster.
    However, all results here support the conclusions we made in the paper.
