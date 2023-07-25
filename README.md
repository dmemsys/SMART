# SMART: A High-Performance Adaptive Radix Tree for Disaggregated Memory

This is the implementation repository of our *OSDI'23* paper: **SMART: A High-Performance Adaptive Radix Tree for Disaggregated Memory**.
This artifact provides the source code of **SMART** and scripts to reproduce all the experiment results in our paper.
**SMART**, a di<u>**S**</u>aggregated-me<u>**M**</u>ory-friendly <u>**A**</u>daptive <u>**R**</u>adix <u>**T**</u>ree, is the first radix tree for disaggregated memory with high performance.


- [SMART](#smart-a-high-performance-adaptive-radix-tree-for-disaggregated-memory)
  * [Supported Platform](#supported-platform)
  * [Create Cluster](#create-cluster)
  * [Source Code *(Artifacts Available)*](#source-code-artifacts-available)
  * [Environment Setup](#environment-setup)
  * [YCSB Workloads](#ycsb-workloads)
  * [Getting Started *(Artifacts Functional)*](#getting-started-artifacts-functional)
  * [Reproduce All Experiment Results *(Results Reproduced)*](#reproduce-all-experiment-results-results-reproduced)
  * [Paper](#paper)


## Supported Platform
We strongly recommend you to run SMART using the r650 instances on [CloudLab](https://www.cloudlab.us/) as the code has been thoroughly tested there.
We haven't done any test in other hardware environment.

If you want to reproduce the results in the paper, 16 r650 machines are needed; otherwise, fewer machines (*i.e.*, 3) is OK.
Each r650 machine has two 36-core Intel Xeon CPUs, 256GB of DRAM, and one 100Gbps Mellanox ConnectX-6 IB RNIC. Each RNIC is connected to a 100Gbps Ethernet switch.


## Create Cluster

You can follow the following steps to create an experimental cluster with 16 nodes on CloudLab:

1) Log into your own account.

2) Now you have logged into Cloublab console. If there are not 16 r650 machines available, please submit a reservation **in advance** via `Experiments`|-->`Reserve Nodes`.

3) Click `Experiments`|-->`Create Experiment Profile`. Upload `./script/cloudlab.profile` provided in this repo.
Input a file name (*e.g.*, SMART) and click `Create` to generate the experiment profile for SMART.

4) Click `Instantiate` to create a 16-node cluster using the profile (This takes about 7 minutes).

5) Try logging into and check each node using the SSH commands provided in the `List View` on CloudLab. If you find some nodes have broken shells (which happens sometimes in CloudLab), you can reload them via `List View`|-->`Reload Selected`.


## Source Code *(Artifacts Available)*
Now you can log into all the CloudLab nodes. Using the following command to clone this github repo in the home directory of **all** nodes:
```shell
git clone https://github.com/dmemsys/SMART.git
```


## Environment Setup

You have to install the necessary dependencies in order to build SMART.
Note that you should run the following steps on **all** nodes you have created.

1) Set bash as the default shell. And enter the SMART directory.
    ```shell
    sudo su
    chsh -s /bin/bash
    cd SMART
    ```

2) Install Mellanox OFED.
    ```shell
    # It doesn't matter to see "Failed to update Firmware"
    # This takes about 8 minutes
    sh ./script/installMLNX.sh
    ```

3) Resize disk partition.

    Since the r650 nodes remain a large unallocated disk partition by default, you should resize the disk partition using the following command:
    ```shell
    # It doesn't matter to see "Failed to remove partition" or "Failed to update system information"
    sh ./script/resizePartition.sh
    # This takes about 6 minutes
    reboot
    # After rebooting, log into all nodes again and execute:
    sudo su
    resize2fs /dev/sda1
    ```

4) Enter the SMART directory. Install libraries and tools.
    ```shell
    cd SMART
    # This takes about 3 minutes
    sh ./script/installLibs.sh
    ```


## YCSB Workloads

You should run the following steps on **all** nodes.

1) Download YCSB source code.
    ```shell
    sudo su
    cd SMART/ycsb
    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
    tar xfvz ycsb-0.11.0.tar.gz
    mv ycsb-0.11.0 YCSB
    ```
2) Download the email dataset for string workloads.
    ```shell
    gdown --id 1ZJcQOuFI7IpAG6ZBgXwhjEeKO1T7Alzp
    ```

3) We first generate a small set of YCSB workloads here for **quick start**.
    ```shell
    # This takes about 2 minutes
    sh generate_small_workloads.sh
    ```


## Getting Started *(Artifacts Functional)*

* HugePages setting.
    ```shell
    sudo su
    echo 36864 > /proc/sys/vm/nr_hugepages
    ulimit -l unlimited
    ```

* Return to the SMART root directory (`./SMART`) and execute the following commands on **all** nodes to compile SMART:
    ```shell
    mkdir build; cd build; cmake ..; make -j
    ```

* Execute the following command on **one** node to initialize the memcached:
    ```shell
    /bin/bash ../script/restartMemc.sh
    ```

* Execute the following command on **all** nodes to split the workloads:
    ```shell
    python3 ../ycsb/split_workload.py <workload_name> <key_type> <CN_num> <client_num_per_CN>
    ```
    * workload_name: the name of the workload to test (*e.g.*, `a` / `b` / `c` / `d` / `la`).
    * key_type: the type of key to test (*i.e.*, `randint` / `email`).
    * CN_num: the number of CNs.
    * client_num_per_CN: the number of clients in each CN.

    **Example**:
    ```shell
    python3 ../ycsb/split_workload.py a randint 16 24
    ```

* Execute the following command in **all** nodes to conduct a YCSB evaluation:
    ```shell
    ./ycsb_test <CN_num> <client_num_per_CN> <coro_num_per_client> <key_type> <workload_name>
    ```
    * coro_num_per_client: the number of coroutine in each client (2 is recommended).

    **Example**:
    ```shell
    ./ycsb_test 16 24 2 randint a
    ```

* Results:
    * Throughput: the throughput of **SMART** among all the cluster will be shown in the terminal of the first node (with 10 epoches by default).
    * Latency: execute the following command in **one** node to calculate the latency results of the whole cluster:
        ```shell
        python3 ../us_lat/cluster_latency.py <CN_num> <epoch_start> <epoch_num>
        ```

        **Example**:
        ```shell
        python3 ../us_lat/cluster_latency.py 16 1 10
        ```

## Reproduce All Experiment Results *(Results Reproduced)*
We provide code and scripts in `./exp` folder for reproducing our experiments. For more details, see [./exp/README.md](./exp).

## Paper
If you use SMART in your research, please cite our paper:
```bibtex
@inproceedings {smart2023,
  author = {Xuchuan Luo and Pengfei Zuo and Jiacheng Shen and Jiazhen Gu and Xin Wang and Michael R. Lyu and Yangfan Zhou},
  title = {{SMART}: A High-Performance Adaptive Radix Tree for Disaggregated Memory},
  booktitle = {17th {USENIX} Symposium on Operating Systems Design and Implementation ({OSDI} 23)},
  year = {2023},
  isbn = {978-1-939133-34-2},
  address = {Boston, MA},
  pages = {553--571},
  url = {https://www.usenix.org/conference/osdi23/presentation/luo},
  publisher = {{USENIX} Association},
  month = jul,
}
```
