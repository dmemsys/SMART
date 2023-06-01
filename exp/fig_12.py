from func_timeout import FunctionTimedOut
from pathlib import Path
import json

from utils.cmd_manager import CMDManager
from utils.log_parser import LogParser
from utils.sed_generator import generate_sed_cmd
from utils.color_printer import print_GOOD, print_WARNING
from utils.func_timer import print_func_time
from utils.pic_generator import PicGenerator


input_path = './params'
style_path = "./styles"
output_path = './results'
fig_num = '12'
small_fig_num = {'YCSB LOAD': 'a', 'YCSB A': 'b', 'YCSB B': 'c', 'YCSB C': 'd', 'YCSB D': 'e'}

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
ycsb_dir      = f'{home_dir}/SMART/ycsb'
cluster_ips   = params['cluster_ips']
master_ip     = params['master_ip']
cmake_options = params['cmake_options']

# fig params
with (Path(input_path) / f'fig_{fig_num}.json').open(mode='r') as f:
    fig_params = json.load(f)
methods            = fig_params['methods']
workload_names     = fig_params['workload_names']
target_epochs      = fig_params['target_epoch']
CN_and_client_nums = fig_params['client_num']  # as Sherman/ART saturates the network more easily than SMART, we test them with fewer clients
MN_num             = fig_params['MN_num']
key_type           = fig_params['key_size']
value_size         = fig_params['value_size']
cache_size         = fig_params['cache_size']
span_size          = fig_params['span_size']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    for workload, workload_name in workload_names.items():
        plot_data = {
            'methods': methods,
            'X_data': {method: [] for method in methods},
            'Y_data': {method: [] for method in methods}
        }
        for method in methods:
            project_dir = f"{home_dir}/{method if method == 'Sherman' else 'SMART'}"
            work_dir = f"{project_dir}/build"
            env_cmd = f"cd {work_dir}"

            # change config
            sed_cmd = generate_sed_cmd('./include/Common.h', method == 'Sherman', 8 if key_type == 'randint' else 32, value_size, cache_size, MN_num, span_size)
            cmake_option = cmake_options[method]
            if workload != 'YCSB LOAD':
                cmake_option = cmake_option.replace('-DLONG_TEST_EPOCH=off', '-DLONG_TEST_EPOCH=on')
            BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"

            cmd.all_execute(BUILD_PROJECT)

            for CN_num, client_num_per_CN in CN_and_client_nums[method][workload]:
                CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
                SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split_workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
                YCSB_TEST = f"{env_cmd} && ./ycsb_test {CN_num} {client_num_per_CN} 2 {key_type} {workload_name}"
                KILL_PROCESS = f"{env_cmd} && killall -9 ycsb_test"

                cmd.all_execute(SPLIT_WORKLOADS, CN_num)
                while True:
                    try:
                        cmd.one_execute(CLEAR_MEMC)
                        cmd.all_execute(KILL_PROCESS, CN_num)
                        logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                        _, p99_lat = cmd.get_cluster_lats(str(Path(project_dir) / 'us_lat'), CN_num, target_epochs[workload])
                        tpt, _, _, _ = tp.get_statistics(logs, target_epochs[workload])
                        break
                    except (FunctionTimedOut, Exception) as e:
                        print_WARNING(f"Error! Retry... {e}")

                print_GOOD(f"[FINISHED POINT] workload={workload} method={method} client_num={CN_num*client_num_per_CN} tpt={tpt} p99_lat={p99_lat}")
                plot_data['X_data'][method].append(tpt)
                plot_data['Y_data'][method].append(p99_lat)
        # save data
        Path(output_path).mkdir(exist_ok=True)
        with (Path(output_path) / f'fig_{fig_num}{small_fig_num[workload]}.json').open(mode='w') as f:
            json.dump(plot_data, f, indent=2)


if __name__ == '__main__':
    cmd = CMDManager(cluster_ips, master_ip)
    tp = LogParser()
    t = main(cmd, tp)
    with (Path(output_path) / 'time.log').open(mode="a+") as f:
        f.write(f"fig_{fig_num}.py execution time: {int(t//60)} min {int(t%60)} s\n")

    pg = PicGenerator(output_path, style_path)
    for small_num in small_fig_num.values():
        pg.generate(fig_num + small_num)
