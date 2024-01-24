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
fig_num = '4'
small_fig_num = 'ef'

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
ycsb_dir      = f'{home_dir}/SMART/ycsb'
cluster_ips   = params['cluster_ips']
master_ip     = params['master_ip']
cmake_options = params['cmake_options']

# fig params
with (Path(input_path) / f'fig_{fig_num}{small_fig_num}.json').open(mode='r') as f:
    fig_params = json.load(f)
methods                   = fig_params['methods']
workload, workload_name   = fig_params['workload_names']
target_epoch              = fig_params['target_epoch']
CN_num, client_num_per_CN = fig_params['client_num']
MN_num                    = fig_params['MN_num']
key_types                 = fig_params['key_size']
value_size                = fig_params['value_size']
cache_sizes                = fig_params['cache_size']
span_size                 = fig_params['span_size']


def get_legend(method, cache_size):
    return f'{method} (w/o cache)' if cache_size == 0 else f'{method} (w/ cache)'

def get_xlabel(key_type):
    return 'int (8 byte)' if key_type == 'randint' else 'string (32 byte)'

@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    plot_methods = [(method, cache_size) for method in methods for cache_size in cache_sizes]
    plot_data = {
        small_num : {
            'methods': [get_legend(method, cache_size) for method, cache_size in plot_methods],
            'bar_groups' : [get_xlabel(key_type) for key_type in ['randint', 'email']],
            'Y_data' : {
                get_legend(method, cache_size): {}  # store RTT/p99 with int, string key, respectively
                for method, cache_size in plot_methods
            }
        } for small_num in small_fig_num
    }
    for method, cache_size in plot_methods:
        project_dir = f"{home_dir}/{method if method == 'Sherman' else 'SMART'}"
        work_dir = f"{project_dir}/build"
        env_cmd = f"cd {work_dir}"

        for key_type in key_types:
            # change config
            sed_cmd = generate_sed_cmd('./include/Common.h', method == 'Sherman', 8 if key_type == 'randint' else 32, value_size, cache_size, MN_num, span_size)
            cmake_option = cmake_options[method].replace('-DENABLE_CACHE=on', '-DENABLE_CACHE=off') if cache_size == 0 else cmake_options[method]
            cmake_option = cmake_option.replace('-DMIDDLE_TEST_EPOCH=off', '-DMIDDLE_TEST_EPOCH=on')
            BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"

            CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
            SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split_workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
            YCSB_TEST = f"{env_cmd} && ./ycsb_test {CN_num} {client_num_per_CN} 2 {key_type} {workload_name}"
            KILL_PROCESS = f"{env_cmd} && killall -9 ycsb_test"

            cmd.all_execute(BUILD_PROJECT, CN_num)
            cmd.all_execute(SPLIT_WORKLOADS, CN_num)
            while True:
                try:
                    cmd.one_execute(CLEAR_MEMC)
                    cmd.all_execute(KILL_PROCESS, CN_num)
                    logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                    _, _, _, _, _, _, tree_h = tp.get_statistics(logs, target_epoch, get_avg=True)
                    _, p99_lat = cmd.get_cluster_lats(str(Path(project_dir) / 'us_lat'), CN_num, target_epoch, get_avg=True)
                    break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")
            if method == 'Sherman' and cache_size > 0:  # Sherman caches the last-level internal node
                rtt = 1.0
            else:
                rtt = tree_h + 1
            print_GOOD(f"[FINISHED POINT] method={get_legend(method, cache_size)} RTT={rtt} p99_lat={p99_lat}")
            plot_data[small_fig_num[0]]['Y_data'][get_legend(method, cache_size)][get_xlabel(key_type)] = rtt
            plot_data[small_fig_num[1]]['Y_data'][get_legend(method, cache_size)][get_xlabel(key_type)] = p99_lat
    # save data
    Path(output_path).mkdir(exist_ok=True)
    for small_num in small_fig_num:
        with (Path(output_path) / f'fig_{fig_num}{small_num}.json').open(mode='w') as f:
            json.dump(plot_data[small_num], f, indent=2)


if __name__ == '__main__':
    cmd = CMDManager(cluster_ips, master_ip)
    tp = LogParser()
    t = main(cmd, tp)
    with (Path(output_path) / 'time.log').open(mode="a+") as f:
        f.write(f"fig_{fig_num}{small_fig_num}.py execution time: {int(t//60)} min {int(t%60)} s\n")

    pg = PicGenerator(output_path, style_path)
    for small_num in small_fig_num:
        plot_text = {}
        if small_num == 'f':
            with (Path(output_path) / f'fig_{fig_num}{small_num}.json').open(mode='r') as f:
                data = json.load(f)
                plot_text = {'text1': '{:.1f}'.format(float(data['Y_data'][get_legend('ART', 600)][get_xlabel('randint')])),
                             'text2': '{:.1f}'.format(float(data['Y_data'][get_legend('ART', 600)][get_xlabel('email')]))}
        pg.generate(fig_num + small_num, plot_text)
