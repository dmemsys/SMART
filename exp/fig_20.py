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
fig_num = '20'
small_fig_num = {'YCSB A': 'a', 'YCSB C': 'b'}

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
method                  = fig_params['methods']
workload_names          = fig_params['workload_names']
target_epoch            = fig_params['target_epoch']
client_nums             = fig_params['client_num']
MN_num                  = fig_params['MN_num']
key_type                = fig_params['key_size']
value_size              = fig_params['value_size']
cache_size              = fig_params['cache_size']
coroutine_nums          = fig_params['coroutine_num']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    for workload, workload_name in workload_names.items():
        def get_legend(method, coroutine_num):
            # return f'{method} (w/o coroutine)' if coroutine_num <= 1 else f'{method} ({coroutine_num}-coroutine)'
            return f'{coroutine_num}-coroutine'

        methods = [get_legend(method, coroutine_num) for coroutine_num in coroutine_nums]
        plot_data = {
            'methods': methods,
            'X_data': [0] + [t[0] * t[1] for t in client_nums],
            'Y_data': {method: [0] for method in methods}
        }
        for coroutine_num in coroutine_nums:
            project_dir = f"{home_dir}/SMART"
            work_dir = f"{project_dir}/build"
            env_cmd = f"cd {work_dir}"

            # change config
            sed_cmd = generate_sed_cmd('./include/Common.h', method == 'Sherman', 8 if key_type == 'randint' else 32, value_size, cache_size, MN_num)
            cmake_option = cmake_options[method]
            BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"

            cmd.all_execute(BUILD_PROJECT)

            for CN_num, client_num_per_CN in client_nums:
                CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
                SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split_workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
                YCSB_TEST = f"{env_cmd} && ./ycsb_test {CN_num} {client_num_per_CN} {coroutine_num} {key_type} {workload_name}"
                KILL_PROCESS = f"{env_cmd} && killall -9 ycsb_test"

                cmd.all_execute(SPLIT_WORKLOADS, CN_num)
                while True:
                    try:
                        cmd.one_execute(CLEAR_MEMC)
                        cmd.all_execute(KILL_PROCESS, CN_num)
                        logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                        tpt, _, _, _, _, _, _ = tp.get_statistics(logs, target_epoch, get_avg=True)
                        break
                    except (FunctionTimedOut, Exception) as e:
                        print_WARNING(f"Error! Retry... {e}")

                print_GOOD(f"[FINISHED POINT] workload={workload} method={get_legend(method, coroutine_num)} client_num={CN_num*client_num_per_CN} tpt={tpt}")
                plot_data['Y_data'][get_legend(method, coroutine_num)].append(tpt)
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
