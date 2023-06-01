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
fig_num = '18a'

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
methods                   = fig_params['methods']
zipfians, read_ratio      = fig_params['zipfian'], fig_params['read_ratio']
target_epoch              = fig_params['target_epoch']
CN_num, client_num_per_CN = fig_params['client_num']
MN_num                    = fig_params['MN_num']
key_type                  = fig_params['key_size']
value_size                = fig_params['value_size']
cache_size                = fig_params['cache_size']
span_size                 = fig_params['span_size']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    plot_data = {
        'methods': methods,
        'X_data': zipfians,
        'Y_data': {method: [] for method in methods}
    }
    for method in methods:
        project_dir = f"{home_dir}/{method if method == 'Sherman' else 'SMART'}"
        work_dir = f"{project_dir}/build"
        env_cmd = f"cd {work_dir}"

        # change config
        sed_cmd = generate_sed_cmd('./include/Common.h', method == 'Sherman', 8 if key_type == 'randint' else 32, value_size, cache_size, MN_num, span_size)
        BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_options[method]} .. && make clean && make -j"

        cmd.all_execute(BUILD_PROJECT, CN_num)

        for zipfian in zipfians:
            CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
            ZIPFIAN_TEST = f"{env_cmd} && ./zipfian_test {CN_num} {read_ratio} {client_num_per_CN} {zipfian} 2"
            KILL_PROCESS = f"{env_cmd} && killall -9 zipfian_test"

            while True:
                try:
                    cmd.one_execute(CLEAR_MEMC)
                    cmd.all_execute(KILL_PROCESS, CN_num)
                    logs = cmd.all_long_execute(ZIPFIAN_TEST, CN_num)
                    tpt, _, _, _ = tp.get_statistics(logs, target_epoch)
                    break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")

            print_GOOD(f"[FINISHED POINT] method={method} zipfian={zipfian} tpt={tpt}")
            plot_data['Y_data'][method].append(tpt)
    # save data
    Path(output_path).mkdir(exist_ok=True)
    with (Path(output_path) / f'fig_{fig_num}.json').open(mode='w') as f:
        json.dump(plot_data, f, indent=2)


if __name__ == '__main__':
    cmd = CMDManager(cluster_ips, master_ip)
    tp = LogParser()
    t = main(cmd, tp)
    with (Path(output_path) / 'time.log').open(mode="a+") as f:
        f.write(f"fig_{fig_num}.py execution time: {int(t//60)} min {int(t%60)} s\n")

    pg = PicGenerator(output_path, style_path)
    pg.generate(fig_num)
