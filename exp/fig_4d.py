from func_timeout import FunctionTimedOut
from pathlib import Path
import json

from utils.cmd_manager import CMDManager
from utils.log_parser import LogParser
from utils.sed_generator import sed_MN_num
from utils.color_printer import print_GOOD, print_WARNING
from utils.func_timer import print_func_time
from utils.pic_generator import PicGenerator


input_path = './params'
style_path = "./styles"
output_path = './results'
fig_num = '4d'

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
cluster_ips   = params['cluster_ips']
master_ip     = params['master_ip']

# fig params
with (Path(input_path) / f'fig_{fig_num}.json').open(mode='r') as f:
    fig_params = json.load(f)
zipfian, read_ratio       = fig_params['zipfian'], fig_params['read_ratio']
client_num_per_CNs        = fig_params['client_num_per_CN']
MN_num                    = fig_params['MN_num']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    CN_num = 1
    plot_data = {
        'methods': ['RDMA_WRITE', 'RDMA_CAS'],
        'X_data': [1] + client_num_per_CNs,
        'Y_data': {'RDMA_WRITE': [0], 'RDMA_CAS': [0]}
    }
    project_dir = f"{home_dir}/SMART"
    work_dir = f"{project_dir}/build"
    env_cmd = f"cd {work_dir}"

    # change config
    sed_cmd = sed_MN_num('./include/Common.h', MN_num)
    BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake .. && make clean && make -j"

    cmd.all_execute(BUILD_PROJECT, CN_num)

    for client_num_per_CN in client_num_per_CNs:
        CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
        REDUNDANT_TEST = f"{env_cmd} && ./redundant_test {read_ratio} {client_num_per_CN} {zipfian}"
        KILL_PROCESS = f"{env_cmd} && killall -9 redundant_test"

        while True:
            try:
                cmd.one_execute(CLEAR_MEMC)
                cmd.all_execute(KILL_PROCESS, CN_num)
                logs = cmd.all_long_execute(REDUNDANT_TEST, CN_num)
                _, redundant_write, redundant_cas = tp.get_redundant_statistics(list(logs.values())[0])  # CN_num = 1
                break
            except (FunctionTimedOut, Exception) as e:
                print_WARNING(f"Error! Retry... {e}")

        print_GOOD(f"[FINISHED POINT] RDMA_READ zipfian={zipfian} avg.redundant_write={redundant_write} avg.redundant_cas={redundant_cas}")
        plot_data['Y_data']['RDMA_WRITE'].append(redundant_write)
        plot_data['Y_data']['RDMA_CAS'].append(redundant_cas)
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
