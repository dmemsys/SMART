import paramiko
import time
import socket
from func_timeout import func_set_timeout

from utils.lat_parser import LatParser
from utils.color_printer import print_OK, print_FAIL


BUFFER_SIZE = 16 * 1024 * 1024
END_PROMPT = '[END]'
OOM_PROMPT = 'shared memory space run out'
DEADLOCK_PROMPT = 'Deadlock'


class CMDManager(object):

    def __init__(self, cluster_ips: list, master_ip: str):
        super().__init__()
        self.__cluster_ips = cluster_ips
        self.__master_idx = cluster_ips.index(master_ip)
        self.__CNs = [self.__get_ssh_CNs(hostname) for hostname in cluster_ips]
        self.__shells = [cli.invoke_shell() for cli in self.__CNs]
        for shell in self.__shells:
            shell.setblocking(False)
            self.__clear_shell(shell)
        self.__lat_parser = LatParser(self.__CNs)

    def __del__(self):
        for cli in self.__CNs:
            cli.close()

    def __clear_shell(self, shell):
        shell.send('\n')
        while True:
            try:
                shell.recv(BUFFER_SIZE)
                break
            except socket.timeout:
                continue

    def __match_prompt(self, content: str, end: str):
        if end in content:
            return True
        return False

    def __get_ssh_CNs(self, hostname: str):
        port = 22
        cli = paramiko.SSHClient()
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cli.connect(hostname, port, compress=True)
        return cli

    @func_set_timeout(60)
    def all_execute(self, command: str, CN_num: int = -1):
        if CN_num < 0:  # -1 means use all CNs
            CN_num = len(self.__CNs)
        outs = {}
        errs = {}
        stdouts = {}
        stderrs = {}
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__cluster_ips[:CN_num]}')
        for i in range(CN_num):
            cli_ip = self.__cluster_ips[i]
            _, stdouts[cli_ip], stderrs[cli_ip] = self.__CNs[i].exec_command(command, get_pty=True)
        for i in range(CN_num):
            cli_ip = self.__cluster_ips[i]
            outs[cli_ip] = stdouts[cli_ip].readlines()  # block here
            errs[cli_ip] = stderrs[cli_ip].readlines()  # TODO: Retry
            for line in outs[cli_ip]:
                print(f'[CN {cli_ip} OUTPUT] {line.strip()}')
            for line in errs[cli_ip]:
                print_FAIL(f'[CN {cli_ip} ERROR] {line.strip()}')
        return outs


    def one_execute(self, command: str):
        # one of the nodes (i.e., master node) will do some special task
        cli = self.__CNs[self.__master_idx]
        cli_ip = self.__cluster_ips[self.__master_idx]
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IP={cli_ip}')
        try:
            _, stdout, stderr = cli.exec_command(command, get_pty=True)
            out = stdout.readlines()  # block here
            err = stderr.readlines()
            for line in out:
                print(f'[CN {cli_ip} OUTPUT] {line.strip()}')
            for line in err:
                print_FAIL(f'[CN {cli_ip} OUTPUT] {line.strip()}')
        except:
            print_FAIL(f'[CN {cli_ip}] FAILURE: {command}')
        return out


    @func_set_timeout(600)
    def all_long_execute(self, command: str, CN_num: int = -1):
        if CN_num < 0:  # -1 means use all CNs
            CN_num = len(self.__CNs)
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__cluster_ips[:CN_num]}')
        if not command.endswith('\n'):
            command += '\n'
        for i in range(CN_num):
            self.__shells[i].send(command)
        for i in range(CN_num):
            while not self.__shells[i].recv_ready():
                time.sleep(0.2)

        outs = {self.__cluster_ips[i]: '' for i in range(CN_num)}
        for i in range(CN_num):
            cli_ip = self.__cluster_ips[i]
            while not self.__match_prompt(outs[cli_ip], END_PROMPT):
                try:
                    msg = self.__shells[i].recv(BUFFER_SIZE).decode()
                    if msg:
                        print(f'[CN {cli_ip} OUTPUT] {msg.strip()}')
                        outs[cli_ip] += msg
                    if self.__match_prompt(outs[cli_ip], OOM_PROMPT):
                        raise Exception(OOM_PROMPT)
                    if self.__match_prompt(outs[cli_ip], DEADLOCK_PROMPT):
                        raise Exception(DEADLOCK_PROMPT)
                except socket.timeout:
                    continue

        for ip in outs.keys():
            outs[ip] = outs[ip].strip().split('\n')
        return outs

    def get_cluster_lats(self, lat_dir_path: str, CN_num: int, target_epoch: int, get_avg: bool=False):
        if get_avg:
            start_epoch = max(target_epoch // 2, target_epoch - 4)
            p50_p99_lats = self.__lat_parser.load_remote_lats(lat_dir_path, CN_num, start_epoch, target_epoch - start_epoch + 1)
            assert(p50_p99_lats)
            p50s, p99s = zip(*list(p50_p99_lats.values()))
            p50 = sum(p50s) / len(p50s)
            p99 = sum(p99s) / len(p99s)
        else:
            p50, p99 = self.__lat_parser.load_remote_lats(lat_dir_path, CN_num, target_epoch, 1)[target_epoch]  # to save time, we simply use the latency result in one epoch
        assert(p50 is not None and p99 is not None)
        return p50, p99
