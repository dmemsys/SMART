
from pathlib import Path
import paramiko
import sys

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

if (len(sys.argv) != 4) :
    print(bcolors.WARNING + 'Usage:')
    print('python3 cluster_latency.py compute_node_num epoch_start epoch_num' + bcolors.ENDC)
    exit(0)

node_num = int(sys.argv[1])
epoch_start = int(sys.argv[2])
epoch_num = int(sys.argv[3])


# epoch_start = 1
# epoch_num = 10
cluster_ips = [
  '10.10.1.1',
  '10.10.1.2',
  '10.10.1.3',
  '10.10.1.4',
  '10.10.1.5',
  '10.10.1.6',
  '10.10.1.7',
  '10.10.1.8',
  '10.10.1.9',
  '10.10.1.10',
  '10.10.1.11',
  '10.10.1.12',
  '10.10.1.13',
  '10.10.1.14',
  '10.10.1.15',
  '10.10.1.16',
][:node_num]

lat_cnt = dict()
lat_dir = Path(__file__).resolve().parent
print(f'latency_dir: {lat_dir}')


def get_sftp_client(hostname):
  port = 22
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  client.connect(hostname, port, compress=True)
  return client.open_sftp()


def load_remote_lat(sftp_client : paramiko.SFTPClient, file_path):
  remote_file = sftp_client.open(file_path)
  try:
    for line in remote_file:
      lat, cnt = line.strip().split('\t', 1)
      if int(cnt):
        if lat not in lat_cnt:
          lat_cnt[lat] = 0
        lat_cnt[lat] += int(cnt)
  finally:
    remote_file.close()


def cal_lat(e_id):
  print(f'### epoch-{e_id} ###')
  all_lat = sum(lat_cnt.values())
  th50 = all_lat / 2
  th90 = all_lat * 9 / 10
  th95 = all_lat * 95 / 100
  th99 = all_lat * 99 / 100
  th999 = all_lat * 999 / 1000
  cum = 0
  for lat, cnt in sorted(lat_cnt.items(), key=lambda s:float(s[0])):
    cum += cnt
    if cum >= th50:
      print(f'p50 {lat}', end='\t')
      th50 = all_lat + 1
    if cum >= th90:
      print(f'p90 {lat}', end='\t')
      th90 = all_lat + 1
    if cum >= th95:
      print(f'p95 {lat}', end='\t')
      th95 = all_lat + 1
    if cum >= th99:
      print(f'p99 {lat}', end='\t')
      th99 = all_lat + 1
    if cum >= th999:
      print(f'p999 {lat}')
      th999 = all_lat + 1


if __name__ == '__main__':
  sftp_clients = [get_sftp_client(hostname) for hostname in cluster_ips]
  for e_id in range(epoch_start, epoch_start + epoch_num):
    lat_cnt.clear()
    for client in sftp_clients:
      load_remote_lat(client, str(lat_dir / f'epoch_{e_id}.lat'))
    cal_lat(e_id)
