from typing import List
from pathlib import Path
import paramiko


class LatParser(object):

    def __init__(self, clients: List[paramiko.SSHClient]):
        self.__sftps = [cli.open_sftp() for cli in clients]
        self.__lat_cnt = dict()


    def load_remote_lats(self, lat_dir_path: str, CN_num: int, epoch_start: int = 1, epoch_num: int = 10):
        p50_p99_lats = {}
        for e_id in range(epoch_start, epoch_start + epoch_num):
            self.__lat_cnt.clear()
            for sftp in self.__sftps[:CN_num]:
                remote_file = sftp.open(str(Path(lat_dir_path) / f'epoch_{e_id}.lat'))
                try:
                    for line in remote_file:
                        lat, cnt = line.strip().split('\t', 1)
                        if int(cnt):
                            if lat not in self.__lat_cnt:
                                self.__lat_cnt[lat] = 0
                            self.__lat_cnt[lat] += int(cnt)
                finally:
                    remote_file.close()
            if self.__lat_cnt:
                p50_p99_lats[e_id] = self.__cal_lat()
                print(f'epoch_id={e_id} p50_lat={p50_p99_lats[e_id][0]} p99_lat={p50_p99_lats[e_id][1]}')
        return p50_p99_lats


    def __cal_lat(self):
        all_lat = sum(self.__lat_cnt.values())
        th50 = all_lat / 2
        th99 = all_lat * 99 / 100
        cum = 0
        p50, p99 = None, None
        for lat, cnt in sorted(self.__lat_cnt.items(), key=lambda s:float(s[0])):
            cum += cnt
            if cum >= th50:
                p50 = float(lat)
                th50 = all_lat + 1
            if cum >= th99:
                p99 = float(lat)
                break
        assert(p50 is not None and p99 is not None)
        return p50, p99

