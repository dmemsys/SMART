from typing import Optional

from utils.color_printer import print_FAIL


class LogParser(object):

    def __init__(self):
        pass

    def get_statistics(self, logs: dict, target_epoch: int, get_avg: bool=False):
        for log in logs.values():
            if get_avg:
                tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = self.__parse_log_avg(log, target_epoch)
            else:
                tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = self.__parse_log(log, target_epoch)
            if tpt is not None:
                break
        assert(tpt is not None)
        return tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate

    def __parse_log(self, log, target_epoch):
        tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = None, 0, 0, 0
        flag = False
        for line in log:
            if f"epoch {target_epoch} passed!" in line:
                flag = True

            elif flag and "cluster throughput" in line:
                tpt = float(line.strip().split(' ')[2])

            elif flag and "cache hit rate" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    cache_hit_rate = float(data)

            elif flag and "avg. lock/cas fail cnt" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    lock_fail_cnt = float(data)

            elif flag and "read invalid leaf rate" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    leaf_invalid_rate = float(data) * 100
                break

        return tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate

    def __parse_log_avg(self, log, target_epoch):
        tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = [], [], [], []
        flag = False
        start_epoch = max(target_epoch // 2, target_epoch - 4)
        cnt = start_epoch
        for line in log:
            if f"epoch {start_epoch} passed!" in line:
                flag = True

            elif flag and "cluster throughput" in line:
                tpt.append(float(line.strip().split(' ')[2]))

            elif flag and "cache hit rate" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    cache_hit_rate.append(float(data))

            elif flag and "avg. lock/cas fail cnt" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    lock_fail_cnt.append(float(data))

            elif flag and "read invalid leaf rate" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    leaf_invalid_rate.append(float(data) * 100)
                cnt += 1
                if cnt == target_epoch:
                    break

        def get_avg(l):
            return sum(l) / len(l) if l else 0

        return get_avg(tpt) if tpt else None, get_avg(cache_hit_rate), get_avg(lock_fail_cnt), get_avg(leaf_invalid_rate)

    def get_redundant_statistics(self, log):
        redundant_read, redundant_write, redundant_cas = None, None, None
        flag = False
        for line in log:
            if "Calculation done!" in line:
                flag = True

            elif flag and "Avg. redundant rdma_read" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    redundant_read = float(data)

            elif flag and "Avg. redundant rdma_write" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    redundant_write = float(data)

            elif flag and "Avg. redundant rdma_cas" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    redundant_cas = float(data)
                break

        return redundant_read, redundant_write, redundant_cas
