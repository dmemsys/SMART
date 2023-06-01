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

if (len(sys.argv) != 5) and (len(sys.argv) != 6):
    print(bcolors.WARNING + 'Usage:')
    print('python3 split_workload.py workload_name[a/b/c/d/e] key_type[randint/email] CN_num client_per_CN (loader_num)' + bcolors.ENDC)
    exit(0)

workload = sys.argv[1]
keyType = sys.argv[2]
CNum = sys.argv[3]
clientPerNode = sys.argv[4]
loader_num = '8' if len(sys.argv) == 5 else sys.argv[5]

print(bcolors.OKGREEN + 'workload = ' + workload)
print('key type = ' + keyType)
print('CN num = ' + CNum)
print('client-num/CN = ' + clientPerNode)
print('loader_num = ' + loader_num + bcolors.ENDC)

CNum = int(CNum)
clientPerNode = int(clientPerNode)
loader_num = int(loader_num)
splitNums = {
    "load": CNum * min(clientPerNode, loader_num),
    "txn": CNum * clientPerNode
}
for op in ["load", "txn"]:
    splitNum = splitNums[op]
    fname = f"{sys.path[0]}/workloads/{op}_{keyType}_workload{workload}"
    print("spliting: ", fname)
    with open(fname, "r") as wlFile:
        lines = wlFile.readlines()
        lineNum = len(lines)
        splitSize = lineNum // splitNum
        for i in range(splitNum):
            print(i * splitSize, (i + 1) * splitSize)
            slines = lines[int(i * splitSize): int((i + 1) * splitSize)]
            splitFname = fname + str(i)
            with open(splitFname, "w") as outFile:
                outFile.writelines(slines)
