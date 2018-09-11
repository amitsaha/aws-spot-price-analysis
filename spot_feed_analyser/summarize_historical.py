import gzip
import sys
import os
import struct
from collections import defaultdict

parsed_data = defaultdict(list)

def read(gzip_file_path):
    with gzip.open(gzip_file_path) as f:
        return f.readlines()

def parse(data):
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-data-feeds.html#using-spot-instances-format
    for line in data:
        if line.startswith(b"#"):
            pass
        else:
            fields = line.split(b"\t")
            if b"t2.medium" in fields[1]:
                instance_type = "t2.medium"
            elif b"c5.2xlarge" in fields[1]:
                instance_type = "c5.2xlarge"
            else:
                raise Exception(f"Unrecognized instance type: {fields[1]}")

            if b"0002" in fields[2]:
                instance_os = "WINDOWS"
            else:
                instance_os = "LINUX"
            key = f"{instance_os}-{instance_type}"
            parsed_data[key].append({
                'MyMaxPrice': fields[5],
                'MarketPrice': fields[6],
                'Charge': fields[7],
            })

def summarize():
    categories = parsed_data.keys()
    for c in categories:
        print(c)

        MyMaxPrice = float(parsed_data[c][0]['MyMaxPrice'].split(b' ')[0])

        print(f"MyMaxPrice: {MyMaxPrice}")
        
        charged = [float(data["Charge"].split(b' ')[0]) for data in parsed_data[c]]
        AveragePaid = sum(charged)/len(charged)
        PercentagePaid = ((AveragePaid - MyMaxPrice)/MyMaxPrice)*100
        print(f"Average paid: {AveragePaid} ({PercentagePaid}%)")

        print("---")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit('Usage: read_file <dir>')
    dir_name = sys.argv[1]
    for f in os.listdir(dir_name):
        parse(read(os.path.join(dir_name, f)))
    summarize()
