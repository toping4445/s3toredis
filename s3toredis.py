
import pyarrow.parquet as pq
import s3fs
from rediscluster import RedisCluster
import logging
import json
import sys
import time

logging.basicConfig(level=logging.INFO)

s3 = s3fs.S3FileSystem()
rc = RedisCluster(startup_nodes=[{"host": "url","port": "6379"}], decode_responses=False,skip_full_coverage_check=True)
pipe = rc.pipeline()

df = pq.ParquetDataset('s3://bucket/fileurl', filesystem=s3).read_pandas().to_pandas()

total_size = 0
start = time.time()

for index, row in df.iterrows():
    id = row['id']
    yyyymmdd = str(row['event_timestamp'])[:10].replace("-","")
    row = row.drop(labels=['id', 'label','event_timestamp']).to_dict()
    key = yyyymmdd+":rms"
    json_val = json.dumps(row)
    size = sys.getsizeof(key)+sys.getsizeof(id)+sys.getsizeof(json_val)

    pipe.hset(key, id, json_val)
    total_size += size

    # if total_size > 60000:
    #     pipe.execute()
    #     total_size = 0

pipe.execute()
end = time.time()

print(f"{end - start:.5f} sec")
print(total_size)

print(rc.hget("20220701:rms",id))

#10만건에 30초 260MB...(텍스트)    #하루치 데이터 2.5~3시간  #  79G 흠... 압축이 되려나
#60KB씩 나누면 그게 더 오래걸리네... 80초