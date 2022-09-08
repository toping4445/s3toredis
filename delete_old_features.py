from rediscluster import RedisCluster
import logging
import time
import os
from datetime import datetime,timedelta
from multiprocessing import Pool
import multiprocessing as mp

logging.basicConfig(level=logging.INFO)

STD_DATE = os.environ.get("STD_DATE", "2022-07-01")
TTL = os.environ.get("TTL", 0)
MIN_KEY_VALUE = os.environ.get("MIN_KEY_VALUE", 1)
MAX_KEY_VALUE = os.environ.get("MAX_KEY_VALUE", 2500000)
TABLE_NAME = os.environ.get("TABLE_NAME", "rms_features_feast")
REDIS_HOST = os.environ.get("REDIS_HOST","feast-datahub.0437il.clustercfg.apn2.cache.amazonaws.com")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "rms")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")
TESTING = os.environ.get("TESTING", 0)
PROCESS_NUM = os.environ.get("PROCESS_NUM", 4)


rc = RedisCluster(startup_nodes=[{"host": REDIS_HOST,"port": REDIS_PORT}], decode_responses=False,skip_full_coverage_check=True)
org_date_format = '%Y-%m-%d'
converted_date_format = '%Y%m%d'
expiration_date = (datetime.strptime(STD_DATE, org_date_format) - timedelta(days=TTL)).strftime(converted_date_format)

def func(start_key,end_key):
    c_proc = mp.current_process()
    print(f"Running on Process {c_proc.name} PID {c_proc.pid} ({start_key},{end_key-1})")
    child_pipe = rc.pipeline()

    for current_key in range(start_key,end_key):
        child_pipe.zremrangebyscore(f"{MODEL_NAME}:{MODEL_VERSION}:{str(current_key)}", expiration_date, expiration_date)

    result = child_pipe.execute()
    deleted_cnt = sum(result)
    print(f"{c_proc.name}-{c_proc.pid} deleted_cnt = {deleted_cnt}")
    return deleted_cnt

if __name__ == '__main__':

    print(expiration_date)
    total_success_cnt = 0

    start = time.time()

    object_list = []

    for i in range(MIN_KEY_VALUE, MAX_KEY_VALUE, 100000):
        object_list.append(tuple((i,i+100000 if i+100000 < MAX_KEY_VALUE else MAX_KEY_VALUE)))

    pool = Pool(PROCESS_NUM)
    result = pool.starmap(func, object_list)
    total_deleted_cnt = sum(result)
    elapsed_time = time.time() - start
    print(f"total elapsed time = {elapsed_time:.5f} sec")
    print(f"total deleted cnt = {total_deleted_cnt}")

    pool.close()
    pool.join()

# 한방에 - 100sec
# 10만건씩 멀티프로세싱 - 30sec
