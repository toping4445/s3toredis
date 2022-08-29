import pyarrow.parquet as pq
import s3fs
from rediscluster import RedisCluster
import logging
import json
import sys
import time
import os
import boto3
from datetime import datetime,timedelta
import snappy

logging.basicConfig(level=logging.INFO)

ACCESS_KEY = os.environ.get("ACCESS_KEY", "")
SECRET_KEY = os.environ.get("SECRET_KEY", "")
SESSION_TOKEN = os.environ.get("SESSION_TOKEN", "")
STD_DATE = os.environ.get("STD_DATE", "2022-07-01")
TTL = os.environ.get("TTL", 3)
MIN_KEY_VALUE = os.environ.get("MIN_KEY_VALUE", 1)
MAX_KEY_VALUE = os.environ.get("MAX_KEY_VALUE", 2500000)
BUCKET = os.environ.get("BUCKET", "sagemaker-yelo-test")
TABLE_NAME = os.environ.get("TABLE_NAME", "rms_features_feast")
REDIS_HOST = os.environ.get("REDIS_HOST", "")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "rms")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")



s3_fs = s3fs.S3FileSystem()
rc = RedisCluster(startup_nodes=[{"host": REDIS_HOST,"port": REDIS_PORT}], decode_responses=False,skip_full_coverage_check=True)

# startup_nodes = [{"host": "", "port": "6379"},
#                  {"host": "", "port": "6379"},
#                  {"host": "", "port": "6379"}
#                 ]
# # Note: See note on Python 3 for decode_responses behaviour
# rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=False, skip_full_coverage_check=True)


pipe = rc.pipeline()

session = boto3.Session(
    region_name = 'ap-northeast-2',
    # aws_access_key_id=ACCESS_KEY,
    # aws_secret_access_key=SECRET_KEY,
    # aws_session_token=SESSION_TOKEN
)
s3 = session.resource('s3')
bucket = s3.Bucket(BUCKET)

#TTL 지난 데이터 삭제 로직 추가
# org_date_format = '%Y-%m-%d'
# converted_date_format = '%Y%m%d'
# # expiration_date = (datetime.strptime(STD_DATE, org_date_format) - timedelta(days=3)).strftime(converted_date_format)
# expiration_date = '20220701'
#
# start = time.time()
#
# for current_key in range(MIN_KEY_VALUE,MAX_KEY_VALUE):
#     pipe.zremrangebyscore("rms:v1:"+str(current_key),expiration_date,expiration_date)
#
# result = pipe.execute()
# end = time.time()
# print(f"elapsed time : {end - start:.5f} sec")
# print(f"deleted values cnt : {str(sum(result))}")



prefix = "data/"+TABLE_NAME+"/yyyymmdd="+STD_DATE
total_size = 0
total_success_cnt = 0

start = time.time()



for idx, object in enumerate(bucket.objects.filter(Prefix=prefix)):
    if idx ==1:
        break


    url = "s3://"+BUCKET+"/"+str(object.key)
    df = pq.ParquetDataset(
        url,
        filesystem=s3_fs).read_pandas().to_pandas()

    cur_time = time.time()
    print(f"read df elapsed time : {cur_time - start:.5f} sec")

    # 모델에서 필요한 feature들만 남기기
    # df = df[[feature_list_for_certain_model_ver]]
    df.drop(['label', 'event_timestamp'], axis=1)
    df_json = df.to_json(orient="records")
    print(len(df_json))
    json_list = json.loads(df_json)
    print(len(json_list))
    #
    #
    # for idx2, features in enumerate(json_list):
    #     yyyymmdd = (datetime.strptime(STD_DATE, org_date_format).strftime(converted_date_format))
    #     pay_account_id = str(features["pay_account_id"])
    #     features.pop("pay_account_id", None)
    #     key = ":".join([MODEL_NAME,MODEL_VERSION,pay_account_id])
    #     compressed_feature_json = snappy.compress(json.dumps(features))
    #     pipe.zadd(key, {compressed_feature_json:yyyymmdd})
    #
    #
    # cur_time = time.time()
    # print(f"itr-{idx} - add all features elapsed time : {cur_time - start:.5f} sec")
    # result = pipe.execute()
    # cur_time = time.time()
    # print(f"itr-{idx} - execute done : {cur_time - start:.5f} sec")
    # total_success_cnt += sum(result)

end = time.time()
print(f"total elapsed time : {end - start:.5f} sec")
print("total_success_cnt : "+str(total_success_cnt))


# #모델,account_id별 가장 최근에 적재된 feature 추출
# print(rc.zrange("rms:v1:10691",-1,-1))

#10만건에 30초 260MB...(텍스트)    #하루치 데이터 2.5~3시간  #  79G 흠... 압축이 되려나
#60KB씩 나누면 그게 더 오래걸리네... 80초
# size = sys.getsizeof(key) + sys.getsizeof(feature_json)
# total_size += size
# if total_size > 60000:
#     result = pipe.execute()
#     total_success_cnt += sum(result)
#     total_size = 0