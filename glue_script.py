import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime
import boto3
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

# S3からpartファイル名を探す
bucket = "akimoto-serverless-etl"
prefix = f"tmp/users_export/{run_id}"

s3_output_path = f"s3://{bucket}/{prefix}"
rds_iam_role = "arn:aws:iam::450347635113:role/s3-access-for-aurora-role"
# Script generated for node PostgreSQL
df = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"", 
        "withHeader": True, 
        "separator": ","
    }, 
    connection_type="s3", 
    format="csv", 
    connection_options={
        "paths": [f"s3://{bucket}/users/"], 
        "recurse": True
    }, 
    transformation_ctx="read")

empty_df = DynamicFrame.fromDF(spark.createDataFrame([], df.toDF().schema), glueContext, "EmptyFrame")

df = ApplyMapping.apply(
    frame=df, 
    mappings=[("username", "string", "username", "string"), 
              ("email", "string", "email", "string"), 
              ("first_name", "string", "first_name", "string"), 
              ("last_name", "string", "last_name", "string"), 
              ("birth_date", "string", "birth_date", "date")], 
    transformation_ctx="ChangeSchema")

df =  DynamicFrame.fromDF(
        df.toDF().dropDuplicates(["email"]), 
        glueContext, 
        "DropDuplicates"
    )

# Script generated for node Select Fields
#SelectFields_node1754063701594 = SelectFields.apply(
#    frame=AmazonS3_node1754126501538,
#    paths=["id", "username"],
#    transformation_ctx="SelectFields_node1754063701594"
#)
df = df.coalesce(1)

glueContext.write_dynamic_frame.from_options(
    frame=df,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": s3_output_path,
        "partitionKeys": []
    },
    transformation_ctx="WriteToS3"
)

print("S3出力")
print(df.toDF().count())

s3 = boto3.client("s3")
resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
print("S3結果")
print(resp)
filename = resp["Contents"][0]["Key"]

# conn_options = glueContext.extract_jdbc_conf("Aurora connection")

# # AuroraにCOPY SQLを実行（psycopg2）
# conn = psycopg2.connect(
#     host=conn_options["fullUrl"],
#     dbname="gluedb",
#     user=conn_options["user"],
#     password=conn_options["password"]
#     )
    
# cur = conn.cursor()

# sql = f"""
# SELECT aws_s3.table_import_from_s3(
#             'users',
#             'username,email,first_name,last_name,birth_date',
#             '(format csv, header)',
#               aws_commons.create_s3_uri('{bucket}', '{filename}','ap-northeast-1')
#             );
# """

# cur.execute(sql)
# conn.commit()
# cur.close()
# conn.close()

# df = df.toDF()
# df.write \
#     .format("jdbc") \
#     .option("url", conn_options["fullUrl"]) \
#     .option("dbtable", "users") \
#     .option("user", conn_options["user"]) \
#     .option("password", conn_options["password"]) \
#     .option("batchsize", "100000") \
#     .mode("append") \
#     .save()
glueContext.write_dynamic_frame.from_options(
    frame=empty_df,
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "Aurora connection",
        "dbtable": "users",  # 書き込み対象のダミー
        "preactions": f"""
            SELECT aws_s3.table_import_from_s3(
            'users',
            'username,email,first_name,last_name,birth_date',
            '(format csv, header)',
               aws_commons.create_s3_uri('{bucket}', '{filename}','ap-northeast-1')
            );
        """,
        "postactions": ""  # ログ記録等入れても良い
    },
    transformation_ctx="ExecuteCopy"
)

job.commit()
