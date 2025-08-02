import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

s3_output_path = f"s3://akimoto-serverless-etl/tmp/users_export/{run_id}"
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
        "paths": ["s3://akimoto-serverless-etl/users/"], 
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
               aws_commons.create_s3_uri('akimoto-serverless-etl', 'run-1754136980092-part-r-00000.csv','ap-northeast-1')
            );
        """,
        "postactions": ""  # ログ記録等入れても良い
    },
    transformation_ctx="ExecuteCopy"
)

job.commit()
