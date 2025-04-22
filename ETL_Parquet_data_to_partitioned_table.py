import awswrangler as wr
import boto3

s3=boto3.client("s3"); P="youtube/raw_statistics/"; D="dataengineeing-project-0-cleansed"
v={}; [v.setdefault(k:=p.split("=")[0], set()).add(p.split("=")[1]) for o in s3.list_objects_v2(Bucket=D, Prefix=P)["Contents"] for p in o["Key"].replace(P,"").split("/") if "=" in p]
df=wr.s3.read_parquet('s3://dataengineeing-project-0-cleansed/youtube/raw_statistics/')

column_types=wr.catalog.extract_athena_types(df)[0]
partition_keys=["region"]
column_types = {k: v for k, v in column_types.items() if k not in partition_keys}

wr.catalog.create_parquet_table(
    database='db_youtube_cleaned', 
    table='raw_statistics', 
    path='s3://dataengineeing-project-0-cleansed/youtube/raw_statistics/', 
    columns_types=column_types,
    partitions_types= {'region': 'string'}    
    )
    



wr.catalog.add_parquet_partitions(
    database="db_youtube_cleaned",
    table="raw_statistics", 
    partitions_values= { f"s3://{D}/{P}region={val}/": [val] for val in v["region"]}
)

