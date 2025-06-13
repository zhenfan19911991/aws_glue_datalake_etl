import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749795511638 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1749795511638")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749795489876 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1749795489876")

# Script generated for node SQL Query
SqlQuery7040 = '''
select a.*, b.* from step_trainer_trusted a 
left join accelerometer_trsuted b 
on a.sensorreadingtime = b.timestamp

'''
SQLQuery_node1749795533550 = sparkSqlQuery(glueContext, query = SqlQuery7040, mapping = {"accelerometer_trsuted":AWSGlueDataCatalog_node1749795511638, "step_trainer_trusted":AWSGlueDataCatalog_node1749795489876}, transformation_ctx = "SQLQuery_node1749795533550")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749795533550, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749795487911", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749795544088 = glueContext.getSink(path="s3://zhen-project/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749795544088")
AmazonS3_node1749795544088.setCatalogInfo(catalogDatabase="zhen_project",catalogTableName="machine_learning_curated")
AmazonS3_node1749795544088.setFormat("json")
AmazonS3_node1749795544088.writeFrame(SQLQuery_node1749795533550)
job.commit()