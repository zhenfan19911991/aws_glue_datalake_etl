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
AWSGlueDataCatalog_node1749795073585 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="customers_curated", transformation_ctx="AWSGlueDataCatalog_node1749795073585")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749795083865 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1749795083865")

# Script generated for node SQL Query
SqlQuery7168 = '''
select distinct b.*
from customer_curated a 
join sedi_training_landing b 
on a.serialnumber = b.serialnumber
'''
SQLQuery_node1749795102270 = sparkSqlQuery(glueContext, query = SqlQuery7168, mapping = {"sedi_training_landing":AWSGlueDataCatalog_node1749795083865, "customer_curated":AWSGlueDataCatalog_node1749795073585}, transformation_ctx = "SQLQuery_node1749795102270")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749795102270, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749795070953", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749795111834 = glueContext.getSink(path="s3://zhen-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749795111834")
AmazonS3_node1749795111834.setCatalogInfo(catalogDatabase="zhen_project",catalogTableName="step_trainer_trusted")
AmazonS3_node1749795111834.setFormat("json")
AmazonS3_node1749795111834.writeFrame(SQLQuery_node1749795102270)
job.commit()