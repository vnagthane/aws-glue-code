# aws-glue-code

import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "cred_txn_s3_db", table_name = "s3_cred_txn_csv", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("accnum", "string", "accnum", "string"), ("amount", "double", "amount", "decimal(10,8)"), ("date", "string", "date", "string"), ("category", "string", "category", "string")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["accnum", "amount"], transformation_ctx = "selectfields2")

agg_df = selectfields2.toDF().groupBy("accnum").agg(sum(col("amount")).alias("amount"))
agg_dyf = DynamicFrame.fromDF(agg_df, glueContext, "agg_dyf")

resolvechoice3 = ResolveChoice.apply(frame = agg_dyf, choice = "MATCH_CATALOG", database = "cred_txn_agg_rs_db", table_name = "rs_dev_public_cred_txn_agg", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "cred_txn_agg_rs_db", table_name = "rs_dev_public_cred_txn_agg", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")

job.commit()
