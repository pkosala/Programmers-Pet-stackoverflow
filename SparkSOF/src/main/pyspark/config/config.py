''' IMPORTANT: RENAME THIS FILE TO CONFIG.PY TO RUN '''

import os

# Program settings
SRC_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DEBUG = True


# AWS settings
S3_BUCKET_BATCH_RAW = "insightstackoverflowsample"
S3_DATA_FOLDER = "/FinalData/Questions/"


JDBC_HOST = "ec2-x-yy-zz-ww.compute-1.amazonaws.com"
JDBC_PORT = "80"
JDBC_DB_NAME = "insight"
JDBC_USER = ""
JDBC_PASSWORD = ""
JDBC_DRIVER = "org.postgresql.Driver"

SPARK_MASTER_URL = ""
SPARK_APP_NAME = ""
LSH_num_bands = 100
LSH_band_width = 10
LSH_num_buckets = 10000