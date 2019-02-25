''' IMPORTANT: RENAME THIS FILE TO CONFIG.PY TO RUN '''

import os

# Program settings
SRC_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DEBUG = True


# AWS settings
S3_BUCKET_BATCH_RAW = "insightstackoverflowsample"
S3_DATA_FOLDER = "/FinalData/Questions/"


JDBC_HOST = "ec2-3-94-26-85.compute-1.amazonaws.com"
JDBC_PORT = "80"
JDBC_DB_NAME = "insight"
JDBC_USER = "pooja"
JDBC_PASSWORD = "123"
JDBC_DRIVER = "org.postgresql.Driver"

SPARK_MASTER_URL = ""
SPARK_APP_NAME = ""