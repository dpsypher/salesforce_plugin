import csv
from tempfile import NamedTemporaryFile
import os
import json

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
import boto3
from smart_open import open as smart_open

from airflow.providers.salesforce.hooks.salesforce import SalesforceHook


REDSHIFT_UNLOAD_SQL = """
    -- Column List is important to include if columns are no longer what they were
    UNLOAD ('{SQL_STATEMENT}')
    TO '{S3_PATH}'
    IAM_ROLE 'arn:aws:iam::728973467215:role/RedshiftCopyUnload'
    DELIMITER '{DELIMITER}'
    -- GZIP
    ALLOWOVERWRITE
    MANIFEST
    HEADER
    -- ADDQUOTES  -- psycopg2.errors.FeatureNotSupported: ADDQUOTES is not supported for UNLOAD to CSV
    MAXFILESIZE 128 MB
    CSV;
    COMMIT;
"""

SQL_STATEMENT = """select {COLUMNS} from oneplatform_st.nbn_remediation__c nrc"""
S3_PATH = "s3://{BUCKET_NAME}/{S3_KEY}"
COLUMNS=["name","action_type__c","brand__c","comms_trigger_c","customer_action__c","nbn_location_id__c","nbn_pri__c"]
COLUMN_LIST='("' + '","'.join(COLUMNS) + '")' if COLUMNS else ""
        

class S3OperatorToSalesforceBulkQuery(BaseOperator):
    template_fields = ('s3_key',)

    def __init__(
        self,
        sf_conn_id,
        object_type,
        object_key,
        s3_conn_id,
        s3_bucket,
        s3_key,
        *args,
        **kwargs
    ):

        super().__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.object = object_type[0].upper() + object_type[1:].lower()
        self.object_key = object_key  # for example 'Id'

    def execute(self, context):
        self.log.info("S3 Key: %s", self.s3_key)

        s3 = S3Hook(self.s3_conn_id)
        manifest = json.loads(s3.read_key(f"{self.s3_key}manifest", bucket_name=self.s3_bucket))

        session_arg = {}
        session_arg.update(
            {
                "aws_access_key_id": s3.get_credentials().access_key,
                "aws_secret_access_key": s3.get_credentials().secret_key,
            }
        )
        session = boto3.Session(**session_arg)

        for _file in manifest['entries']:
            data = []
            with smart_open(
                _file['url'],
                "r",
                transport_params={"session": session},
            ) as file_in:
                reader = csv.DictReader(file_in)
                for row in reader:
                    data.append(row)

            if data:
                sf_conn = SalesforceHook(self.sf_conn_id).get_conn()
                sf_conn.bulk.__getattr__(self.object).upsert(data, self.object_key, batch_size=10000, use_serial=True)
