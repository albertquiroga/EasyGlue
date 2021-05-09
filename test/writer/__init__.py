import unittest

import easyglue
from test.utils import delete_all_s3_objects

from pyspark.context import SparkContext
from awsglue.context import GlueContext


class WriterTest(unittest.TestCase):
    glue = GlueContext(SparkContext.getOrCreate())
    dataset = glue.create_dynamic_frame. \
        from_options(connection_type="s3",
                     connection_options={"paths": ["s3://bertolb/sampledata/mockaroo/json/"]},
                     format="json",
                     transformation_ctx="datasource0")

    delete_all_s3_objects("bertolb", "test/easyglue/outputs/")
