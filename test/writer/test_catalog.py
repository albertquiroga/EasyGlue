from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

import easyglue
from test.writer import WriterTest
from test.test_utils import delete_all_s3_objects
from test.test_utils.resources import DATABASE_NAME, TABLE_NAME, TABLE_BUCKET, TABLE_PREFIX


class TestS3Read(WriterTest):
    _glue: GlueContext
    dataset: DynamicFrame

    @classmethod
    def tearDown(cls) -> None:
        delete_all_s3_objects(TABLE_BUCKET, TABLE_PREFIX)  # TODO see if this can be replaced by purge_table

    def test_catalog(self):
        self.dataset.write().catalog(database=DATABASE_NAME, table=TABLE_NAME)
        data = self.glue.create_dynamic_frame. \
            from_catalog(database=DATABASE_NAME,
                         table_name=TABLE_NAME)
        self.assertEqual(1000, data.count())

    def test_table(self):
        self.dataset.write().table(f"{DATABASE_NAME}.{TABLE_NAME}")
        data = self.glue.create_dynamic_frame. \
            from_catalog(database=DATABASE_NAME,
                         table_name=TABLE_NAME)
        self.assertEqual(1000, data.count())
