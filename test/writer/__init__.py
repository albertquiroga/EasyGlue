from awsglue.context import GlueContext

from test import EasyGlueTest
from test.test_utils import delete_all_s3_objects
from test.test_utils.resources import create_sample_table, delete_sample_table


class WriterTest(EasyGlueTest):
    glue: GlueContext

    @classmethod
    def setUpClass(cls) -> None:
        super(WriterTest, cls).setUpClass()
        cls.dataset = cls.glue.create_dynamic_frame. \
            from_options(connection_type="s3",
                         connection_options={"paths": ["s3://bertolb/sampledata/mockaroo/json/"]},
                         format="json",
                         transformation_ctx="datasource0")
        create_sample_table()

    @classmethod
    def tearDownClass(cls) -> None:
        delete_all_s3_objects("bertolb", "test/easyglue/outputs/")
        delete_sample_table()
