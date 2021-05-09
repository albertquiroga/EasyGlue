from test.writer import WriterTest

from test.utils import list_all_objects


def check_if_compressed(bucket: str, prefix: str) -> bool:
    objects = list_all_objects(bucket, prefix)
    first_object = objects[0]
    return first_object.endswith(".gz") or first_object.endswith(".bz2")


class TestEasyDynamicFrameWriter(WriterTest):

    def test_format_option(self):
        output_path = "s3://bertolb/test/easyglue/outputs/format_option/"
        self.dataset.write().format_option("writeHeader", False).csv(output_path)
        data = self.glue.create_dynamic_frame. \
            from_options(connection_type="s3",
                         connection_options={"paths": [output_path]},
                         format="csv",
                         format_options={"withHeader": False})
        self.assertEqual(1000, data.count())

    def test_format_options(self):
        output_path = "s3://bertolb/test/easyglue/outputs/format_options/"
        self.dataset.write().format_options({"writeHeader": False}).csv(output_path)
        data = self.glue.create_dynamic_frame. \
            from_options(connection_type="s3",
                         connection_options={"paths": [output_path]},
                         format="csv",
                         format_options={"withHeader": False})
        self.assertEqual(1000, data.count())

    def test_connection_option(self):
        output_path = "s3://bertolb/test/easyglue/outputs/connection_option/"
        self.dataset.write().connection_option("compression", "gzip").json(output_path)
        compressed = check_if_compressed("bertolb", "test/easyglue/outputs/connection_option/")
        self.assertEqual(compressed, True)

    def test_connection_options(self):
        output_path = "s3://bertolb/test/easyglue/outputs/connection_options/"
        self.dataset.write().connection_options({"compression": "gzip"}).json(output_path)
        compressed = check_if_compressed("bertolb", "test/easyglue/outputs/connection_options/")
        self.assertEqual(compressed, True)