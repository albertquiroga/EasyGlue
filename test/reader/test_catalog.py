import unittest

import easyglue

from pyspark.context import SparkContext
from awsglue.context import GlueContext


class TestCatalogRead(unittest.TestCase):

    glue = GlueContext(SparkContext.getOrCreate())

    def test_table(self):
        data = self.glue.read().table("sampledata.mockaroo_csv")
        self.assertEqual(1000, data.count())

    def test_catalog(self):
        data = self.glue.read().catalog("sampledata", "mockaroo_csv")
        self.assertEqual(1000, data.count())


if __name__ == '__main__':
    unittest.main()
