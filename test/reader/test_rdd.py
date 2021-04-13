import unittest

import easyglue

from pyspark.context import SparkContext
from pyspark.sql.types import *
from awsglue.context import GlueContext


class TestRDDRead(unittest.TestCase):

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)

    def test_rdd(self):
        data = [["Mark,", 1], ["Anne", 2], ["Carlos", 3], ["Ghada", 4], ["Mikhail", 5]]
        rdd = self.sc.parallelize(data)

        schemaString = "name id"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)

        dyf = self.glue.read().rdd(rdd, "test_rdd", schema)
        self.assertEqual(5, dyf.count())


if __name__ == '__main__':
    unittest.main()
