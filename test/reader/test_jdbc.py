import unittest

import easyglue
from test.test_utils import get_connection_options
from test.reader import ReaderTest

SECRET_NAME = "mysql"
TABLE_NAME = "easyglue"


class TestJDBC(ReaderTest):

    @classmethod
    def setUpClass(cls) -> None:
        super(TestJDBC, cls).setUpClass()
        cls.jdbc_options = get_connection_options(SECRET_NAME, TABLE_NAME)

    def test_jdbc(self):
        data = self.glue.read().jdbc(self.jdbc_options)
        self.assertEqual(1000, data.count())


if __name__ == '__main__':
    unittest.main()
