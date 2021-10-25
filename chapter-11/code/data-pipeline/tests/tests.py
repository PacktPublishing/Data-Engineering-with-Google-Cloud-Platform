"""
DAG Integrity Tests
"""

import unittest
from airflow.models import DagBag


class TestDags(unittest.TestCase):
    """DAG Test Case"""

    LOAD_THRESHOLD_SECONDS = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_dags_syntax(self):
        """Assert DAG bag load correctly"""
        for key in self.dagbag.dags:
            print(key)
        self.assertFalse(
            len(self.dagbag.import_errors),
            f"DAG import errors. Errors: {self.dagbag.import_errors}")


if __name__ == '__main__':
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestDags)
    unittest.TextTestRunner(verbosity=2).run(SUITE)