"""
DAG Unit Tests
"""

import unittest
from airflow.models import DagBag

class TestDags(unittest.TestCase):
    """DAG Test Case"""

    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_loaded(self):
        """Assert DAG bag load correctly"""
        print(len(self.dagbag.dags))

        for dag in self.dagbag.dags:
            self.assertIsNotNone(dag, "DAG is empty: {}".format(dag))
            print(dag)

        self.assertEqual(
                len(self.dagbag.import_errors), 0,
                "DAG Errors: {}".format(self.dagbag.import_errors)
            )

if __name__ == '__main__':
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestDags)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
