# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
