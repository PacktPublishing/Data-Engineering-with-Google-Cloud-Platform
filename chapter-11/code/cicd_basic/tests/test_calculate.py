import unittest
import calculate

class TestSum(unittest.TestCase):

    def testValue_sum(self):
        self.assertEqual(calculate.sum_two_values(1,2), 3, "Should be equal to 3")

if __name__ == '__main__':
    unittest.main()