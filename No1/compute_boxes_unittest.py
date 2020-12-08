import unittest
from compute_boxes import CombineBoxes


class TestCombineBoxes(unittest.TestCase):
    def test_compute_boxes(self):
        cb = CombineBoxes()
        self.assertEqual(cb.compute_boxes(20, 25), 5, "Total boxes should be 5 with 4 cake and 5 apple each")
        self.assertEqual(cb.compute_boxes(40, 50), 10, "Total boxes should be 10 with 4 cake and 5 apple each")
        self.assertEqual(cb.compute_boxes(50, 80), 10, "Total boxes should be 10 with 5 cake and 8 apple each")


if __name__ == "__main__":
    unittest.main()