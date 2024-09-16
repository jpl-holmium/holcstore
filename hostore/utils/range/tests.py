from django.test import SimpleTestCase

from hostore.utils.range.range import Range


class RangeTest(SimpleTestCase):

    def test_difference(self):
        # Cas 1
        interval_1 = Range(0, 5)
        interval_2 = Range(1, 4)
        result = interval_1.difference_missing(interval_2, min_delta=0)
        self.assertEqual(result, [])

        # Cas 2
        interval_1 = Range(0, 5)
        interval_2 = Range(-1, 6)
        result = interval_1.difference_missing(interval_2, min_delta=0)
        self.assertEqual(result, [Range(-1, 0), Range(5, 6)])

        # Cas 3
        interval_1 = Range(0, 5)
        interval_2 = Range(-1, 3)
        result = interval_1.difference_missing(interval_2, min_delta=0)
        self.assertEqual(result, [Range(-1, 0)])

        # Cas 4
        interval_1 = Range(0, 5)
        interval_2 = Range(3, 6)
        result = interval_1.difference_missing(interval_2, min_delta=0)
        self.assertEqual(result, [Range(5, 6)])

        # Cas 5
        interval_1 = Range(0, 5)
        interval_2 = Range(8, 9)
        result = interval_1.difference_missing(interval_2, min_delta=0)
        self.assertEqual(result, [Range(5, 9)])

        # Cas 6
        interval_1 = Range(0, 5)
        interval_2 = Range(-2, -1)
        result = interval_1.difference_missing(interval_2, min_delta=0)
        self.assertEqual(result, [Range(-2, 0)])

        # Cas 7
        interval_1 = Range(0, 5)
        interval_2 = Range(-0.5, 6)
        result = interval_1.difference_missing(interval_2, min_delta=1)
        self.assertEqual(result, [Range(5, 6)])

        # Cas 8
        interval_1 = Range(0, 5)
        interval_2 = Range(0, 6)
        result = interval_1.difference_missing(interval_2, min_delta=1)
        self.assertEqual(result, [Range(5, 6)])

    def test_intersection(self):
        # Cas 1
        interval_1 = Range(0, 5)
        interval_2 = Range(1, 4)
        result = interval_1.intersection(interval_2, min_delta=0)
        self.assertEqual(result, Range(1, 4))

        # Cas 2
        interval_1 = Range(0, 5)
        interval_2 = Range(4, 5)
        result = interval_1.intersection(interval_2, min_delta=2)
        self.assertEqual(result, None)

        # Cas 3
        interval_1 = Range(-1, 3)
        interval_2 = Range(4, 6)
        result = interval_1.intersection(interval_2, min_delta=0)
        self.assertEqual(result, None)

    def test_combine(self):
        # Cas 1
        combined = Range.combine([Range(0, 5), Range(-3, -2), Range(6, 7)])
        self.assertEqual(combined, [Range(-3, -2), Range(0, 5) , Range(6, 7)])

        # Cas 2
        combined = Range.combine([Range(-3, 5), Range(-4, -2), Range(6, 7)])
        self.assertEqual(combined, [Range(-4, 5), Range(6, 7)])

        # Cas 3
        combined = Range.combine([Range(-3, 6), Range(-4, -2), Range(6, 7)])
        self.assertEqual(combined, [Range(-4, 7)])

        # Cas 3
        combined = Range.combine([])
        self.assertEqual(combined, [])

    def test_hash(self):
        r = Range(0, 5)
        self.assertTrue(isinstance(r.__hash__(), int))
