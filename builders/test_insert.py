from unittest import TestCase

from builders.insert import InsertBuilder


class TestInsertBuilder(TestCase):

    def test_build(self):
        builder = InsertBuilder() \
            .into("B") \
            .add("a", "$V12IM$The quick brown fox jumps over the lazy dog$V12IM$") \
            .add("b", "12") \
            .add("c", "1.0") \
            .add("d", "TIMESTAMP '2004-10-19 10:23:54+02'")

        self.assertEqual(builder.build(),
                         'INSERT INTO "B" ("a", "b", "c", "d") VALUES ($V12IM$The quick brown fox jumps over the lazy dog$V12IM$, 12, 1.0, TIMESTAMP \'2004-10-19 10:23:54+02\')')

    def test_override_add(self):
        builder = InsertBuilder() \
            .into("B") \
            .add("a", "1") \
            .add("b", "3") \
            .add("a", "4")

        self.assertEqual(builder.build(), 'INSERT INTO "B" ("a", "b") VALUES (4, 3)')

    def test_override_into(self):
        builder = InsertBuilder() \
            .into("B") \
            .add("a", "1") \
            .add("b", "3") \
            .into("C")

        self.assertEqual(builder.build(), 'INSERT INTO "C" ("a", "b") VALUES (1, 3)')

    def test_is_empty(self):
        builder = InsertBuilder()

        self.assertTrue(builder.is_empty())

        builder.add("a", "1")

        self.assertFalse(builder.is_empty())

        builder.add("b", "3") \
            .add("c", "1.0")

        self.assertFalse(builder.is_empty())

    def test_empty(self):
        builder = InsertBuilder() \
            .into("B")

        self.assertRaises(AssertionError, builder.build)

    def test_no_into(self):
        builder = InsertBuilder() \
            .add("a", "1") \
            .add("b", "3") \
            .add("c", "1.0")

        self.assertRaises(AssertionError, builder.build)