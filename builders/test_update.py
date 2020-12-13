from unittest import TestCase

from builders.update import UpdateBuilder

class TestUpdateBuilder(TestCase):

    def test_build(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", "1") \
            .add("a", "$V12IM$The quick brown fox jumps over the lazy dog$V12IM$") \
            .add("b", "12") \
            .add("c", "1.0") \
            .add("d", "TIMESTAMP '2004-10-19 10:23:54+02'")

        self.assertEqual(builder.build(),
                         'UPDATE "B" SET "a" = $V12IM$The quick brown fox jumps over the lazy dog$V12IM$, "b" = 12, "c" = 1.0, "d" = TIMESTAMP \'2004-10-19 10:23:54+02\' WHERE "id" = 1')

    def test_override_add(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", "1") \
            .add("a", "1") \
            .add("b", "3") \
            .add("a", "4")

        self.assertEqual(builder.build(), 'UPDATE "B" SET "a" = 4, "b" = 3 WHERE "id" = 1')

    def test_override_where(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", "1") \
            .add("a", "1") \
            .add("b", "3") \
            .where("id", "2")

        self.assertEqual(builder.build(), 'UPDATE "B" SET "a" = 1, "b" = 3 WHERE "id" = 2')

    def test_override_table(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", "1") \
            .add("a", "1") \
            .add("b", "3") \
            .table("C")

        self.assertEqual(builder.build(), 'UPDATE "C" SET "a" = 1, "b" = 3 WHERE "id" = 1')

    def test_is_empty(self):
        builder = UpdateBuilder()

        self.assertTrue(builder.is_empty())

        builder.add("a", "1")

        self.assertFalse(builder.is_empty())

        builder.add("b", "3") \
            .add("c", "1.0")

        self.assertFalse(builder.is_empty())

    def test_empty(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", "1")

        self.assertRaises(AssertionError, builder.build)

    def test_no_table(self):
        builder = UpdateBuilder() \
            .add("a", "1") \
            .add("b", "3") \
            .add("c", "1.0")

        self.assertRaises(AssertionError, builder.build)

    def test_no_where(self):
        builder = UpdateBuilder() \
            .table("B") \
            .add("a", "1") \
            .add("b", "3") \
            .add("c", "1.0")

        self.assertRaises(AssertionError, builder.build)