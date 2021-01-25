from unittest import TestCase
import datetime

from builders.update import UpdateBuilder
from fields.storetype import Text, Integer, Float, TimeStamp

class TestUpdateBuilder(TestCase):

    def test_build(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .add("a", Text(), "The quick brown fox jumps over the lazy dog") \
            .add("b", Integer(), 12) \
            .add("c", Float(), 1.0) \
            .add("d", TimeStamp(), datetime.datetime(2004, 10, 19, 10, 23, 54, tzinfo=datetime.timezone.utc))

        self.assertRegex(builder.build(),
                         r'UPDATE "B" SET "a" = \$[A-Za-z0-9]{5}\$The quick brown fox jumps over the lazy dog\$[A-Za-z0-9]{5}\$, "b" = 12, "c" = 1\.0, "d" = TIMESTAMP \'2004-10-19 10:23:54\' WHERE "id" = 1')

    def test_override_add(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .add("a", Integer(), 4)

        self.assertEqual(builder.build(), 'UPDATE "B" SET "a" = 4, "b" = 3 WHERE "id" = 1')

    def test_override_where(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .where("id", Integer(), 2)

        self.assertEqual(builder.build(), 'UPDATE "B" SET "a" = 1, "b" = 3 WHERE "id" = 2')

    def test_override_table(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .table("C")

        self.assertEqual(builder.build(), 'UPDATE "C" SET "a" = 1, "b" = 3 WHERE "id" = 1')

    def test_is_empty(self):
        builder = UpdateBuilder()

        self.assertTrue(builder.is_empty())

        builder.add("a", Integer(), 1)

        self.assertFalse(builder.is_empty())

        builder.add("b", Integer(), 3) \
            .add("c", Float(), 1.0)

        self.assertFalse(builder.is_empty())

    def test_empty(self):
        builder = UpdateBuilder() \
            .table("B") \
            .where("id", Integer(), 1)

        self.assertRaises(AssertionError, builder.build)

    def test_no_table(self):
        builder = UpdateBuilder() \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .add("a", Float(), 1.0)

        self.assertRaises(AssertionError, builder.build)

    def test_no_where(self):
        builder = UpdateBuilder() \
            .table("B") \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .add("a", Float(), 1.0)

        self.assertRaises(AssertionError, builder.build)