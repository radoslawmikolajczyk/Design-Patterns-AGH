from unittest import TestCase
import datetime
from builders.insert import InsertBuilder
from fields.storetype import Text, Integer, Float, TimeStamp

class TestInsertBuilder(TestCase):

    def test_build(self):
        builder = InsertBuilder() \
            .into("B") \
            .add("a", Text(), "The quick brown fox jumps over the lazy dog") \
            .add("b", Integer(), 12) \
            .add("c", Float(), 1.0) \
            .add("d", TimeStamp(), datetime.datetime(2004, 10, 19, 10, 23, 54, tzinfo=datetime.timezone.utc))

        self.assertRegex(builder.build(),
                         r'INSERT INTO "B" \("a", "b", "c", "d"\) VALUES \(\$[A-Za-z0-9]{5}\$The quick brown fox jumps over the lazy dog\$[A-Za-z0-9]{5}\$, 12, 1\.0, TIMESTAMP \'2004-10-19 10:23:54\'\)')

    def test_override_add(self):
        builder = InsertBuilder() \
            .into("B") \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .add("a", Integer(), 4)

        self.assertEqual(builder.build(), 'INSERT INTO "B" ("a", "b") VALUES (4, 3)')

    def test_override_into(self):
        builder = InsertBuilder() \
            .into("B") \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .into("C")

        self.assertEqual(builder.build(), 'INSERT INTO "C" ("a", "b") VALUES (1, 3)')

    def test_is_empty(self):
        builder = InsertBuilder()

        self.assertTrue(builder.is_empty())

        builder.add("a", Integer(), 1)

        self.assertFalse(builder.is_empty())

        builder.add("b", Integer(), 3) \
            .add("c", Float(), 1.0)

        self.assertFalse(builder.is_empty())

    def test_empty(self):
        builder = InsertBuilder() \
            .into("B")

        self.assertRaises(AssertionError, builder.build)

    def test_no_into(self):
        builder = InsertBuilder() \
            .add("a", Integer(), 1) \
            .add("b", Integer(), 3) \
            .add("c", Float(), 1.0)

        self.assertRaises(AssertionError, builder.build)