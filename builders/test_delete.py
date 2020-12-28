import datetime
from unittest import TestCase

from builders.delete import DeleteBuilder
from fields.storetype import Integer, TimeStamp


class TestDeleteBuilder(TestCase):

    def test_build(self):
        builder = DeleteBuilder() \
            .table("B") \
            .where("id", Integer(), 1)

        self.assertEqual(builder.build(), 'DELETE FROM "B" WHERE "id" = 1')

    def test_override_table(self):
        builder = DeleteBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .table("C")

        self.assertEqual(builder.build(), 'DELETE FROM "C" WHERE "id" = 1')

    def test_override_where(self):
        builder = DeleteBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .where("tid", TimeStamp(), datetime.datetime(2004, 10, 19, 10, 23, 54, tzinfo=datetime.timezone.utc))

        self.assertEqual(builder.build(), 'DELETE FROM "B" WHERE "tid" = TIMESTAMP \'2004-10-19 10:23:54\'')

    def test_no_table(self):
        builder = DeleteBuilder() \
            .where("id", Integer(), 1)

        self.assertRaises(AssertionError, builder.build)

    def test_no_where(self):
        builder = DeleteBuilder() \
            .table("B")

        self.assertRaises(AssertionError, builder.build)
