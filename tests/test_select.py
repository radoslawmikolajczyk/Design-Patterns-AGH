from unittest import TestCase
from builders.select import SelectBuilder
from fields.storetype import Integer
class TestSelectBuilder(TestCase):

    def test_build(self):
        builder = SelectBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .join("C", "id", "B", "id") \
            .join("D", "id", "C", "id") \
            .add("B", "a") \
            .add("B", "b") \
            .add("C", "c") \
            .add("D", "d")

        self.assertEqual(builder.build(), ('SELECT "B"."a", "B"."b", "C"."c", "D"."d" FROM "B" JOIN "C" ON ("C"."id" = "B"."id") JOIN "D" ON ("D"."id" = "C"."id") WHERE "B"."id" = 1', [('B','a'), ('B', 'b'), ('C', 'c'), ('D', 'd')]))

    def test_build_no_join(self):
        builder = SelectBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .add("B", "a") \
            .add("B", "b")

        self.assertEqual(builder.build(), ('SELECT "B"."a", "B"."b" FROM "B"  WHERE "B"."id" = 1', [('B', 'a'), ('B', 'b')]))

    def test_override_from(self):
        builder = SelectBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .table("D") \
            .join("C", "id", "D", "id") \
            .add("D", "a") \
            .add("D", "b") \
            .add("C", "c")

        self.assertEqual(builder.build(), ('SELECT "D"."a", "D"."b", "C"."c" FROM "D" JOIN "C" ON ("C"."id" = "D"."id") WHERE "D"."id" = 1', [('D', 'a'), ('D', 'b'), ('C', 'c')]))

    def test_override_where(self):
        builder = SelectBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .join("C", "id", "B", "id") \
            .add("B", "a") \
            .add("B", "b") \
            .add("C", "c") \
            .where("id", Integer(), 2)

        self.assertEqual(builder.build(), ('SELECT "B"."a", "B"."b", "C"."c" FROM "B" JOIN "C" ON ("C"."id" = "B"."id") WHERE "B"."id" = 2', [('B', 'a'), ('B', 'b'), ('C', 'c')]))

    def test_is_empty(self):
        builder = SelectBuilder()

        self.assertTrue(builder.is_empty())

        builder.add("B", "a")

        self.assertFalse(builder.is_empty())

        builder.add("B", "b") \
            .add("C", "c")

        self.assertFalse(builder.is_empty())

    def test_empty(self):
        builder = SelectBuilder() \
            .table("B") \
            .where("id", Integer(), 1) \
            .join("C", "id", "B", "id")

        self.assertRaises(AssertionError, builder.build)

    def test_no_where(self):
        builder = SelectBuilder() \
            .table("B") \
            .join("C", "id", "B", "id") \
            .add("B", "a") \
            .add("B", "b") \
            .add("C", "c")

        self.assertRaises(AssertionError, builder.build)

    def test_no_table(self):
        builder = SelectBuilder() \
            .where("id", Integer(), 1) \
            .join("C", "id", "B", "id") \
            .add("B", "a") \
            .add("B", "b") \
            .add("C", "c")

        self.assertRaises(AssertionError, builder.build)