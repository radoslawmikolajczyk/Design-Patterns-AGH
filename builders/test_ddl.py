from unittest import TestCase

from builders.ddl import DDLBuilder, DDLPrimaryKeyBuildable, DDLForeignKeyBuildable, DDLUniqueBuildable, DDLBuildable
from fields.storetype import Text, Integer, Float, TimeStamp


class TestDDLBuilder(TestCase):
    def test_build(self):
        builder = DDLBuilder() \
            .name("A") \
            .field("id", Integer()) \
            .field("a", Text()) \
            .field("b", Integer()) \
            .field("c", Float()) \
            .field("d", TimeStamp()) \
            .primary_key("id") \
            .foreign_key("b", "B", "id") \
            .unique("a")
        print(builder.build())
        self.assertRegex(builder.build(),
                         r'CREATE TABLE "A" \("id" INTEGER, "a" VARCHAR\(255\), "b" INTEGER, "c" NUMERIC, "d" TIMESTAMP, PRIMARY KEY\("id"\), CONSTRAINT fk_[A-Za-z0-9]{5} FOREIGN KEY\("b"\) REFERENCES "B"\("id"\), CONSTRAINT uk_[A-Za-z0-9]{5} UNIQUE \("a"\)\)')

    def test_override_name(self):
        builder = DDLBuilder() \
            .name("A") \
            .field("id", Integer()) \
            .field("a", Text()) \
            .field("b", Integer()) \
            .field("c", Float()) \
            .field("d", TimeStamp()) \
            .primary_key("id") \
            .name("B")
        self.assertEqual(builder.build(), 'CREATE TABLE "B" ("id" INTEGER, "a" VARCHAR(255), "b" INTEGER, "c" NUMERIC, "d" TIMESTAMP, PRIMARY KEY("id"))')

    def test_override_primary_key(self):
        builder = DDLBuilder() \
            .name("A") \
            .primary_key("id") \
            .field("id", Integer()) \
            .field("a", Text()) \
            .field("b", Integer()) \
            .field("c", Float()) \
            .field("d", TimeStamp()) \
            .primary_key("b")
        self.assertEqual(builder.build(), 'CREATE TABLE "A" ("id" INTEGER, "a" VARCHAR(255), "b" INTEGER, "c" NUMERIC, "d" TIMESTAMP, PRIMARY KEY("b"))')

    def test_is_empty(self):
        builder = DDLBuilder()

        self.assertTrue(builder.is_empty())

        builder.field("a", Integer())

        self.assertFalse(builder.is_empty())

        builder.field("b", Integer()) \
            .field("c", Float())

        self.assertFalse(builder.is_empty())


    def test_no_name(self):
        builder = DDLBuilder() \
            .field("id", Integer()) \
            .field("a", Text()) \
            .field("b", Integer()) \
            .field("c", Float()) \
            .field("d", TimeStamp()) \
            .primary_key("id")
        self.assertRaises(AssertionError, builder.build)

    def test_no_fields(self):
        builder = DDLBuilder() \
            .name("A") \
            .primary_key("id")
        self.assertRaises(AssertionError, builder.build)

    def test_no_primary_key(self):
        builder = DDLBuilder() \
            .name("A") \
            .field("id", Integer()) \
            .field("a", Text()) \
            .field("b", Integer()) \
            .field("c", Float()) \
            .field("d", TimeStamp())
        self.assertRaises(AssertionError, builder.build)

    def test_primary_key_constraint(self):
        constraint = DDLPrimaryKeyBuildable("a")

        self.assertEqual(constraint.build(), 'PRIMARY KEY("a")')

    def test_foreign_key_constraint(self):
        constraint = DDLForeignKeyBuildable("a", "B", "b")

        self.assertRegex(constraint.build(), r'CONSTRAINT fk_[A-Za-z0-9]{5} FOREIGN KEY\("a"\) REFERENCES "B"\("b"\)')

    def test_unique_constraint(self):
        constraint = DDLUniqueBuildable("a")

        self.assertRegex(constraint.build(), r'CONSTRAINT uk_[A-Za-z0-9]{5} UNIQUE \("a"\)')

    def test_base_constraint(self):
        constraint = DDLBuildable()

        self.assertRaises(NotImplementedError, constraint.build)