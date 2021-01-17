from unittest import TestCase

from builders.ddl import DDLBuilder, DDLPrimaryKeyBuildable, DDLForeignKeyBuildable, DDLUniqueBuildable, DDLBuildable, DDLField
from fields.storetype import Text, Integer, Float, TimeStamp


class TestDDLBuilder(TestCase):
    def test_build(self):
        builder = DDLBuilder() \
            .name("A") \
            .field("id", Integer()) \
            .field("a", Text()) \
            .field("b", Integer()) \
            .field("c", Float(), False) \
            .field("d", TimeStamp()) \
            .primary_key("id") \
            .foreign_key("b", "B", "id") \
            .unique("a")

        self.assertRegex(builder.build(),
                         r'CREATE TABLE IF NOT EXISTS "A" \("id" INTEGER, "a" VARCHAR\(255\), "b" INTEGER, "c" NUMERIC NOT NULL, "d" TIMESTAMP, PRIMARY KEY\("id"\), CONSTRAINT fk_[A-Za-z0-9]{6} FOREIGN KEY\("b"\) REFERENCES "B"\("id"\), CONSTRAINT uk_[A-Za-z0-9]{6} UNIQUE \("a"\)\)')

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
        self.assertEqual(builder.build(), 'CREATE TABLE IF NOT EXISTS "B" ("id" INTEGER, "a" VARCHAR(255), "b" INTEGER, "c" NUMERIC, "d" TIMESTAMP, PRIMARY KEY("id"))')

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
        self.assertEqual(builder.build(), 'CREATE TABLE IF NOT EXISTS "A" ("id" INTEGER, "a" VARCHAR(255), "b" INTEGER, "c" NUMERIC, "d" TIMESTAMP, PRIMARY KEY("b"))')

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

        self.assertEqual(builder.build(), 'CREATE TABLE IF NOT EXISTS "A" ("id" INTEGER, "a" VARCHAR(255), "b" INTEGER, "c" NUMERIC, "d" TIMESTAMP)')

    def test_primary_key_constraint(self):
        constraint = DDLPrimaryKeyBuildable("a")

        self.assertEqual(constraint.build(), 'PRIMARY KEY("a")')

    def test_foreign_key_constraint(self):
        constraint = DDLForeignKeyBuildable("a", "B", "b")

        self.assertRegex(constraint.build(), r'CONSTRAINT fk_[A-Za-z0-9]{6} FOREIGN KEY\("a"\) REFERENCES "B"\("b"\)')

    def test_unique_constraint(self):
        constraint = DDLUniqueBuildable("a")
        print(constraint.build())
        self.assertRegex(constraint.build(), r'CONSTRAINT uk_[A-Za-z0-9]{6} UNIQUE \("a"\)')

    def test_base_buildable(self):
        constraint = DDLBuildable()

        self.assertRaises(NotImplementedError, constraint.build)

    def test_ddl_filed(self):
        tests = [
            (DDLField("a", Integer(), True), '"a" INTEGER'),
            (DDLField("a", Integer(), False), '"a" INTEGER NOT NULL'),
            (DDLField("a", Text(), False), '"a" VARCHAR(255) NOT NULL'),
            (DDLField("a", Text(), True), '"a" VARCHAR(255)'),
            (DDLField("a", Text(max_length = 10), True), '"a" VARCHAR(10)'),
            (DDLField("a", Float(), True), '"a" NUMERIC'),
            (DDLField("a", Float(), False), '"a" NUMERIC NOT NULL'),
            (DDLField("a", TimeStamp(), True), '"a" TIMESTAMP'),
            (DDLField("a", TimeStamp(), False), '"a" TIMESTAMP NOT NULL'),
            (DDLField("a", TimeStamp(with_zone=True), True), '"a" TIMESTAMP WITH TIME ZONE'),
        ]

        for (f, r) in tests:
            self.assertEqual(f.build(), r)