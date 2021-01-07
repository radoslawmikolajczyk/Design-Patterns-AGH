from unittest import TestCase
import builders.stringutils as strutl
import re


class TestStringUtils(TestCase):

    def test_quote_database_name_unsafe(self):
        self.assertEqual(strutl.quote_database_object_name_unsafe('ABCD"GHD'), '"ABCD""GHD"')

    def test_quote_known_string_unsafe(self):
        self.assertEqual(strutl.quote_known_string_unsafe("ABCD'GHD"), "'ABCD''GHD'",)

    def test_quote_string(self):
        self.assertRegex(strutl.quote_string("ABCD"), r"\$[A-Za-z0-9]{5}\$ABCD\$[A-Za-z0-9]{5}\$")

    def test_generate_escape_seq(self):
        for _ in range(100):
            self.assertRegex(strutl.generate_escape_seq(), r"^[A-Za-z][A-Za-z0-9]{5}$")

