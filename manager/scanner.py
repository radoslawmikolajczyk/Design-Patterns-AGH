from collections import defaultdict


class Scanner:

    def __init__(self):
        self.__skipped_pattern = '__'
        self.__const_table_name = '_table_name'

    def __all_subclasses(self, cls):
        all_subclasses = []

        for subclass in cls.__subclasses__():
            all_subclasses.append(subclass)
            all_subclasses.extend(self.__all_subclasses(subclass))

        return all_subclasses

    def get_all_instances(self, cls):
        cls = self.__all_subclasses(cls)
        tables = dict()
        class_names = dict()
        class_tables = dict()
        for subclass in cls:

            values = subclass.__dict__
            dictionary = defaultdict(list)
            for a, b in values.items():
                if not (a.startswith(self.__skipped_pattern) and a.endswith(self.__skipped_pattern)):
                    dictionary[a].append(b)
            dictionary = dict(dictionary)
            new_dict = {}
            t_name = subclass.__name__.lower()
            for key, value in dictionary.items():
                if not key == self.__const_table_name:
                    new_dict[key] = value[0]
                else:
                    t_name = dictionary.get(self.__const_table_name)[0]

            tables[t_name] = new_dict
            class_names[subclass.__name__] = subclass
            class_tables[subclass] = t_name
        return tables, class_names, class_tables
