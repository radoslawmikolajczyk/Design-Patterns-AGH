from entity.entity import Entity
from fields.field import Field, PrimaryKey, Column
from fields.relationship import Relationship, OneToOne, ManyToOne, ManyToMany


def find_name_of_first_fk_column(all_data, class_name, second_table_name):
    first_fk_name = None
    second_fields = all_data[second_table_name].items()
    for s_field_name, s_field_object in second_fields:
        if isinstance(s_field_object, ManyToMany) and s_field_object.other == class_name:
            first_fk_name = get_column_name(s_field_object, s_field_name)

    assert first_fk_name is not None
    return first_fk_name


def find_foreign_key_values_in_m2m(objects, fk_field_name):
    second_fk_values = []
    if not isinstance(objects, ManyToMany):
        for relation_object in objects:
            value = getattr(relation_object, fk_field_name)
            second_fk_values.append(value)
    return second_fk_values


def get_column_name(field, field_name):
    if field.name == Field.default_name:
        return field_name
    else:
        return field.name


def get_table_name(entity: Entity):
    if entity._table_name != "":
        table_name = entity._table_name
    else:
        if isinstance(entity, Entity):
            entity = type(entity)
        table_name = entity.__name__.lower()
    return table_name


def find_primary_key_of_table(all_data, table_name):
    fields = all_data[table_name].items()

    for field_name, field_object in fields:
        if isinstance(field_object, PrimaryKey):
            column_name = get_column_name(field_object, field_name)
            primary_key = [field_name, column_name, field_object.type]
            return primary_key

    # if we don't find the primary key field, the primary key is the first column
    for field_name, field_object in fields:
        if isinstance(field_object, Column):
            column_name = get_column_name(field_object, field_name)
            primary_key = [field_name, column_name, field_object.type]
            return primary_key

    return None


def get_all_inheritances(classes):
    inheritance_dictionary = dict()
    for base_class in classes:
        val = []
        for subclass in classes:
            if issubclass(base_class, subclass) and base_class != subclass:
                val.append(subclass)
        if val:
            inheritance_dictionary[base_class] = val
    return inheritance_dictionary


def get_key_by_value(dictionary, table_name):
    items_list = dictionary.items()
    for item in items_list:
        if item[1] == table_name:
            return item[0]


def check_got_many_relation(table_name, junction_tables):
    for junction_name in junction_tables:
        if table_name in junction_name:
            return True
    return False


def extract_junction_data(table_name: str, junction_tables, junction_queries):
    junction_data = []
    for index, junction_table_name in enumerate(junction_tables):
        if table_name in junction_table_name:
            junction_query = junction_queries[index]
            junction_query = junction_query.split(" ")
            junction_key = None
            other_junction_key = None
            for part_index, part in enumerate(junction_query):
                if 'KEY' in part and table_name in junction_query[part_index + 2]:
                    junction_key = part.split('"')[1]
                elif 'KEY' in part and table_name not in junction_query[part_index + 2]:
                    other_junction_key = part.split('"')[1]
            junction_data.append((junction_table_name, junction_key, other_junction_key))
    return junction_data


def find_many_to_many_other_field(table_name, data):
    names = []
    for key, value in data[table_name].items():
        if isinstance(value, ManyToMany):
            names.append(value)
    return names


def find_names_types_values_of_column(table_name, entity, data, class_names):
    fields = data[table_name].items()
    types = dict()  # [field_name : column_type]
    names = dict()  # [field_name : column_name]
    values = dict()  # [field_name : value]

    for field_name, field_object in fields:
        if isinstance(field_object, Column) or isinstance(field_object, PrimaryKey):
            types[field_name] = field_object.type
            if entity is not None:
                values[field_name] = getattr(entity, field_name)
        elif isinstance(field_object, ManyToOne) or isinstance(field_object, OneToOne):
            primary_key = find_type_of_primary_key_of_relation(field_object.other, data, class_names)
            assert primary_key is not None
            primary_key_field_name, primary_key_name, primary_key_type = primary_key
            types[field_name] = primary_key_type
            if entity is not None:
                relation_object = getattr(entity, field_name)
                if not isinstance(relation_object, Relationship):
                    values[field_name] = getattr(relation_object, primary_key_field_name)
                else:
                    values[field_name] = None

        names[field_name] = get_column_name(field_object, field_name)

    return types, names, values


def find_type_of_primary_key_of_relation(table_name, data, class_names):
    # we need to find a primary key of the table to which relationship is
    # in order to find the type of the relation field
    entity = class_names.get(table_name)
    table_name = get_table_name(entity)
    primary_key = find_primary_key_of_table(data, table_name)
    assert primary_key is not None
    primary_key_field_name, primary_key_name, primary_key_type = primary_key
    return primary_key_field_name, primary_key_name, primary_key_type


def get_object_pk(obj, model, data):
    table_name = get_table_name(model)
    pk_field_name, _, _ = find_primary_key_of_table(data, table_name)
    primary_key = getattr(obj, pk_field_name)
    return primary_key


def get_field_names_dict(model, data, class_names, inheritance):
    table_name = get_table_name(model)
    _, names, _ = find_names_types_values_of_column(table_name, None, data, class_names)
    model_inherits = (type(model) in inheritance.keys())
    if model_inherits:
        __add_parent_fields(model, names, data, class_names, inheritance)
    return names


def __add_parent_fields(model, child_names, data, class_names, inheritance):
    parents = inheritance[type(model)]
    table_name = get_table_name(model)
    _, model_pk, _ = find_primary_key_of_table(data, table_name)

    for parent_class in parents:
        parent_name = get_table_name(parent_class)
        _, names, _ = find_names_types_values_of_column(parent_name, None, data, class_names)
        for key, value in names.items():
            if key not in child_names.keys():
                child_names[key] = value
