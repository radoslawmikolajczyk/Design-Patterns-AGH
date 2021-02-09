from entity.entity import Entity
from fields.field import Field
from fields.relationship import ManyToMany


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
    if entity._table_name is not "":
        table_name = entity._table_name
    else:
        if isinstance(entity, Entity):
            entity = type(entity)
        table_name = entity.__name__.lower()
    return table_name
