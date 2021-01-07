import random
import string


def quote_database_object_name_unsafe(data: str) -> str:
    """Quote safe known string in double quotes.

    THIS METHOD IS UNSAFE FOR UNKNOWN STRINGS.

    Parameters
    ----------
    data : string to be quoted

    Returns
    -------
    Quoted string
    """
    escaped_str = data.replace('"', '""')
    return f'"{escaped_str}"'


def quote_known_string_unsafe(data: str) -> str:
    """Quote safe known string in single quotes.

    THIS METHOD IS UNSAFE FOR UNKNOWN STRINGS.

    Parameters
    ----------
    data : string to be quoted

    Returns
    -------
    Quoted string
    """
    escaped_str = data.replace("'", "''")
    return f"'{escaped_str}'"


def quote_string(data: str) -> str:
    """Quote string in $ quotes with 5 character escape sequence that is not used in the string.

    Parameters
    ----------
    data : string to be quoted

    Returns
    -------
    Quoted string
    """
    while True:
        escape_seq = generate_escape_seq()
        if escape_seq not in data:
            break

    return f"${escape_seq}${data}${escape_seq}$"


def generate_escape_seq() -> str:
    """Generate 6 character escape sequence.
    Digits, upper and lower letters are used.
    Escape seq never starts with a number.

    Returns
    -------
    6 character escape sequence
    """
    return ''.join([
        random.choice(string.ascii_letters),
        *[random.choice(string.digits + string.ascii_letters) for _ in range(5)]
    ])
