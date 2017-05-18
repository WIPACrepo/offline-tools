
import string

def make_regex_for_var(pattern, var_name, ignored_var_names = []):
    """
    Uses a string pattern that contains at least one variable name `var_name` to
    create a regex in order to find from a string exactly this variable.

    For instance: We want to find the `sub_run_id` in a file name that follows this pattern:
    `Level2_{detector_configuration}.{season}_data_Run{run_id:0>8}_Subrun{sub_run_id:0>8}.i3.bz2`

    Use `make_regex_for_var(pattern, 'sub_run_id')` in order to gather the regex.

    Args:
        pattern (str): The pattern that contains `var_name`
        var_name (str): The variable name for that the regex should be created
        ignored_var_names (list|str): List of var_names that are ignore and can have any value. If the value is '*', all other var names are ignored.

    Returns:
        str: The regex to find `var_name`
    """

    import re

    formatter = string.Formatter()

    if ignored_var_names == '*':
        ignored = lambda name: True
    else:
        ignored = lambda name: name in ignored_var_names

    result = ''
    for v in formatter.parse(pattern):
        result += re.escape(v[0])

        if v[1] is not None:
            if v[1] == var_name:
                result += '(.+)'
            elif ignored(v[1]):
                result += '.*'
            else:
                result += re.escape('{' + v[1])

                if v[3] is not None:
                    result += re.escape('!' + v[3])

                if v[2] is not None and len(v[2]):
                    result += re.escape(':' + v[2])

                result += re.escape('}')

    return result

def replace_var(text, var_name, replacement):
    """
    Replaces a verable in a string that can be used with `format()`.

    Example:

        replace_var('Hello {name}, my name is {my_name:>9}', 'my_name', 'secret')

    That is in particular interesting if you want to create a glob for a path that contains vars:

        replace_var('/my/path/{user}/documents/{year:0>4}/file_{month}{day}.dat', 'month', '*')

    Args:
        text (str): The string
        var_name (str): The variable name
        replacement (str): The replacement for the variable

    Returns:
        str: the modified string
    """

    formatter = string.Formatter()

    result = ''
    for v in formatter.parse(text):
        result += v[0]

        if v[1] is not None:
            if v[1] == var_name:
                result += replacement
            else:
                result += '{' + v[1]

                if v[3] is not None:
                    result += '!' + v[3]

                if v[2] is not None and len(v[2]):
                    result += ':' + v[2]

                result += '}'

    return result

def replace_all_vars(text, replacement):
    """
    Like replace_var but it replaces all variables.

    Args:
        text (str): The string
        replacement (str): The replacement for the variable

    Returns:
        str: the modified string
    """

    formatter = string.Formatter()

    result = ''
    for v in formatter.parse(text):
        result += v[0]

        if v[1] is not None:
            result += replacement

    return result

