
import string

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

