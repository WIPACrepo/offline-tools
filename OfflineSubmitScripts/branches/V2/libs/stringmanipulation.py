
import string

def replace_var(text, var_name, replacement):
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
