def xstr(s):
    if s is None:
        return ''
    return str(s)


def xint(s):
    if s is None:
        return ''
    try:
        return int(s)
    except ValueError:
        return ''


def xfloat(s):
    if s is None:
        return ''
    try:
        return float(s)
    except ValueError:
        return ''
