_USE_DEFAULT = object()

def prompt_yes_no(msg, default=None):
    if default is None:
        default_msg = ""
    elif default:
        default_msg = " [Y]"
    else:
        default_msg = " [N]"
    prompt = "{}{}: ".format(msg, default_msg)
    err_msg = "Please input Y or N."
    while True:
        resp = input(prompt).strip().lower()
        if len(resp) > 0:
            if resp[0] == "y":
                return True
            elif resp[0] == "n":
                return False
            else:
                print(err_msg)
        elif default is not None:
            return default
        else:
            print(err_msg)


def prompt_nonempty(msg, default=_USE_DEFAULT):
    if default is _USE_DEFAULT:
        default_msg = ""
    else:
        default_msg = " [default: {}]".format(default)
    prompt = "{}{}: ".format(msg, default_msg)
    err_msg = "Input is required."
    while True:
        resp = input(prompt).strip()
        if len(resp) > 0:
            return resp
        elif default is not _USE_DEFAULT:
            return default
        else:
            print(err_msg)
