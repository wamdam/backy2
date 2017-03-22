import setproctitle

_OLD = ""

def notify(process_name, msg=''):
    """ This method can receive notifications and append them in '[]' to the
    process name seen in ps, top, ...
    """
    global _OLD
    if msg:
        new_msg = '{} [{}]'.format(
                process_name,
                msg.replace('\n', ' ')
        )
    else:
        new_msg = process_name

    if _OLD != new_msg:
        setproctitle.setproctitle(new_msg)
        _OLD = new_msg

