import builtins

import pandas as pd

from session import extract_header


class BlackHoleDisplayHook(object):
    def __call__(self, obj):
        pass


class DisplayHook(object):
    def __init__(self, session, pub_socket):
        self.session = session
        self.pub_socket = pub_socket
        self.parent_header = {}

    def set_parent(self, parent):
        self.parent_header = extract_header(parent)

    def __call__(self, obj, automatic=True):
        if obj is None:
            return

        builtins._ = obj
        if automatic:
            if isinstance(obj, pd.DataFrame):
                msg = self.session.msg("pyout", {"data": {"category": "markdown", "content": obj.to_markdown()}},
                                       parent=self.parent_header)
            elif getattr(obj, "_repr_markdown_", None) is not None:
                msg = self.session.msg("pyout", {"data": {"category": "markdown", "content": obj._repr_markdown_()}},
                                       parent=self.parent_header)
            elif getattr(obj, '_repr_html_', None) is not None:
                msg = self.session.msg("pyout", {"data": {"category": "html", "content": obj._repr_html_()}},
                                       parent=self.parent_header)
            else:
                msg = self.session.msg("pyout", {"data": {"category": "text", "content": repr(obj)}},
                                       parent=self.parent_header)
        else:
            msg = self.session.msg("pyout", {"data": obj}, parent=self.parent_header)

        self.pub_socket.send_json(msg)
