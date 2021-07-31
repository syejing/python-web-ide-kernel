import itertools
import rlcompleter


class KernelCompleter(object):
    def __init__(self, namespace):
        self.namespace = namespace
        self.completer = rlcompleter.Completer(namespace)

    def complete(self, text):
        matches = []
        for state in itertools.count():
            comp = self.completer.complete(text, state)
            if comp is None:
                break
            matches.append(comp)
        return matches

