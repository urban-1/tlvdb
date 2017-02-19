class ErrorWithCode(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return repr("%d: %s" % (self.code, self.message))

class TransactionError(RuntimeError, ErrorWithCode):
    def __init__(self, message):
        super(TransactionError, self).__init__(1000, message)


class AlreadyInTrance(RuntimeError, ErrorWithCode):
    def __init__(self, message):
        super(TransactionError, self).__init__(1001, message)
