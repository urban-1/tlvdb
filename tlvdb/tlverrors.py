#
# Project errors:
#
# - 0-100       reserved
# - 100-200     TLV errors
# - 500-600     Indexing errors
# - 1000-       Database errors
#
#


class ErrorWithCode(RuntimeError):
    def __init__(self, message, code):
        self.code = code
        self.message = message

    def __str__(self):
        return repr("%d: %s" % (self.code, self.message))


#
# TLV
#
class TlvSpecError(ErrorWithCode):
    def __init__(self, message, original_message):
        final_message = "%s. Struct Message: %s" % (message, original_message)
        super(TlvSpecError, self).__init__(final_message, 100)


#
# Database
#
class AlreadyInTrance(ErrorWithCode):
    def __init__(self, message, code=1000):
        super(AlreadyInTrance, self).__init__(message, code)


class TransactionError(ErrorWithCode):
    def __init__(self, message, code=1010):
        super(TransactionError, self).__init__(message, code)


class IndexNotFoundError(TransactionError):
    def __init__(self, message):
        super(IndexNotFoundError, self).__init__(message, 1011)
