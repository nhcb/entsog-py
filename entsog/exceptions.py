class NoMatchingDataError(Exception):
    pass

class UnauthorizedError(Exception):
    pass

class PaginationError(Exception):
    pass

class BadGatewayError(Exception):
    pass

class TooManyRequestsError(Exception):
    pass

class GatewayTimeOut(Exception):
    pass

class NotFoundError(Exception):
    pass
