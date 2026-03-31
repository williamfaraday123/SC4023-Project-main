class EncodingError(Exception):
    pass

class InvalidValueError(EncodingError):
    pass

class InvalidBlockError(EncodingError):
    pass

class InvalidConversionError(EncodingError):
    pass

class InvalidDateError(EncodingError):
    pass

class DataOverflowError(EncodingError):
    pass

class StorageError(Exception):
    pass
