class DataScribeError(Exception):
    pass


class ConnectorError(DataScribeError):
    pass


class LLMClientError(DataScribeError):
    pass


class WriterError(DataScribeError):
    pass


class ConfigError(DataScribeError):
    pass
