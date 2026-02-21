import logging

LOGGER_NAME = "DataProcessor"
LOG_FORMAT = "%(asctime)s | [%(name)s] %(levelname)s - %(message)s"


def init_logging() -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
