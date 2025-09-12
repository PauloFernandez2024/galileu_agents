import logging
from logging.handlers import RotatingFileHandler

def setup_logger(log_path, log_level, max_log_size=1 * 1024 * 1024, backup_count=2):
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))

    handler_rotating = RotatingFileHandler(
        log_path, maxBytes=max_log_size, backupCount=2
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
    handler_rotating.setFormatter(formatter)
    logger.addHandler(handler_rotating)
    return logger
