import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logconfig.conf')

logging.config.fileConfig(log_file_path)

def getLogger(name):
    logger = logging.getLogger(name)
    logger.info('logger retrieved')
    return logger

# logger.debug('debug message')
# logger.info('info message')
# logger.warning('warn message')
# logger.error('error message')
# logger.critical('critical message')