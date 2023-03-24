import logging
from utils import parse_config


config = parse_config()
if config['log_level'] == 'ERROR':
    log_level = logging.ERROR
elif config['log_level'] == 'DEBUG':
    log_level = logging.DEBUG
else:
    log_level = loggin.INFO

logging.basicConfig(filename=config['log_file'], encoding='utf-8', level=log_level)
