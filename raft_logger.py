import logging
import sys
from utils import parse_config


config = parse_config()

if config['log_level'] == 'ERROR':
    log_level = logging.ERROR
elif config['log_level'] == 'DEBUG':
    log_level = logging.DEBUG
else:
    log_level = logging.INFO

logger = logging.getLogger('raft_logger')
logger.setLevel(log_level)
formatter = formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')


stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
file_handler = logging.FileHandler(config['log_file'])
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stdout_handler)
