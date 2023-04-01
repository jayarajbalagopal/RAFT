import json


def parse_config():
    file = open("config.json", 'r')
    config = json.loads(file.read())
    return config
