from pymilvus import connections
from configparser import ConfigParser 
from pymilvus.exceptions import MilvusException


def connect():
    configur = ConfigParser() 
    configur.read('config.ini')
    section = 'Connection'

    # optional arguments
    user = None
    password = None
    if 'user' in configur[section]:
        user = configur.get(section, 'user')
    if 'password' in configur[section]:
        password = configur.get(section, 'password')

    # necessary arguments
    alias = configur.get(section, 'alias')
    host = configur.get(section, 'host')
    port = configur.get(section, 'port')

    if user is None or password is None:
        connections.connect(
            alias = alias,
            host = host,
            port = port
        )
    else:
        connections.connect(
            alias = alias,
            user = user,
            password = password,
            host = host,
            port = port
        )
    

def disconnect():
    configur = ConfigParser() 
    configur.read('config.ini')
    section = 'Connection'

    # necessary arguments
    alias = configur.get(section, 'alias')

    connections.disconnect(
        alias = alias
    )