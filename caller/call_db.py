from configparser import ConfigParser 
from pymilvus import db

def create_db(query):
    db_name = query['name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Database'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    if timeout is None:
        db.create_database(db_name=db_name, using=using)
    else:
        db.create_database(db_name=db_name, using=using, timeout=timeout)

def use_db(query):
    db_name = query['name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    db.using_database(db_name=db_name, using=using)

def show_db(query):
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Database'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')
    
    db_list = None
    if timeout is None:
        db_list = db.list_database(using=using)
    else:
        db_list = db.list_database(using=using, timeout=timeout)
    
    # print output
    print('database:')
    print('---------')
    for item in db_list:
        print(item)

def drop_db(query):
    db_name = query['name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Database'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    if timeout is None:
        db.drop_database(db_name=db_name, using=using)
    else:
        db.drop_database(db_name=db_name, using=using, timeout=timeout)