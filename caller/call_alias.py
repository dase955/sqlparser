from pymilvus import utility, Collection
from configparser import ConfigParser 

def create_alias(query):
    collection_name = query['coll']
    alias = query['alias']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Alias'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    if timeout is None:
        utility.create_alias(collection_name=collection_name, alias=alias, using=using)
    else:
        utility.create_alias(collection_name=collection_name, alias=alias, using=using, timeout=timeout)
        
def drop_alias(query):
    alias = query['alias']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Alias'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    if timeout is None:
        utility.drop_alias(alias=alias, using=using)
    else:
        utility.drop_alias(alias=alias, using=using, timeout=timeout)
        
def show_alias(query):
    collection_name = query['coll']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    alias_list = None
    collection = Collection(collection_name, using=using)
    alias_list = [alias for alias in collection.aliases]
    
    # print output
    print('alias:')
    print('------')
    for item in alias_list:
        print(item)
