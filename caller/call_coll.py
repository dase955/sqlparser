from pymilvus import CollectionSchema, FieldSchema, utility, Collection, DataType
from configparser import ConfigParser 

def show_coll(query):
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Collection'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')
    
    collection_list = None
    collection_names = []
    if timeout is None:
        collection_names = utility.list_collections(using=using)
    else:
        collection_names = utility.list_collections(using=using, timeout=timeout)

    collection_list = [collection_name + ' : ' + Collection(collection_name, using=using).description for collection_name in collection_names]

    # print output
    print('collection:')
    print('-----------')
    for item in collection_list:
        print(item)

def drop_coll(query):
    collection_name = query['name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Collection'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    if timeout is None:
        utility.drop_collection(collection_name=collection_name, using=using)
    else:
        utility.drop_collection(collection_name=collection_name, using=using, timeout=timeout)
    
def rename_coll(query):
    old_collection_name = query['old_coll']
    new_collection_name = query['new_coll']
    new_db_name = query['new_db']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Collection'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    if timeout is None:
        utility.rename_collection(old_collection_name=old_collection_name, new_collection_name=new_collection_name,
                                        new_db_name=new_db_name, using=using)
    else:
        utility.rename_collection(old_collection_name=old_collection_name, new_collection_name=new_collection_name,
                                        new_db_name=new_db_name, using=using, timeout=timeout)

def load_coll(query):
    collection_name = query['name']
    replica_number = 1
    if 'replica_number' in query:
        replica_number = query['replica_number']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    _async = False
    _refresh = False
    section = 'Collection'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')
    if '_async' in configur[section]:
        _async = configur.getboolean(section, '_async')
    if '_refresh' in configur[section]:
        _refresh = configur.getboolean(section, '_refresh')

    collection = Collection(collection_name, using=using)
    if timeout is None:
        collection.load(replica_number=replica_number, _async=_async, _refresh=_refresh)
    else:
        collection.load(replica_number=replica_number, timeout=timeout, _async=_async, _refresh=_refresh)
        
def release_coll(query):
    collection_name = query['name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Collection'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(collection_name, using=using)
    if timeout is None:
        collection.release()
    else:
        collection.release(timeout=timeout)
        
def compact_coll(query):
    collection_name = query['name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Collection'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(collection_name, using=using)
    if timeout is None:
        collection.compact()
    else:
        collection.compact(timeout=timeout)