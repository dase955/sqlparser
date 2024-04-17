from pymilvus import Collection, Partition
from configparser import ConfigParser 

def create_part(query):
    collection_name = query['coll']
    partition_name = query['part']
    description = ''
    if 'description' in query:
        description = query['description']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    collection = Collection(collection_name, using=using)
    collection.create_partition(partition_name=partition_name, description=description)
    
def show_part(query):
    collection_name = query['coll']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    partition_list = None
    collection = Collection(collection_name, using=using)
    # partition_list = [partition.name + ' --- ' + partition.description for partition in collection.partitions]
    partition_list = [partition.name for partition in collection.partitions]

    # print output
    print('partition:')
    print('----------')
    for item in partition_list:
        print(item)
        
def drop_part(query):
    collection_name = query['coll']
    partition_name = query['part']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Partition'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(collection_name, using=using)
    if timeout is None:
        collection.drop_partition(partition_name=partition_name)
    else:
        collection.drop_partition(partition_name=partition_name, timeout=timeout)
        
def load_part(query):
    collection_name = query['coll']
    partition_names = query['parts']
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
    section = 'Partition'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')
    if '_async' in configur[section]:
        _async = configur.getboolean(section, '_async')
    if '_refresh' in configur[section]:
        _refresh = configur.getboolean(section, '_refresh')

    collection = Collection(collection_name, using=using)
    if timeout is None:
        collection.load(partition_names=partition_names, replica_number=replica_number, _async=_async, _refresh=_refresh)
    else:
        collection.load(partition_names=partition_names, replica_number=replica_number, timeout=timeout, _async=_async, _refresh=_refresh)
    
def release_part(query):
    collection_name = query['coll']
    partition_names = query['parts']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Partition'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(collection_name, using=using)
    for name in partition_names:
        partition = Partition(collection=collection, name=name)
        if timeout is None:
            partition.release()
        else:
            partition.release(timeout=timeout)