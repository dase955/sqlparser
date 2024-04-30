from pymilvus import Collection, DataType, utility
from configparser import ConfigParser 

def bulk_insert(query):
    collection_name = query['coll']
    files = query['files']
    partition_name = None
    if 'part' in query:
        partition_name = query['part']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using -- connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Insert'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    if timeout is None:
        for file in files:
            utility.do_bulk_insert(collection_name=collection_name, files=[file], partition_name=partition_name, using=using)
    else:
        for file in files:
            utility.do_bulk_insert(collection_name=collection_name, files=[file], partition_name=partition_name, using=using)
            
def insert(query):
    count = None
    collection_name = query['coll_name']
    partition_name = None
    data = query['data']
    if 'part_name' in query:
        partition_name = query['part_name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Insert'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(name=collection_name, using=using)
        
    if timeout is None:
        count = collection.insert(data=data, partition_name=partition_name).insert_count
    else:
        count = collection.insert(data=data, partition_name=partition_name, timeout=timeout).insert_count

    # Milvus automatically triggers the flush() operation.
    # In most cases, manual calls to this operation are not necessary.
    # collection.flush()
    print('insert ' + str(count) + ' rows')

def upsert(query):
    count = None
    collection_name = query['coll_name']
    partition_name = None
    data = query['data']
    if 'part_name' in query:
        partition_name = query['part_name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Insert'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(name=collection_name, using=using)
        
    if timeout is None:
        count = collection.upsert(data=data, partition_name=partition_name).upsert_count
    else:
        count = collection.upsert(data=data, partition_name=partition_name, timeout=timeout).upsert_count
    # collection.flush()
    print('upsert ' + str(count) + ' rows')