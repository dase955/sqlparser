from pymilvus import Collection
from configparser import ConfigParser 

def delete(query):
    count = None
    collection_name = query['coll_name']
    partition_name = None
    if 'part_name' in query:
        partition_name = query['part_name']
    expr = query['expr']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Delete'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')
    
    collection = Collection(name=collection_name, using=using)
    if timeout is None:
        count = collection.delete(expr=expr, partition_name=partition_name).delete_count
    else:
        count = collection.delete(expr=expr, partition_name=partition_name, timeout=timeout).delete_count
    print('delete ' + count + ' rows')