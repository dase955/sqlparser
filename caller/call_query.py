from pymilvus import Collection
from configparser import ConfigParser 

def query(query):
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    consistency_level = 'Bounded'  # (Strong, Bounded, Session, Eventually), default: Bounded
    ignore_growing = False
    section = 'Query'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')
    if 'consistency_level' in configur[section]:
        consistency_level = configur.get(section, 'consistency_level')
    if 'ignore_growing' in configur[section]:
        ignore_growing = configur.getboolean(section, 'ignore_growing')

    collection_name = query['coll_name']
    field_list = query['fields']
    expr = query['expr']
    partition_names = query['parts']
    limit = query['limit']
    offset = query['offset']
    

    result_list = None
    output_fields = None # set this list according to field_list
    collection = Collection(collection_name, using=using)
    output_fields = field_list

    if timeout is None:
        result_list = collection.query(expr=expr, output_fields=output_fields, partition_names=partition_names,
                                       limit=limit, offset=offset, consistency_level=consistency_level, ignore_growing=ignore_growing)
    else:
        result_list = collection.query(expr=expr, output_fields=output_fields, partition_names=partition_names, timeout=timeout, 
                                       limit=limit, offset=offset, consistency_level=consistency_level, ignore_growing=ignore_growing)
        
    for result in result_list:
        print(result)