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