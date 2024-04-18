from pymilvus import Collection, DataType
from configparser import ConfigParser 

def create_idx(query):
    index_name = query['idx']
    collection_name = query['coll']
    field_name = query['field']
    index_params = None
    if 'params' in query:
        index_params = query['params']
    print(index_params)
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Index'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(collection_name, using=using)
    # check field type, vector field only support vector index, scalar field only support scalar index
    is_vector = False
    for field in collection.schema.fields:
        field = field.to_dict()
        if field['name'] == field_name:
            is_vector = (field['type']==DataType.FLOAT_VECTOR) or (field['type']==DataType.BINARY_VECTOR)
    # vector field but not vector index
    if (is_vector) and (index_params is None):
        print('need params when build index on vector field.')
    # scalar field but not scalar index
    elif (not is_vector) and (index_params is not None):
        print('need no params when build index on scalar field.')
    else:
        if timeout is None:
            collection.create_index(field_name=field_name, index_params=index_params, index_name=index_name)
        else:
            collection.create_index(field_name=field_name, index_params=index_params, timeout=timeout, index_name=index_name)
            
def drop_idx(query):
    collection_name = query['coll']
    index_name = query['idx']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Index'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    collection = Collection(collection_name, using=using)
    if timeout is None:
        collection.drop_index(index_name=index_name)
    else:
        collection.drop_index(timeout=timeout, index_name=index_name)
        
def show_idx(query):
    collection_name = query['coll']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    index_list = None
    collection = Collection(collection_name, using=using)
    index_list = [index.index_name + ' : ' + ('scalar index' if len(index.params) == 0 else 'vector index') for index in collection.indexes]
    
    # print output
    print('index:')
    print('------')
    for item in index_list:
        print(item)