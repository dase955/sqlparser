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

    collection_list = [collection_name for collection_name in collection_names]

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
        
def str_to_dtype_for_field(field_dict):
    # field type 和 element type需要改
    str_to_dtype = {}
    str_to_dtype['BOOL'] = DataType.BOOL
    str_to_dtype['INT8'] = DataType.INT8
    str_to_dtype['INT16'] = DataType.INT16
    str_to_dtype['INT32'] = DataType.INT32
    str_to_dtype['INT64'] = DataType.INT64
    str_to_dtype['FLOAT'] = DataType.FLOAT
    str_to_dtype['DOUBLE'] = DataType.DOUBLE
    str_to_dtype['VARCHAR'] = DataType.VARCHAR
    str_to_dtype['JSON'] = DataType.JSON
    str_to_dtype['ARRAY'] = DataType.ARRAY
    str_to_dtype['BINARY_VECTOR'] = DataType.BINARY_VECTOR
    str_to_dtype['FLOAT_VECTOR'] = DataType.FLOAT_VECTOR

    # convert str to DataType
    field_dict['type'] = str_to_dtype[field_dict['type']]
    if 'element_type' in field_dict:
        field_dict['element_type'] = str_to_dtype[field_dict['element_type']]

    return field_dict

def create_coll(query):
    # convert field str to field dict, including converting DataType str to DataType
    field_dicts = [str_to_dtype_for_field(item) for item in query['fields']]
    collection_name = query['name']
    
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    section = 'Collection'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')

    # convert field dict to FieldSchema
    # pymilvus的construct_from_dict不能将vector的dim、array的max_capacity、varchar的max_length解析，这里分开处理
    fields = []
    for field_dict in field_dicts:
        if field_dict['type'] in (DataType.BINARY_VECTOR, DataType.FLOAT_VECTOR):
            fields.append(FieldSchema(name=field_dict['name'], dim=field_dict['dim'], 
                                      dtype=field_dict['type'], description=field_dict.get('description', ''),
                                      is_dynamic=field_dict.get('is_dynamic', False)))
        elif field_dict['type'] is DataType.VARCHAR:
            fields.append(FieldSchema(name=field_dict['name'], dtype=field_dict['type'],
                                      max_length=field_dict['max_length'], description=field_dict.get('description', ''),
                                      auto_id=field_dict.get('auto_id', False), is_primary=field_dict.get('is_primary', False),
                                      is_partition_key=field_dict.get('is_partition_key', False), is_dynamic=field_dict.get('is_dynamic', False)))
        elif field_dict['type'] is DataType.ARRAY and field_dict['element_type'] is not DataType.VARCHAR:
            fields.append(FieldSchema(name=field_dict['name'], dtype=field_dict['type'],
                                      max_capacity=field_dict['max_capacity'], element_type=field_dict['element_type'],
                                      description=field_dict.get('description', ''), is_dynamic=field_dict.get('is_dynamic', False)))
        elif field_dict['type'] is DataType.ARRAY and field_dict['element_type'] is DataType.VARCHAR:
            fields.append(FieldSchema(name=field_dict['name'], dtype=field_dict['type'], 
                                      max_capacity=field_dict['max_capacity'], element_type=field_dict['element_type'], 
                                      max_length=field_dict['max_length'],
                                      description=field_dict.get('description', ''), is_dynamic=field_dict.get('is_dynamic', False)))
        else:
            fields.append(FieldSchema.construct_from_dict(field_dict))
        #fields = [FieldSchema.construct_from_dict(field_dict) for field_dict in field_dicts] 
    schema = CollectionSchema(fields=fields, description=query['params'].get('description', ''), 
                              enable_dynamic_field=query['params'].get('enable_dynamic_field', 0)) # 0->False, 1->True
    if timeout is None:
        collection = Collection(name=collection_name, schema=schema, using=using, 
                                num_shards=query['params'].get('num_shards', 1))
    else:
        collection = Collection(name=collection_name, schema=schema, using=using, 
                                num_shards=query['params'].get('num_shards', 1), timeout=timeout)
        
    # 检查时可以用CollectionSchema下的to_dict方法、Collection的num_shards方法和FieldSchema的to_dict方法
    # 记得更新下READMD里Create Collection