from pymilvus import Collection, DataType
from configparser import ConfigParser 

def search(query):
    configur = ConfigParser() 
    configur.read('config.ini')

    # using is connection alias
    section = 'Connection'
    using = configur.get(section, 'alias')

    timeout = None
    consistency_level = 'Bounded'  # (Strong, Bounded, Session, Eventually), default: Bounded
    _async = False
    _callback = False
    round_decimal = -1
    section = 'Search'
    if 'timeout' in configur[section]:
        timeout = configur.getfloat(section, 'timeout')
    if 'consistency_level' in configur[section]:
        consistency_level = configur.get(section, 'consistency_level')
    if '_async' in configur[section]:
        _async = configur.getboolean(section, '_async')
    if '_callback' in configur[section]:
        _callback = configur.getboolean(section, '_callback')
    if 'round_decimal' in configur[section]:
        round_decimal = configur.getint(section, 'round_decimal')
    param = query['param']

    collection_name = query['coll_name']
    field_list = query['fields']
    partition_names = query['parts']
    data = query['data']
    anns_field = query['anns']
    limit = query['limit']
    expr = query['expr']

    result = None
    result_dict_list = []
    output_fields = [] # set this list according to field_list
    
    collection = Collection(collection_name, using=using)
    output_fields = field_list

    # for binary vector
    new_data = []
    for vector in data:
        all_int = True
        for num in vector:
            if not isinstance(num, int):
                all_int = False
                break
        if all_int:
            new_data.append(bytes(vector))
        else:
            new_data.append(vector)
    data = new_data

    if timeout is None:
        result = collection.search(data=data, anns_field=anns_field, param=param, limit=limit, expr=expr, 
                                   partition_names=partition_names, output_fields=output_fields, round_decimal=round_decimal,
                                   consistency_level=consistency_level, _async=_async, _callback=_callback)
    else:
        result = collection.search(data=data, anns_field=anns_field, param=param, limit=limit, expr=expr, 
                                   partition_names=partition_names, output_fields=output_fields, round_decimal=round_decimal,
                                   timeout=timeout, consistency_level=consistency_level, _async=_async, _callback=_callback)

    for hits in result:
        for hit in hits:
            # get the value of an output field specified in the search request.
            # dynamic fields are supported, but vector fields are not supported yet.    
            result_dict = {'pk': hit.entity.pk, 'score': hit.entity.score}
            result_dict = result_dict | hit.fields
            result_dict_list.append(result_dict)
    
    for result_dict in result_dict_list:
        print(result_dict)

    return result
