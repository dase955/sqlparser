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
    ignore_growing = False
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
    if 'ignore_growing' in configur[section]:
        ignore_growing = configur.getboolean(section, 'ignore_growing')
    param['ignore_growing'] = ignore_growing

    json_dict = {}
    result = None
    result_dict_list = []
    message = 'success'
    output_fields = [] # set this list according to field_list
    try:
        collection = Collection(collection_name, using=using)
        
        if len(field_list) == 0:
            message = 'failure'
        elif field_list[0] == '*':
            for field in collection.schema.fields:
                if field.to_dict()['type'] != DataType.BINARY_VECTOR:
                    continue;
                elif field.to_dict()['type'] != DataType.FLOAT_VECTOR:
                    continue;
                else:
                    output_fields.append(field.to_dict()['name'])
        else:
            output_fields = field_list

        if message == 'success':
            if timeout is None:
                result = collection.search(data=data, anns_field=anns_field, param=param, limit=limit, expr=expr, 
                                            partition_names=partition_names, output_fields=output_fields, round_decimal=round_decimal,
                                            consistency_level=consistency_level, _async=_async, _callback=_callback)
            else:
                result = collection.search(data=data, anns_field=anns_field, param=param, limit=limit, expr=expr, 
                                            partition_names=partition_names, output_fields=output_fields, round_decimal=round_decimal,
                                            timeout=timeout, consistency_level=consistency_level, _async=_async, _callback=_callback)
            for hits in result: # type: ignore
                for hit in hits:
                    # get the value of an output field specified in the search request.
                    # dynamic fields are supported, but vector fields are not supported yet.    
                    result_dict = {}
                    result_dict['pk'] = hit.entity.pk
                    result_dict['score'] = hit.entity.score
                    for field_name in output_fields:
                        result_dict[field_name] = hit.entity.get(field_name)
                    result_dict_list.append(result_dict)
    except MilvusException:
        message = 'failure'
    finally:
        json_dict['message'] = message

    if message == 'success':
        json_dict['result'] = [json.dumps(result_dict) for result_dict in result_dict_list]
    json_str = json.dumps(json_dict, indent=4)
    json_str = json_str.replace(',', ',\n')

    return json_str