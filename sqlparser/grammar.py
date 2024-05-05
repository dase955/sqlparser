# -*- coding: utf-8 -*-
import json

from ply import lex,yacc

from . import lexer
from .exceptions import GrammarException

def p_expression(p):
    """ expression : db END
                   | coll END
                   | part END
                   | idx END
                   | insert END
                   | delete END
                   | query END
                   | search END
    """
    p[0] = p[1]

###################################################
############         Database          ############
###################################################

def p_db(p):
    """ db : create_db
           | show_db
           | drop_db
           | use_db
    """
    p[0] = p[1]

def p_create_db(p):
    """ create_db : CREATE DATABASE STRING
    """
    p[0] = {
        'type' : 'create_db',
        'name' : p[3]
    }

def p_use_db(p):
    """ use_db : USE STRING
    """
    p[0] = {
        'type' : 'use_db',
        'name' : p[2]
    }

def p_show_db(p):
    """ show_db : SHOW DATABASES
    """
    p[0] = {
        'type' : 'show_db'
    }

def p_drop_db(p):
    """ drop_db : DROP DATABASE STRING
    """
    p[0] = {
        'type' : 'drop_db',
        'name' : p[3]
    }

###################################################
############        Collection         ############
###################################################
def p_coll(p):
    """ coll : show_coll
             | drop_coll
             | create_alias
             | drop_alias
             | show_alias
             | rename_coll
             | load_coll
             | release_coll
             | compact_coll
             | create_coll
    """
    p[0] = p[1]

def p_show_coll(p):
    """ show_coll : SHOW COLLECTIONS
    """
    p[0] = {
        'type' : 'show_coll'
    }

def p_drop_coll(p):
    """ drop_coll : DROP COLLECTION STRING
    """
    p[0] = {
        'type' : 'drop_coll',
        'name' : p[3]
    }

def p_create_alias(p):
    """ create_alias : CREATE ALIAS STRING FOR STRING
    """
    p[0] = {
        'type' : 'create_alias',
        'alias' : p[3],
        'coll' : p[5]
    }

def p_drop_alias(p):
    """ drop_alias : DROP ALIAS STRING FOR STRING
    """
    p[0] = {
        'type' : 'drop_alias',
        'alias' : p[3],
        'coll' : p[5]
    }

def p_show_alias(p):
    """ show_alias : SHOW ALIASES FOR STRING
    """
    p[0] = {
        'type' : 'show_alias',
        'coll' : p[4]
    }

def p_rename_coll(p):
    """ rename_coll : RENAME COLLECTION STRING TO STRING IN STRING
    """
    p[0] = {
        'type' : 'rename_coll',
        'old_coll' : p[3],
        'new_coll' : p[5],
        'new_db' : p[7]
    }

def p_load_coll(p):
    """ load_coll : LOAD COLLECTION STRING
                  | LOAD COLLECTION STRING WITH "{" QSTRING ":" NUMBER "}"
    """
    p[0] = {
        'type' : 'load_coll',
        'name' : p[3]
    }
    if len(p) > 4:
        p[0][p[6]] = p[8]

def p_release_coll(p):
    """ release_coll : RELEASE COLLECTION STRING
    """
    p[0] = {
        'type' : 'release_coll',
        'name' : p[3]
    }

def p_compact_coll(p):
    """ compact_coll : COMPACT COLLECTION STRING
    """
    p[0] = {
        'type' : 'compact_coll',
        'name' : p[3]
    }

def p_create_coll(p):
    """ create_coll : CREATE COLLECTION STRING "(" field_list ")" WITH "{" coll_param_list "}"
                    | CREATE COLLECTION STRING "(" field_list ")"
    """
    if len(p) == 11:
        p[0] = {
            'type' : 'create_coll',
            'name' : p[3],
            'fields' : p[5],
            'params' : p[9]
        }
    else:
        p[0] = {
            'type' : 'create_coll',
            'name' : p[3],
            'fields' : p[5],
            'params' : dict()
        }

def p_field_list(p):
    """ field_list : field field_list
                   | COMMA field field_list
                   | empty
    """
    if len(p) == 2:
        p[0] = list()
    if len(p) == 3:
        p[0] = p[1] + p[2]
    elif len(p) == 4:
        p[0] = p[2] + p[3]

def p_field(p):
    """ field : STRING type attr_list
    """
    p[0] = [{'name' : p[1] } | p[2] | p[3]]

def p_type(p):
    """ type : STRING
             | STRING "(" NUMBER ")"
             | STRING STRING "(" NUMBER ")"
             | STRING "(" NUMBER ")" STRING "(" NUMBER ")"
    """
    p[0] = dict()
    if len(p) == 2:
        p[0]['type'] = p[1].upper()
    elif len(p) == 5: # VARCHAR done
        p[0]['type'] = p[1].upper() # should be VARCHAR
        p[0]['max_length'] = p[3]
    elif len(p) == 6:
        if p[2].upper() == 'VECTOR': # VECTOR done, only ARRAY left
            p[0]['type'] = (p[1] + '_' + p[2]).upper()
            p[0]['dim'] = p[4]
        elif p[2].upper() == 'ARRAY': # not include VARCHAR ARRAY
            p[0]['type'] = p[2].upper()
            p[0]['element_type'] = p[1].upper()
            p[0]['max_capacity'] = p[4]
    elif len(p) == 9: # VARCHAR ARRAY, ARRAY done
        p[0]['type'] = p[5].upper() # should be ARRAY
        p[0]['element_type'] = p[1].upper() # should be VARCHAR
        p[0]['max_capacity'] = p[7]
        p[0]['max_length'] = p[3]

def p_attr_list(p):
    """ attr_list : empty
                  | attr attr_list
    """
    if len(p) == 2:
        p[0] = dict()
    elif len(p) == 3:
        p[0] = p[1] | p[2]

def p_attr(p):
    """ attr : PRIMARY KEY
             | PARTITION KEY
             | AUTO ID
             | DESCRIPTION "(" QSTRING ")"
    """
    p[0] = dict()
    # cannot explicitly set a field as a dynamic field
    if len(p) == 3:
        if p[1].upper() == 'PRIMARY' and p[2].upper() == 'KEY':
            p[0]['is_primary'] = True
        elif p[1].upper() == 'PARTITION' and p[2].upper() == 'KEY':
            p[0]['is_partition_key'] = True
        elif p[1].upper() == 'AUTO' and p[2].upper() == 'ID':
            p[0]['auto_id'] = True
    elif len(p) == 5:
        if p[1].upper() == 'DESCRIPTION':
            p[0]['description'] = p[3]

def p_coll_param_list(p):
    """ coll_param_list : coll_param coll_param_list
                        | COMMA coll_param coll_param_list
                        | empty
    """
    if len(p) == 2:
        p[0] = dict()
    elif len(p) == 3:
        p[0] = p[1] | p[2]
    elif len(p) == 4:
        p[0] = p[2] | p[3]

def p_coll_param(p):
    """ coll_param : QSTRING ":" QSTRING
                   | QSTRING ":" NUMBER
    """
    p[0] = dict()
    if len(p) > 2:
        p[0][p[1]] = p[3]

###################################################
############         Partition         ############
###################################################
def p_part(p):
    """ part : create_part
             | show_part
             | drop_part  
             | load_part
             | release_part
    """
    p[0] = p[1]

def p_create_part(p):
    """ create_part : CREATE PARTITION STRING ON STRING
                    | CREATE PARTITION STRING ON STRING WITH "{" QSTRING ":" QSTRING "}"
    """
    p[0] = {
        'type' : 'create_part',
        'part' : p[3],
        'coll' : p[5]
    }
    if len(p) > 6:
        p[0][p[8]] = p[10]

def p_show_part(p):
    """ show_part : SHOW PARTITIONS ON STRING
    """
    p[0] = {
        'type' : 'show_part',
        'coll' : p[4]
    }

def p_drop_part(p):
    """ drop_part : DROP PARTITION STRING ON STRING
    """
    p[0] = {
        'type' : 'drop_part',
        'part' : p[3],
        'coll' : p[5]
    }

def p_load_part(p):
    """ load_part : LOAD PARTITION STRING part_list ON STRING
                  | LOAD PARTITION STRING part_list ON STRING WITH "{" QSTRING ":" NUMBER "}"
    """
    p[0] = {
        'type' : 'load_part',
        'coll' : p[6],
        'parts' : [p[3]] + p[4]
    }
    if len(p) > 7:
        p[0][p[9]] = p[11]

def p_release_part(p):
    """ release_part : RELEASE PARTITION STRING part_list ON STRING
    """
    p[0] = {
        'type' : 'release_part',
        'coll' : p[6],
        'parts' : [p[3]] + p[4]
    }

def p_part_list(p):
    """ part_list : empty
                  | COMMA STRING part_list
    """
    if len(p) == 2:
        p[0] = []
    else:
        p[0] = [p[2]] + p[3]

###################################################
############           Index           ############
###################################################
def p_idx(p):
    """ idx : create_idx
            | show_idx
            | drop_idx
    """
    p[0] = p[1]

def p_create_idx(p):
    """ create_idx : CREATE INDEX STRING ON STRING "(" STRING ")"
                   | CREATE INDEX STRING ON STRING "(" STRING ")" WITH "{" idx_param_list "}"
    """
    p[0] = {
        'type' : 'create_idx',
        'idx' : p[3],
        'coll' : p[5],
        'field' : p[7]
    }
    if len(p) > 9:
        new_param_list = dict()
        old_param_list = p[11]
        if 'index_type' in old_param_list:
            new_param_list['index_type'] = old_param_list['index_type']
            del old_param_list['index_type']
        if 'metric_type' in old_param_list:
            new_param_list['metric_type'] = old_param_list['metric_type']
            del old_param_list['metric_type']
        new_param_list['params'] = old_param_list
        p[0]['params'] = new_param_list

def p_idx_param_list(p):
    """ idx_param_list : coll_param_list
    """
    p[0] = p[1]

def p_show_idx(p):
    """ show_idx : SHOW INDEXES ON STRING
    """
    p[0] = {
        'type' : 'show_idx',
        'coll' : p[4]
    }

def p_drop_idx(p):
    """ drop_idx : DROP INDEX STRING ON STRING
    """
    p[0] = {
        'type' : 'drop_idx',
        'idx' : p[3],
        'coll' : p[5]
    }

###################################################
############           Insert          ############
###################################################
def p_insert(p):
    """ insert : bulk_insert
               | insert_coll
               | insert_part
               | upsert_coll
               | upsert_part
    """
    p[0] = p[1]

def p_bulk_insert(p):
    """ bulk_insert : BULK INSERT PARTITION STRING ON STRING FROM file_list
                    | BULK INSERT COLLECTION STRING FROM file_list
    """
    if len(p) == 7:
        p[0] = {
            'type' : 'bulk_insert',
            'coll' : p[4],
            'files' : p[6]
        }
    else:
        p[0] = {
            'type' : 'bulk_insert',
            'part' : p[4],
            'coll' : p[6],
            'files' : p[8]
        }

def p_file_list(p):
    """ file_list : QSTRING file_list
                  | COMMA QSTRING file_list
                  | empty
    """
    if len(p) == 2:
        p[0] = []
    elif len(p) == 3:
        p[0] = [p[1]] + p[2]
    else:
        p[0] = [p[2]] + p[3]

def p_insert_coll(p):
    """ insert_coll : INSERT INTO STRING "(" field_name_list ")" VALUES values_list
    """
    p[0] = dict()
    p[0]['type'] = 'insert'
    p[0]['coll_name'] = p[3]
    data = list()
    for values in p[8]:
        new_dict = dict()
        for i in range(0, len(p[5])):
            new_dict[p[5][i]] = values[i]
        data.append(new_dict)
    p[0]['data'] = data

def p_insert_part(p):
    """ insert_part : INSERT INTO PARTITION STRING ON STRING "(" field_name_list ")" VALUES values_list
    """
    p[0] = dict()
    p[0]['type'] = 'insert'
    p[0]['part_name'] = p[4]
    p[0]['coll_name'] = p[6]
    data = list()
    for values in p[11]:
        new_dict = dict()
        for i in range(0, len(p[8])):
            new_dict[p[8][i]] = values[i]
        data.append(new_dict)
    p[0]['data'] = data

def p_upsert_coll(p):
    """ upsert_coll : UPSERT INTO STRING "(" field_name_list ")" VALUES values_list
    """
    p[0] = dict()
    p[0]['type'] = 'upsert'
    p[0]['coll_name'] = p[3]
    data = list()
    for values in p[8]:
        new_dict = dict()
        for i in range(0, len(p[5])):
            new_dict[p[5][i]] = values[i]
        data.append(new_dict)
    p[0]['data'] = data

def p_upsert_part(p):
    """ upsert_part : UPSERT INTO PARTITION STRING ON STRING "(" field_name_list ")" VALUES values_list
    """
    p[0] = dict()
    p[0]['type'] = 'upsert'
    p[0]['part_name'] = p[4]
    p[0]['coll_name'] = p[6]
    data = list()
    for values in p[11]:
        new_dict = dict()
        for i in range(0, len(p[8])):
            new_dict[p[8][i]] = values[i]
        data.append(new_dict)
    p[0]['data'] = data

def p_field_name_list(p):
    """ field_name_list : field_name field_name_list
                        | COMMA field_name field_name_list
                        | empty
    """
    p[0] = list()
    if len(p) == 3:
        p[0] = p[1] + p[2]
    elif len(p) == 4:
        p[0] = p[2] + p[3]

def p_field_name(p):
    """ field_name : STRING
                   | COUNT "(" "*" ")"
                   | "*"
    """
    if len(p) == 2:
        p[0] = [ p[1] ]
    elif len(p) == 5:
        p[0] = [ 'count(*)' ]

def p_values_list(p):
    """ values_list : value_tuple values_list
                    | COMMA value_tuple values_list
                    | empty
    """
    p[0] = list()
    if len(p) == 3:
        p[0] = p[1] + p[2]
    elif len(p) == 4:
        p[0] = p[2] + p[3]

def p_value_tuple(p):
    """ value_tuple : "(" value_list ")"
    """
    p[0] = [ p[2] ]

def p_value_list(p):
    """ value_list : value value_list
                   | COMMA value value_list
                   | empty
    """
    p[0] = list()
    if len(p) == 3:
        p[0] = p[1] + p[2]
    elif len(p) == 4:
        p[0] = p[2] + p[3]

def p_value(p):
    """ value : single_value
    """
    if len(p) == 2:
        p[0] = [ p[1] ]
    elif len(p) == 4:
        p[0] = [ p[2] ]
    
def p_json_value(p):
    """ json_value : kv_pair json_value
                   | COMMA kv_pair json_value
                   | empty
    """
    p[0] = dict()
    if len(p) == 3:
        p[0] = p[1] | p[2]
    elif len(p) == 4:
        p[0] = p[2] | p[3]

def p_kv_pair(p):
    """ kv_pair : QSTRING ":" value
    """
    p[0] = { p[1] : p[3][0] }

def p_single_value(p):
    """ single_value : NUMBER
                     | FLOAT
                     | QSTRING
                     | "{" json_value "}"
                     | "[" multi_value "]"
                     | BOOLEAN
                     | NULL
    """
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 4:
        p[0] = p[2]

def p_multi_value(p):
    """ multi_value : single_value multi_value
                    | COMMA single_value multi_value
                    | empty
    """
    p[0] = list()
    if len(p) == 3:
        p[0] = [p[1]] + p[2]
    elif len(p) == 4:
        p[0] = [p[2]] + p[3]

###################################################
############         Delete          ############
###################################################
def p_delete(p):
    """ delete : delete_coll
               | delete_part
    """
    p[0] = p[1]

def p_delete_coll(p):
    """ delete_coll : DELETE FROM STRING where
                    | DELETE FROM STRING WITH "{" QSTRING ":" QSTRING "}"
    """
    p[0] = dict()
    p[0]['type'] = 'delete'
    p[0]['coll_name'] = p[3]
    if len(p) == 5:
        p[0]['expr'] = p[4]['expr']
    elif len(p) == 10:
        p[0][p[6]] = p[8]

def p_delete_part(p):
    """ delete_part : DELETE FROM PARTITION STRING ON STRING where
                    | DELETE FROM PARTITION STRING ON STRING WITH "{" QSTRING ":" QSTRING "}"
    """
    p[0] = dict()
    p[0]['type'] = 'delete'
    p[0]['part_name'] = p[4]
    p[0]['coll_name'] = p[6]
    if len(p) == 8:
        p[0]['expr'] = p[7]['expr']
    elif len(p) == 13:
        p[0][p[9]] = p[11]

###################################################
############           Query           ############
###################################################
def p_query(p):
    """ query : query_coll
              | query_part
    """
    p[0] = p[1]

def p_query_coll(p):
    """ query_coll : SELECT field_name_list FROM STRING limit offset WITH "{" QSTRING ":" QSTRING "}"
                   | SELECT field_name_list FROM STRING limit offset where
    """
    p[0] = dict()
    p[0]['type'] = 'query'
    p[0]['fields'] = p[2]
    p[0]['coll_name'] = p[4]
    p[0]['limit'] = p[5]
    p[0]['offset'] = p[6]
    p[0]['parts'] = None
    if len(p) == 13:
        p[0][p[9]] = p[11]
    elif len(p) == 8:
        p[0]['expr'] = p[7]

def p_query_part(p):
    """ query_part : SELECT field_name_list FROM PARTITION part_name_list ON STRING limit offset WITH "{" QSTRING ":" QSTRING "}"
                   | SELECT field_name_list FROM PARTITION part_name_list ON STRING limit offset where
    """
    p[0] = dict()
    p[0]['type'] = 'query'
    p[0]['fields'] = p[2]
    p[0]['parts'] = p[5]
    p[0]['coll_name'] = p[7]
    p[0]['limit'] = p[8]
    p[0]['offset'] = p[9]
    if len(p) == 16:
        p[0][p[12]] = p[14]
    elif len(p) == 11:
        p[0]['expr'] = p[10]

def p_part_name_list(p):
    """ part_name_list : field_name_list
    """
    p[0] = p[1]

###################################################
############           Search          ############
###################################################
def p_search(p):
    """ search : search_coll
               | search_part
    """
    p[0] = p[1]

def p_search_coll(p):
    """ search_coll : SELECT field_name_list FROM STRING ORDER BY STRING "<" "-" ">" vec_list limit offset where WITH "{" search_param_list "}"
    """
    p[0] = dict()
    p[0]['type'] = 'search'
    p[0]['fields'] = p[2]
    p[0]['coll_name'] = p[4]
    p[0]['anns'] = p[7]
    p[0]['data'] = p[11]
    p[0]['limit'] = p[12]
    # p[0]['offset'] = p[13]
    p[0]['expr'] = p[14]
    p[0]['parts'] = None
    if 'expr' in p[17]:
        p[0]['expr'] = p[17]['expr']
        del p[17]['expr']

    new_param_list = dict()
    new_param_list['offset'] = p[13]
    new_param_list['metric_type'] = p[17].get('metric_type', None)
    if 'metric_type' in p[17]:
        del p[17]['metric_type']
    new_param_list['params'] = p[17]
    p[0]['param'] = new_param_list

def p_search_part(p):
    """ search_part : SELECT field_name_list FROM PARTITION part_name_list ON STRING ORDER BY STRING "<" "-" ">" vec_list limit offset where WITH "{" search_param_list "}"
    """
    p[0] = dict()
    p[0]['type'] = 'search'
    p[0]['fields'] = p[2]
    p[0]['coll_name'] = p[7]
    p[0]['anns'] = p[10]
    p[0]['data'] = p[14]
    p[0]['limit'] = p[15]
    # p[0]['offset'] = p[16]
    p[0]['expr'] = p[17]
    p[0]['parts'] = p[5]
    if 'expr' in p[20]:
        p[0]['expr'] = p[20]['expr']
        del p[20]['expr']

    new_param_list = dict()
    new_param_list['offset'] = p[16]
    new_param_list['metric_type'] = p[20].get('metric_type', None)
    if 'metric_type' in p[20]:
        del p[20]['metric_type']
    new_param_list['params'] = p[20]
    p[0]['param'] = new_param_list

def p_vec_list(p):
    """ vec_list : "[" value_list "]"
    """
    p[0] = p[2]

def p_search_param_list(p):
    """ search_param_list : coll_param_list
    """
    p[0] = p[1]

###################################################
############           Where           ############
###################################################
def p_where(p):
    """ where : WHERE conditions
              | empty
    """
    if len(p) == 2:
        p[0] = str()
    elif len(p) == 3:
        p[0] = p[2]



def p_conditions(p):
    """ conditions : NOT conditions
                   | conditions AND conditions
                   | conditions OR conditions
                   | "(" conditions ")"
                   | compare
                   | condition_function
    """
    bool_expr = None
    if len(p) == 2:
        # compare
        bool_expr = p[1]['expr']
    elif len(p) == 3:
        # not
        bool_expr = f'NOT {p[2]["expr"]}'
    elif len(p) == 4:
        if '(' in p:
            # brackets
            bool_expr = f'({p[2]["expr"]})'
        elif p[2].upper() == 'AND':
            bool_expr = f'({p[1]["expr"]}) AND ({p[3]["expr"]})'
        elif p[2].upper() == 'OR':
            bool_expr = f'({p[1]["expr"]}) OR ({p[3]["expr"]})'

    p[0] = {
        'expr': bool_expr
    }

def p_limit(p):
    """ limit : empty
              | LIMIT NUMBER
    """
    if len(p) == 2:
        p[0] = None
    elif len(p) == 3:
        p[0] = p[2]

def p_offset(p):
    """ offset : empty
               | OFFSET NUMBER
    """
    if len(p) == 2:
        p[0] = None
    elif len(p) == 3:
        p[0] = p[2]


def p_binary_arith_op(p):
    """ binary_arith_op : "*" "*"
                        | "+"
                        | "-"
                        | "*"
                        | "/"
                        | "%"
    """
    if len(p) == 3:
        # **
        p[0] = "**"
    elif len(p) == 2:
        p[0] = p[1]


def p_unary_arith_op(p):
    """ unary_arith_op : "+"
                       | "-"
    """
    p[0] = p[1]


def p_constant_expr(p):
    """ constant_expr : value
                      | constant_expr binary_arith_op constant_expr
                      | unary_arith_op constant_expr
                      | "(" constant_expr ")"
    """
    # 不在算术表达式中添加括号，直接将原表达式传入milvus，以此回避运算符优先级等问题
    if len(p) == 2:
        p[0] = json.dumps(p[1][0])
    elif len(p) == 3:
        # unary
        p[0] = f"{p[1]}{p[2]}"
    elif len(p) == 4:
        if "(" in p:
            # brackets
            p[0] = f"({p[2]})"
        else:
            # binary
            p[0] = f"{p[1]} {p[2]} {p[3]}"


def p_identifier(p):
    """ identifier : STRING
                   | identifier "[" NUMBER "]"
                   | identifier "[" QSTRING "]"
    """
    if len(p) == 2:
        # simple name
        p[0] = p[1]
    elif len(p) == 5:
        if isinstance(p[3], str):
            p[0] = f'{p[1]}["{p[3]}"]'
        else:
            # number
            p[0] = f'{p[1]}[{p[3]}]'


def p_comparable(p):
    """ comparable : identifier
                   | constant_expr
                   | ARRAY_LENGTH "(" identifier ")"
                   | "(" comparable ")"
    """
    if len(p) == 2:
        # column_id and value
        p[0] = p[1]
    elif len(p) == 4:
        # brackets
        p[0] = f"({p[2]})"
    elif len(p) == 5:
        # ARRAY_LENGTH(id)
        p[0] = f"ARRAY_LENGTH({p[3]})"


def p_compare(p):
    """ compare : comparable COMPARISON comparable
                | comparable ">" comparable
                | comparable "<" comparable
                | identifier like QSTRING
                | identifier BETWEEN value AND value
                | identifier NOT BETWEEN value AND value
                | identifier in "[" value_list "]"
    """
    bool_expr = None
    if len(p) == 4:
        # comparison, like

        if p[2] in ['LIKE', 'NOT LIKE']:
            # like
            bool_expr = f'({p[1]} LIKE "{p[3]}")'
            if p[2].startswith('NOT'):
                bool_expr = f'(NOT {bool_expr})'
        else:
            # comparison
            comp = p[2]
            if comp == '<>':
                comp = '!='
            elif comp == '=':
                comp = '=='
            bool_expr = f'({p[1]} {comp} {p[3]})'
    elif len(p) == 6:
        if p[2] in ['IN', 'NOT IN']:
            # in
            bool_expr = f'({p[1]} IN {p[4]})'
            if p[2].startswith('NOT'):
                bool_expr = f'(NOT {bool_expr})'
        else:
            # between and
            bool_expr = f'({p[3][0]} <= {p[1]} <= {p[5][0]})'
    elif len(p) == 7:
        # not between and
        bool_expr = f'(not ({p[4][0]} <= {p[1]} <= {p[6][0]}))'

    p[0] = {
        'expr': bool_expr
    }


def p_like(p):
    """ like : LIKE
             | NOT LIKE
    """
    if len(p) == 2:
        p[0] = 'LIKE'
    else:
        p[0] = 'NOT LIKE'


def p_in(p):
    """ in : IN
           | NOT IN
    """
    if len(p) == 2:
        p[0] = 'IN'
    else:
        p[0] = 'NOT IN'


def p_condition_function(p):
    """ condition_function : condition_function_def "(" identifier COMMA value ")"
    """
    p[0] = dict()
    if len(p) == 7:
        json_value = json.dumps(p[5][0])
        p[0]['func_name'] = p[1]
        p[0]['expr'] = f'({p[1]}({p[3]}, {json_value}))'
    else:
        p[0]['func_name'] = p[1]
        p[0]['expr'] = f'({p[1]}({p[3]}))'


def p_condition_function_def(p):
    """ condition_function_def : JSON_CONTAINS
                               | JSON_CONTAINS_ALL
                               | JSON_CONTAINS_ANY
                               | ARRAY_CONTAINS
                               | ARRAY_CONTAINS_ALL
                               | ARRAY_CONTAINS_ANY
    """
    p[0] = p[1]


# empty return None
# so expression like (t : empty) => len(p)==2
def p_empty(p):
    """empty :"""
    pass


def p_error(p):
    raise GrammarException("Syntax error in input!")


tokens = lexer.tokens

DEBUG = False

L = lex.lex(module=lexer, optimize=False, debug=DEBUG)
P = yacc.yacc(debug=DEBUG)


def parse_handle(sql):
    return P.parse(input=sql,lexer=L,debug=DEBUG)





