# -*- coding: utf-8 -*-

from ply import lex,yacc

from . import lexer
from .exceptions import GrammarException

def p_expression(p):
    """ expression : db END
                   | coll END
                   | part END
                   | idx END
                   | insert END
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
    """ create_coll : COLLECTION COLLECTION STRING "(" field_list ")" WITH "{" coll_param_list "}"
    """
    # 注意细心检查是不是每个数据类型都可以建，每个数据类型下的参数(如primary key)是不是都可以设置，collection的各个参数是不是都可以设置。
    p[0] = {
        'type' : 'create_coll',
        'name' : p[3],
        'fields' : p[5],
        'params' : p[9]
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
    # str_to_dtype['BOOL'] = DataType.BOOL
    # str_to_dtype['INT8'] = DataType.INT8
    # str_to_dtype['INT16'] = DataType.INT16
    # str_to_dtype['INT32'] = DataType.INT32
    # str_to_dtype['INT64'] = DataType.INT64
    # str_to_dtype['FLOAT'] = DataType.FLOAT
    # str_to_dtype['DOUBLE'] = DataType.DOUBLE
    # str_to_dtype['VARCHAR'] = DataType.VARCHAR
    # str_to_dtype['JSON'] = DataType.JSON
    # str_to_dtype['ARRAY'] = DataType.ARRAY
    # str_to_dtype['BINARY_VECTOR'] = DataType.BINARY_VECTOR
    # str_to_dtype['FLOAT_VECTOR'] = DataType.FLOAT_VECTOR
    # 注意看str_to_dtype的key，这些key是p[0]['type']的值
    # STRING "(" NUMBER ")" 对应 VARCHAR(2) 这种
    # STRING STRING "(" NUMBER ")" 对应 BINARY VECTOR （2）， FLOAT VECTOR （2），和BOOL ARRAY（2）这种，注意当ARRAY的元素为VARCHAR时需要另一种格式
    # STRING "(" NUMBER ")" STRING "(" NUMBER ")" 当ARRAY的元素为VARCHAR，例子为VARCHAR（2）ARRAY（2）
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

def attr_list(p): 
    """ attr_list : empty
                  | attr attr_list
    """
    if len(p) == 2:
        p[0] = dict()
    elif len(p) == 3:
        p[0] = p[1] | p[2]
        
def attr(p): 
    """ attr : STRING STRING
             | STRING
             | STRING "(" QSTRING ")"
    """
    # 例子：VARCHAR(2) PRIMARY KEY AUTO ID DESCRIPTION('test')，这里PRIMARY KEY、AUTO ID和DESCRIPTION的次序必须是可以随机排列的（需要检查一下）
    # default value这个是不用支持的
    # 这里attr有5个，is_dynamic、is_primary、is_partition_key、auto_id、description，记得认真检查下，是不是每个attr都可以实现
    # Primary key和Partition key只能选一个，想设auto id必须先设置为primary key
    p[0] = dict()
    if len(p) == 2: 
        if p[1].upper() == 'DYNAMIC':
            p[0]['is_dynamic'] = True
    elif len(p) == 3:
        if p[1].upper() == 'PRIMARY' and p[2].upper() == 'KEY':
            p[0]['is_primary'] = True
        elif p[1].upper() == 'PARTITION' and p[2].upper() == 'KEY':
            p[0]['is_partition_key'] = True
        elif p[1].upper() == 'AUTO' and p[2].upper() == 'ID':
            p[0]['auto_id'] = True
    elif len(p) == 5:
        if p[1].upper() == 'DESCRIPTION':
            p[0]['description'] = p[3]
            
def coll_param_list(p):
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
    # 这个见README里建立Collection处的param_list
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
    """ idx_param_list : idx_param idx_param_list
                       | COMMA idx_param idx_param_list
                       | empty
    """
    p[0] = dict()
    if len(p) == 3:
        p[0] = p[1] | p[2]
    elif len(p) == 4:
        p[0] = p[2] | p[3]
    
def p_idx_param(p):
    """ idx_param : QSTRING ":" QSTRING
                  | QSTRING ":" NUMBER
    """
    p[0] = dict()
    if len(p) > 2:
        p[0][p[1]] = p[3]

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
            
'''
def p_expression(p):
    """ expression : dml END
                   | ddl END
    """
    p[0] = p[1]

def p_dml(p):
    """ dml : select
            | update
            | insert
            | delete
    """
    p[0] = p[1]


def p_ddl(p):
    """ ddl : create
            | alter
            | drop
    """
    p[0] = p[1]

###################################################
############         select            ############
###################################################
def p_select(p):
    """ select : SELECT columns FROM table join where group_by having order_by limit
    """
    p[0] = {
        'type'  : p[1],
        'column': p[2],
        'table' : p[4],
        'join'  : p[5],
        'where' : p[6],
        'group' : p[7],
        'having': p[8],
        'order' : p[9],
        'limit' : p[10]
    }

def p_table(p):
    """ table : table COMMA table
              | STRING AS STRING
              | STRING STRING
              | STRING
    """
    if ',' in p:
        p[0] = p[1] + p[3]
    else:
        if len(p) == 2:
            p[0] = [{'name':p[1]}]
        if len(p) == 3:
            p[0] = [{'name':p[1],'alias':p[2]}]
        if len(p) == 4:
            p[0] = [{'name':p[1],'alias':p[3]}]


def p_join(p):
    """ join : INNER JOIN table on join
             | LEFT JOIN table on join
             | RIGHT JOIN table on join
             | FULL JOIN table on join
             | JOIN table on join
             | empty
    """
    p[0] = []
    if len(p) == 5:
        p[0] = [{'type':'INNER','table':p[2],'on':p[3]}]+p[4]
    if len(p) == 6:
        p[0] = [{'type':p[1],'table':p[3],'on':p[4]}]+p[5]

def p_on(p):
    """ on : ON item COMPARISON item
    """
    p[0] = [p[2],p[4]]

def p_where(p):
    """ where : WHERE conditions
              | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]

def p_group_by(p):
    """ group_by : GROUP BY strings
                 | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[3]

def p_having(p):
    """ having : HAVING conditions
               | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]

def p_order_by(p):
    """ order_by : ORDER BY order
                 | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[3]


def p_limit(p):
    """ limit : LIMIT numbers
              | empty
    """
    p[0] = []
    if len(p) > 2:
        if len(p[2]) == 1:
            p[2] = [0] + p[2]
        p[0] = p[2]

def p_order(p):
    """ order : order COMMA order
              | string order_type
    """
    if len(p) > 3:
        p[0] = p[1] + p[3]
    else:
        p[0] = [{'name': p[1],'type': p[2]}]

def p_order_type(p):
    """ order_type : ASC
                   | DESC
                   | empty
    """
    if p[1] == 'DESC':
        p[0] = 'DESC'
    else:
        p[0] = 'ASC'


###################################################
############         update            ############
###################################################
def p_update(p):
    """ update : UPDATE table SET set where
    """
    p[0] = {
        'type'  : p[1],
        'table': p[2],
        'column'  : p[4],
        'where' : p[5]
    }

def p_set(p):
    """ set : set COMMA set
            | item COMPARISON item
    """
    if '=' in p:
        p[0] = [{'name':p[1],'value':p[3]}]
    else:
        p[0] = p[1] + p[3]

###################################################
############         insert            ############
###################################################
def p_insert(p):
    """ insert : INSERT into table insert_columns VALUES values
    """
    p[0] = {
        'type': p[1],
        'table': p[3],
        'columns': p[4],
        'values': p[6]
    }

def p_into(p):
    """ into : INTO
             | empty
    """
    pass

def p_insert_columns(p):
    """ insert_columns : "(" columns ")"
                       | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]

def p_value(p):
    """ value : value COMMA value
              | string
              | NUMBER
    """
    if len(p) > 2:
        p[0] = p[1] + p[3]
    else:
        p[0] = [p[1]]

def p_values(p):
    """ values : values COMMA values
               | "(" value ")"
    """
    if ',' in p:
        p[0] = p[1] + p[3]
    else:
        p[0] = [p[2]]

###################################################
############         delete            ############
###################################################
def p_delete(p):
    """ delete : DELETE FROM table where
    """
    p[0] = {
        'type': p[1],
        'table': p[3],
        'where': p[4]
    }

###################################################
############         create            ############
###################################################
def p_create(p):
    """ create : CREATE TABLE string "(" create_columns ")"
    """
    p[0] = {
        'type': p[1],
        'table': p[3],
        'columns': p[5]
    }


def p_create_columns(p):
    """ create_columns : create_columns COMMA create_columns
                       | string datatype
    """
    if len(p) > 3:
        p[0] = p[1] + p[3]
    else:
        p[0] = [{'name':p[1],'type':p[2]}]

def p_datatype(p):
    """ datatype : INT
                 | INTEGER
                 | TINYINT
                 | SMALLINT
                 | MEDIUMINT
                 | BIGINT
                 | FLOAT
                 | DOUBLE
                 | DECIMAL
                 | CHAR "(" NUMBER ")"
                 | VARCHAR "(" NUMBER ")"
    """
    if len(p) > 2:
        p[0] = '%s(%s)'%(p[1],p[3])
    else:
        p[0] = p[1]

###################################################
############         alter              ###########
###################################################
def p_alter(p):
    """ alter : ALTER TABLE string change_column
    """
    p[0] = {
        'type': p[1],
        'table': p[3],
        'columns': p[4]
    }

def p_change_column(p):
    """ change_column : ADD string datatype
                      | DROP COLUMN string
                      | ALTER COLUMN string datatype
    """
    if p[1] == 'ADD':
        p[0] = {'ADD':{'name':p[2],'type':p[3]}}
    if p[1] == 'DROP':
        p[0] = {'DROP': {'name': p[2]}}
    if p[1] == 'ALTER':
        p[0] = {'ALTER': {'name': p[3],'type':p[4]}}

###################################################
############         drop              ############
###################################################
def p_drop(p):
    """ drop : DROP TABLE string
    """
    p[0] = {
        'type': p[1],
        'table': p[3]
    }

###################################################
############         column            ############
###################################################
# p[0] => [x,x..] | [x]
def p_columns(p):
    """ columns : columns COMMA columns
                | column_as
                | column
    """

    if len(p) > 2:
        p[0] = p[1] + p[3]
    else:
        p[0] = [p[1]]

def p_column_as(p):
    """ column_as : column AS item
                  | column item
    """
    p[0] = p[1]
    if len(p) > 3:
        p[0]['alias'] = p[3]
    else:
        p[0]['alias'] = p[2]

def p_column(p):
    """ column : function "(" distinct_item ")"
               | function "(" item ")"
               | distinct_item
               | item
    """
    if len(p) > 2:
        p[0] = {'name': {p[1]:p[3]}}
    else:
        p[0] = {'name':p[1]}

def p_distinct_item(p):
    """ distinct_item : DISTINCT item
                      | DISTINCT "(" item ")"
    """
    if len(p) > 3:
        p[0] = {p[1]:p[3]}
    else:
        p[0] = {p[1]:p[2]}

def p_function(p):
    """ function : COUNT
                 | SUM
                 | AVG
                 | MIN
                 | MAX
    """
    p[0] = p[1]

def p_item(p):
    """ item : string
             | NUMBER
             | "*"
             | string "." item
    """
    if len(p)>2:
        p[0] = p[1]+'.'+p[3]
    else:
        p[0] = p[1]


# p[0] => [1,2] | [1]
def p_numbers(p):
    """ numbers : numbers COMMA numbers
                | NUMBER
    """
    if len(p) > 2:
        p[0] = p[1] + p[3]
    else:
        p[0] = [p[1]]

def p_strings(p):
    """ strings : strings COMMA strings
                | string
    """
    if len(p) > 2:
        p[0] = p[1] + p[3]
    else:
        p[0] = [p[1]]

def p_items(p):
    """ items : strings
              | numbers
    """
    p[0] = p[1]


def p_string(p):
    """ string : STRING
               | QSTRING
    """
    p[0] = p[1]


def p_conditions(p):
    """ conditions : conditions AND conditions
                   | conditions OR conditions
                   | "(" conditions ")"
                   | compare
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        if '(' in p:
            p[0] = [p[2]]
        else:
            p[0] = p[1] + [p[2]] + p[3]

def p_compare(p):
    """ compare : column COMPARISON item
                | column like QSTRING
                | column BETWEEN item AND item
                | column IS null
                | column in lritems
    """
    if len(p) == 4:
        p[0] = {
            'name' : p[1]['name'],
            'value': p[3],
            'compare' : p[2]
        }
    if len(p) == 6:
        p[0] = {
            'name': p[1]['name'],
            'value': [p[3],p[5]],
            'compare': p[2]
        }

def p_lritems(p):
    """ lritems : "(" items ")"
    """
    p[0] = p[2]

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

def p_null(p):
    """ null : NULL
             | NOT NULL
    """
    if len(p) == 2:
        p[0] = 'NULL'
    else:
        p[0] = 'NOT NULL'
'''

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





