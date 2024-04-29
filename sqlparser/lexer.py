# -*- coding: utf-8 -*-

from .exceptions import LexerException

reserved = {
    'select': 'SELECT',
    'from'  : 'FROM',
    'where' : 'WHERE',
    'on'    : 'ON',
    'by'    : 'BY',
    'order' : 'ORDER',
    'limit' : 'LIMIT',
    'offset': 'OFFSET',
    'bulk'  : 'BULK',
    'insert': 'INSERT',
    'upsert': 'UPSERT',
    'into'  : 'INTO',
    'values': 'VALUES',
    'delete': 'DELETE',

    'create': 'CREATE',
    'collection' : 'COLLECTION',
    'collections' : 'COLLECTIONS',
    'partition' : 'PARTITION',
    'partitions' : 'PARTITIONS',
    'alias' : 'ALIAS',
    'aliases' : 'ALIASES',
    'index' : 'INDEX',
    'indexes': 'INDEXES',
    'use'   : 'USE',
    'database': 'DATABASE',
    'databases': 'DATABASES',
    'drop'  : 'DROP',
    'show'  : 'SHOW',
    'rename': 'RENAME',
    'load'  : 'LOAD',
    'release': 'RELEASE',
    'compact': 'COMPACT',

    'primary': 'PRIMARY',
    'key'    : 'KEY',
    'auto'   : 'AUTO',
    'id'     : 'ID',
    'description' : 'DESCRIPTION',

    'and'   : 'AND',
    'for'   : 'FOR',
    'to'    : 'TO',
    'or'    : 'OR',
    'in'    : 'IN',
    'on'    : 'ON',
    'with'  : 'WITH',
    'from'  : 'FROM',
    'like'  : 'LIKE',
    'between': 'BETWEEN',
    'not'   : 'NOT',
    'count' : 'COUNT',

}

tokens = (
    'COMPARISON',
    'BOOLEAN',
    'NULL',
    'STRING',
    'NUMBER',
    'QSTRING',
    'FLOAT',
    'END',
    'COMMA',
) + tuple(set(reserved.values()))

# 原来的<和>移到literals里了，COMPARISON里少了这两个符号，写条件时需要单独做下判断
literals = '()<>{}@%.*[]:-^'
t_COMPARISON = r'<>|!=|>=|<=|='
t_END = r';'
t_COMMA = r','
t_ignore = ' \t\n'


def t_BOOLEAN(t):
    r"(true|false)"
    if t.value.upper() == "TRUE":
        t.value = True
    else:
        t.value = False
    return t


def t_NULL(t):
    r"(null)"
    t.value = None
    return t


def t_STRING(t):
    r"[a-zA-Z][_a-zA-Z0-9]*"
    t.type = reserved.get(t.value.lower(), 'STRING')
    if t.type != 'STRING':
        t.value = t.value.upper()
    return t

def t_QSTRING(t):
    r"('[^']*')|(\"[^\"]*\")|(`[^`]*`)"
    t.value = t.value[1:-1]
    return t

def t_FLOAT(t):
    # r"(-?\d+)(\.\d+)?"
    r"[-+]?[0-9]+(\.([0-9]+)?([eE][-+]?[0-9]+)?|[eE][-+]?[0-9]+)"
    t.value = float(t.value)
    return t


def t_NUMBER(t):
    r"(0|-?[1-9]\d*)"
    t.value = int(t.value)
    return t


def t_error(t):
    raise LexerException("Illegal character '%s' at line %s pos %s"
                         % (t.value[0], t.lineno, t.lexpos))
