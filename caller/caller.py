from caller.call_conn import *
from caller.call_db import *
from caller.call_coll import *
from caller.call_alias import *

func_map = {
    'connect' : connect,
    'disconnect' : disconnect,
    'create_db' : create_db,
    'use_db' : use_db,
    'show_db' : show_db,
    'drop_db' : drop_db,
    'show_coll' : show_coll,
    'drop_coll' : drop_coll,
    'create_alias' : create_alias,
    'drop_alias' : drop_alias,
    'show_alias' : show_alias,
    'rename_coll' : rename_coll
}