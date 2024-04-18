from caller.call_conn import *
from caller.call_db import *
from caller.call_coll import *
from caller.call_alias import *
from caller.call_part import *
from caller.call_idx import *
from caller.call_insert import *

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
    'rename_coll' : rename_coll,
    'load_coll' : load_coll,
    'release_coll' : release_coll,
    'compact_coll' : compact_coll,
    'create_part' : create_part,
    'show_part' : show_part,
    'drop_part' : drop_part,
    'load_part' : load_part,
    'release_part' : release_part,
    'create_idx' : create_idx,
    'show_idx' : show_idx,
    'drop_idx' : drop_idx,
    'bulk_insert' : bulk_insert
}