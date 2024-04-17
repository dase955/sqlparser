# sqlparser
milvus sql parser

## sql example

### Manage Databases

 - CREATE DATABASE {name}

 - USE {name}

 - SHOW DATABASES

 - DROP DATABASE {name}

### Manage Collections

 - create collection collection_name (name type other_attrs, ...) with (dynamic_field = true/false, shards_num=1[, description='...']);  TODO

 - RENAME COLLECTION {old_coll_name} TO {new_coll_name} IN {new_db_name}; 可以用来将collection在两个database里移动

 - SHOW COLLECTIONS; 

 - DROP COLLECTION {name}; 

 - CREATE ALIAS {alias_name} FOR {coll_name};

 - DROP ALIAS {alias_name} FOR {coll_name};

 - SHOW ALIASES FOR {coll_name};

 - LOAD COLLECTION {name} [WITH {"replica_number":2}];

 - RELEASE COLLECTION {name};

 - COMPACT COLLECTION {name};

### Manage Partitions

 - CREATE PARTITION {part_name} ON {coll_name} [WITH {"description":"..."}];

 - SHOW PARTITIONS ON {coll_name};

 - DROP PARTITION {part_name} ON {coll_name};

 - LOAD PARTITION {part_name_1, part_name_2, ...} ON {coll_name} [WITH {"replica_number":2}];
 
 - RELEASE PARTITION {part_name_1, part_name_2, ...} ON {coll_name};