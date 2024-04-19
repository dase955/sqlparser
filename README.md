# SQLParser

## 介绍

该库由Python3实现，用来实现*输入类似SQL格式的命令行来对Milvus进行操作*这一功能，基于Milvus 2.3.x。

其库可以分为以下两个部分：

 - sqlparser：使用模块ply对输入的SQL命令行进行解析，将结果保存到一个字典query里

 - caller：输入上述得到的字典query，调用其对应的（在模块pymilvus实现的）接口，对milvus进行操作。字典query与接口的映射关系见caller中的func_map

另外，本库一次仅支持建立一个Milvus Connection，启动时的相关配置见config.ini。仍需要对config.ini进行注释，TODO。

## SQL语法

### 对Milvus Database的管理

#### 创建一个Database

```
CREATE DATABASE {name};
```

参数解释如下：

 - name：创建的Database的命名，取值类型为字符串，且不可包含单/双引号

#### 切换到一个Database

启动时一次只能对一个Database进行操作，如果需要对另一个Database进行操作，需要切换Database。

```
USE {name};
```
参数解释如下：

 - name：切换到的Database的命名，取值类型为字符串，且不可包含单/双引号

#### 展示已有的Database

仅展示Database的命名
```
SHOW DATABASES;
```

#### 删除一个Database

谨慎删除Database，删除操作不可逆。

```
DROP DATABASE {name}
```

参数解释如下：

 - name：需删除的Database的命名，取值类型为字符串，且不可包含单/双引号

### 对Milvus Collection的管理

#### 创建一个Collection
TODO
```
create collection collection_name (name type other_attrs, ...) with (dynamic_field = true/false, shards_num=1[, description='...']);
```

#### 重命名一个Collection

可以用于将一个collection移动到另一个database，或用于重命名。

```
RENAME COLLECTION {old_coll_name} TO {new_coll_name} IN {new_db_name}; 
```

参数解释如下：

 - old_coll_name：需重命名的collection的原有命名，取值类型为字符串，且不可包含单/双引号

 - new_coll_name：需重命名的collection修改后的命名，可以与old_coll_name重复，取值类型为字符串，且不可包含单/双引号

 - new_db_name：重命名后将这个collection移动到的database的命名，可以与原database相同，取值类型为字符串，且不可包含单/双引号

#### 展示已有的Collection

展示当前Database里的Collection的命名

```
SHOW COLLECTIONS; 
```

#### 删除一个Collection

谨慎删除Collection，删除操作不可逆。

```
DROP COLLECTION {name}; 
```

参数解释如下：

 - name：需删除的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 为一个Collection创建一个别名

创建的别名（alias）可以在其他命令里代替这个collection的命名。

```
CREATE ALIAS {alias_name} FOR {coll_name};
```

参数解释如下：

 - alias_name：创建的alias的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 为一个Collection删除一个别名

```
DROP ALIAS {alias_name} FOR {coll_name};
```

参数解释如下：

 - alias_name：删除的alias命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 展示一个Collection的所有别名

```
SHOW ALIASES FOR {coll_name};
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 将一个Collection载入内存

在查询前，将collection载入内存，replica_number为在内存的备份数量。

```
# 默认replica_number为1
LOAD COLLECTION {name};
# 或者
LOAD COLLECTION {name} WITH {"replica_number":2};
# Example：LOAD COLLECTION coll WITH {"replica_number":2};
```

参数解释如下：

 - name：需载入的collection的命名，取值类型为字符串，且不可包含单/双引号

 - replica_number：载入内存的备份数量，需大于0，且为整数

#### 将载入内存的一个Collection占用的内存释放

```
RELEASE COLLECTION {name};
```

参数解释如下：

 - name：需释放的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 压缩一个Collection里的数据

milvus默认是自动压缩，也可以使用该命令手动压缩。

```
COMPACT COLLECTION {name};
```

参数解释如下：

 - name：需压缩的collection的命名，取值类型为字符串，且不可包含单/双引号

### 对Milvus Partition的管理

#### 为一个Collection创建一个分区

milvus支持将一个Collection分成多个小的分区（Partition），进行更细致的划分，这样在载入内存和查询时只对一个分区里的数据进行操作。

```
CREATE PARTITION {part_name} ON {coll_name};
# 或者
CREATE PARTITION {part_name} ON {coll_name} WITH {"description":"..."};
# Example：CREATE PARTITION part1 ON coll WITH {"description":"text"};
```

参数解释如下：

 - part_name：创建的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - description：创建的分区的描述文本，其值为字符串，需包含在一对单/双引号里

#### 展示一个Collection的分区

仅展示分区的命名。

```
SHOW PARTITIONS ON {coll_name};
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 删除一个Collection的一个分区

谨慎删除分区。

```
DROP PARTITION {part_name} ON {coll_name};
```

参数解释如下：

 - part_name：需删除的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 将一个Collection的多个分区载入内存 TODO

```
LOAD PARTITION {part_name_list} ON {coll_name};
# 或者
LOAD PARTITION {part_name_list} ON {coll_name} WITH {"replica_number":2};
# Example：LOAD PARTITION part1, part2 ON coll WITH {"replica_number":2};
```

参数解释如下：

 - part_name_list：part_name的列表，其中part_name为需载入的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - replica_number：每个分区载入内存的备份数量，需大于0，且为整数

#### 将一个Collection下的（载入内存的）多个分区占用的内存释放

```
RELEASE PARTITION {part_name_list} ON {coll_name};
# Example：RELEASE PARTITION part1, part2 ON coll; 
```

参数解释如下：

 - part_name_list：part_name的列表，其中part_name为需释放的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

### 对一个Collection的索引的管理

#### 对一个Collection建立一个向量索引

```
CREATE INDEX {idx_name} ON {coll_name}({field_name}) WITH {param_list};
# Example：CREATE INDEX idx ON coll(field1) WITH {"index_type":"IVF_FLAT","metric_type":"L2","nlist":1024};
```

参数解释如下：

 - idx_name：需建立的索引的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - field_name：操作的field的命名，取值类型为字符串，且不可包含单/双引号

 - param_list：param的列表，其类型如下，具体取值见Milvus文档：

    - index_type：取值类型为字符串，需包含在一对单/双引号里

    - metric_type：取值类型为字符串，需包含在一对单/双引号里

    - nlist：取值类型为正整数，需包含在一对单/双引号里

    - others：其他参数，如果为布尔值，True需改为1，False需改为0

#### 对一个Collection建立一个标量索引

```
CREATE INDEX {idx_name} ON {coll_name}({field_name});
# Example：CREATE INDEX idx ON coll(field1);
```

参数解释如下：

 - idx_name：需建立的索引的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - field_name：操作的field的命名，取值类型为字符串，且不可包含单/双引号

#### 删除一个Collection的一个（向量/标量）索引

```
DROP INDEX {idx_name} ON {coll_name};
```

参数解释如下：

 - idx_name：需删除的索引的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

#### 展示一个Collection的所有（向量/标量）索引

```
SHOW INDEXES ON {coll_name};
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

### 对一个Collection的数据的管理

#### 批量插入数据到这个Collection的一个分区

```
BULK INSERT PARTITION {part_name} ON {coll_name} FROM {file_list};
# Example：BULK INSERT PARTITION part1 ON coll FROM "1.json","2.json";
```

参数解释如下：

 - part_name：需载入的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - file_list：file的列表，file是外部Json文件的命名，其值为字符串，需包含在一对单/双引号里

#### 批量插入数据到这个Collection

```
BULK INSERT COLLECTION {coll_name} FROM {file_list};
# Example：BULK INSERT COLLECTION coll FROM "1.json","2.json";
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - file_list：file的列表，file是外部Json文件的命名，其值为字符串，需包含在一对单/双引号里