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

#### 创建一个Collection (TODO)

创建一个Collection，每个参数的具体含义见milvus文档

```
CREATE COLLECTION {coll_name} ({field_list}) with {param_list};
# or
CREATE COLLECTION {coll_name} ({field_list})
```

参数解释如下：

 - coll_name：新的collection的命名，取值类型为字符串，且不可包含单/双引号

 - field_list：field的列表，field之间用逗号分割，每个field格式如下：
   
   `{field_name} {field_type} {attr1} {attr2} ...`

   - field_name: field的命名，取值类型为字符串，且不可包含单/双引号
   - field_type: field的类型，其可取的值如下：
     - `VARCHAR(长度)`, 长度取值为 $[1, 65535]$ 内的整数。
     - `BOOL`
     - `INT8`
     - `INT16`
     - `INT32`
     - `INT64`
     - `FLOAT`
     - `DOUBLE`
     - `JSON`
     - `BINARY VECTOR(dim)`, dim取值为 $[1, 32768]$ 内的整数。
     - `FLOAT VECTOR(dim)`, dim取值为 $[1, 32768]$ 内的整数。
     - ARRAY: 数组，需要指定其元素类型，形式为 `{元素类型} ARRAY` ，有效元素类型包括：
       - `INT8`
       - `INT16`
       - `INT32`
       - `INT64`
       - `VARCHAR(长度)`
       - `BOOL`
       - `FLOAT`
       - `DOUBLE`
       
       例: `VARCHAR(32) ARRAY`，`INT32 ARRAY`。
     
     可作为主键或分区键的类型如下: 
       - `INT64`
       - `VARCHAR`
   
   - attr: field的属性，可取值如下:
     - `primary key`: 主键
     - `partition key`: 分区键
     - `auto id`: 为主键自动分配id
     - description: 格式为 `description("描述")` ，作为该 field 的描述

 - param_list：param的列表，param可取的项如下。

   - description：创建的Collection的描述文本，其值为字符串，需包含在一对单/双引号里

   - enable_dynamic_field：可取1或0，1代表True, 0代表False

   - num_shards：取值为1到16里的整数

   - num_partitions: 取值为 $[1, 4096]$ 里的整数

   - primary_field: 主键名称

   - partition_key_field: 分区键名称

 例：` create collection TEST_COLLECTION (field_1 varchar(22222) auto id primary key description("field 1"),
                                         field_2 binary vector(32) description("field 2"),
                                         field_3 BOOL description("field 3"),
                                         field_4 varchar(63333) array(1234) description("field 4"),
                                         field_5 int64 partition key description("field 5"))
                with {'num_shards': 2}; `

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

#### 将一个Collection的多个分区载入内存

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

#### 从文件中插入数据到这个Collection的一个分区

```
BULK INSERT PARTITION {part_name} ON {coll_name} FROM {file_list};
# Example：BULK INSERT PARTITION part1 ON coll FROM "1.json","2.json";
```

参数解释如下：

 - part_name：需载入的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - file_list：file的列表，file是外部Json文件的命名，其值为字符串，需包含在一对单/双引号里

#### 从文件中插入数据到这个Collection

```
BULK INSERT COLLECTION {coll_name} FROM {file_list};
# Example：BULK INSERT COLLECTION coll FROM "1.json","2.json";
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - file_list：file的列表，file是外部Json文件的命名，其值为字符串，需包含在一对单/双引号里

#### 插入数据到这个Collection

```
INSERT INTO {coll_name}({field_name_list}) VALUES ({value_list_1}),({value_list_2}),...;
# Example 1: INSERT INTO book(book_id, book_intro) VALUES (1, [1.0, 2.0]), (2, [3.0, 2.0]);
# Example 2: INSERT INTO book(book_id, book_intro) VALUES (3, [1.0, 3.0]);
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - field_name_list：field命名的列表，其field命名取值类型为字符串，且不可包含单/双引号。需注意，如果启用dynamic_field，这里的field命名可以不存在Collection原来的fields里。启用dynamic_field插入的新的field只能为标量field，不可以为binary_vector和float_vector类型。

 - value_list：value的列表，value的取值根据field的类型有所不同，且不支持NULL值，举例见下。

   - BOOL：1/0，1表示True，0表示False

   - INT8 / INT16 / INT32 / INT64：222，取正负整数即可

   - FLOAT / DOUBLE：1.0，正负浮点数即可

   - VARCHAR："a string 2"，'a string 2'，一个包含在单/双引号里的字符串即可

   - ARRAY：由element type决定，比如BOOL ARRAY可以为[1, 0, ...]，统一的格式为[item1, item2, ...]

   - BINARY VECTOR：[1, 0, 1, 1, ...]，其中的元素只能为0或1

   - FLOAT VECTOR：[1.0, 0.3, -1.2, ...]，其中的元素为正负浮动数

   - JSON：不支持JSON类型，可以使用bulk insert来插入

#### 插入数据到这个Collection的一个分区

```
INSERT INTO PARTITION {part_name} ON {coll_name}({field_name_list}) VALUES ({value_list_1}),({value_list_2}),...;
# Example 1: INSERT INTO PARTITION part1 ON book(book_id, book_intro) VALUES (1, [1.0, 2.0]), (2, [3.0, 2.0]);
# Example 2: INSERT INTO PARTITION part2 ON book(book_id, book_intro) VALUES (3, [1.0, 3.0]);
```

参数解释如下：

 - part_name：操作的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - field_name_list：field命名的列表，其field命名取值类型为字符串，且不可包含单/双引号。需注意，如果启用dynamic_field，这里的field命名可以不存在Collection原来的fields里。启用dynamic_field插入的新的field只能为标量field，不可以为binary_vector和float_vector类型。

 - value_list：value的列表，value的取值根据field的类型有所不同，且不支持NULL值，举例见下。

   - BOOL：1/0，1表示True，0表示False

   - INT8 / INT16 / INT32 / INT64：222，取正负整数即可

   - FLOAT / DOUBLE：1.0，正负浮点数即可

   - VARCHAR："a string 2"，'a string 2'，一个包含在单/双引号里的字符串即可

   - ARRAY：由element type决定，比如BOOL ARRAY可以为[1, 0, ...]，统一的格式为[item1, item2, ...]

   - BINARY VECTOR：[1, 0, 1, 1, ...]，其中的元素只能为0或1

   - FLOAT VECTOR：[1.0, 0.3, -1.2, ...]，其中的元素为正负浮动数

   - JSON：不支持JSON类型，可以使用bulk insert来插入

#### 插入更新数据到这个Collection

```
UPSERT INTO {coll_name}({field_name_list}) VALUES ({value_list_1}),({value_list_2}),...;
# Example 1: UPSERT INTO book(book_id, book_intro) VALUES (1, [1.0, 2.0]), (2, [3.0, 2.0]);
# Example 2: UPSERT INTO book(book_id, book_intro) VALUES (3, [1.0, 3.0]);
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - field_name_list：field命名的列表，其field命名取值类型为字符串，且不可包含单/双引号。需注意，如果启用dynamic_field，这里的field命名可以不存在Collection原来的fields里。启用dynamic_field插入的新的field只能为标量field，不可以为binary_vector和float_vector类型。

 - value_list：value的列表，value的取值根据field的类型有所不同，且不支持NULL值，举例见下。

   - BOOL：1/0，1表示True，0表示False

   - INT8 / INT16 / INT32 / INT64：222，取正负整数即可

   - FLOAT / DOUBLE：1.0，正负浮点数即可

   - VARCHAR："a string 2"，'a string 2'，一个包含在单/双引号里的字符串即可

   - ARRAY：由element type决定，比如BOOL ARRAY可以为[1, 0, ...]，统一的格式为[item1, item2, ...]

   - BINARY VECTOR：[1, 0, 1, 1, ...]，其中的元素只能为0或1

   - FLOAT VECTOR：[1.0, 0.3, -1.2, ...]，其中的元素为正负浮动数

   - JSON：不支持JSON类型，可以使用bulk insert来插入

#### 插入更新数据到这个Collection的一个分区

```
UPSERT INTO PARTITION {part_name} ON {coll_name}({field_name_list}) VALUES ({value_list_1}),({value_list_2}),...;
# Example 1: UPSERT INTO PARTITION part1 ON book(book_id, book_intro) VALUES (1, [1.0, 2.0]), (2, [3.0, 2.0]);
# Example 2: UPSERT INTO PARTITION part2 ON book(book_id, book_intro) VALUES (3, [1.0, 3.0]);
```

参数解释如下：

 - part_name：操作的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - field_name_list：field命名的列表，其field命名取值类型为字符串，且不可包含单/双引号。需注意，如果启用dynamic_field，这里的field命名可以不存在Collection原来的fields里。启用dynamic_field插入的新的field只能为标量field，不可以为binary_vector和float_vector类型。

 - value_list：value的列表，value的取值根据field的类型有所不同，且不支持NULL值，举例见下。

   - BOOL：1/0，1表示True，0表示False

   - INT8 / INT16 / INT32 / INT64：222，取正负整数即可

   - FLOAT / DOUBLE：1.0，正负浮点数即可

   - VARCHAR："a string 2"，'a string 2'，一个包含在单/双引号里的字符串即可

   - ARRAY：由element type决定，比如BOOL ARRAY可以为[1, 0, ...]，统一的格式为[item1, item2, ...]

   - BINARY VECTOR：[1, 0, 1, 1, ...]，其中的元素只能为0或1

   - FLOAT VECTOR：[1.0, 0.3, -1.2, ...]，其中的元素为正负浮动数

   - JSON：不支持JSON类型，可以使用bulk insert来插入

#### 删除一个Collection的数据

```
DELETE FROM {coll_name} WHERE {conditions}
# or
DELETE FROM {coll_name} WITH {'expr':'...'}
# Example: DELETE FROM book WITH {'expr':'book_id < 10 and book_id > 5'}
```

参数解释如下：

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - conditions：需要删除的数据的筛选条件，类似于SQL里的where子句，具体格式如下。 TODO

 - expr：这里的expr与Milvus文档里的expr的格式相同，与conditions只能二选一

#### 删除一个Collection的一个分区里的数据

```
DELETE FROM PARTITION {part_name} ON {coll_name} WHERE {conditions}
# or
DELETE FROM {coll_name} WITH {'expr':'...'}
# Example: DELETE FROM book WITH {'expr':'book_id < 10 and book_id > 5'}
```

参数解释如下：

 - part_name：操作的分区的命名，取值类型为字符串，且不可包含单/双引号

 - coll_name：操作的collection的命名，取值类型为字符串，且不可包含单/双引号

 - conditions：需要删除的数据的筛选条件，类似于SQL里的where子句，具体格式如下。 TODO

 - expr：这里的expr与Milvus文档里的expr的格式相同，与conditions只能二选一