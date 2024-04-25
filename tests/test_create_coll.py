# -*- coding: utf-8 -*-

import unittest
from configparser import ConfigParser

from sqlparser import parse
from caller.call_conn import connect, disconnect
import caller.caller as caller
import pymilvus
import os

# 测试使用的 database
TEST_DATABASE_NAME = "TEST_DATABASE"
# 测试使用的 collection
TEST_COLLECTION_NAME = "TEST_COLLECTION"
CREATE_COLLECTION_FUNC = caller.func_map['create_coll']


def deep_merge_dicts(dict1, dict2):
    """
    Recursively merge two dictionaries.
    """
    result = dict1.copy()
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


class TestCollection(unittest.TestCase):
    def dropTestCollection(self):
        if pymilvus.utility.has_collection(TEST_COLLECTION_NAME, self.using, self.timeout):
            pymilvus.utility.drop_collection(TEST_COLLECTION_NAME, self.timeout, self.using)

    @classmethod
    def setUpClass(cls):
        """
        1. 修改 working directory
        2. 对所有测试，仅连接一次 milvus。
        3. 提前为所有测试处理好连接参数。
        4. 初始化一个 database。
        """
        # 修改 working dir
        os.chdir('..')

        # 连接
        connect()

        # 处理连接参数
        configur = ConfigParser()
        configur.read('config.ini')

        # using -- connection alias
        section = 'Connection'
        using = configur.get(section, 'alias')

        timeout = None
        section = 'Database'
        if 'timeout' in configur[section]:
            timeout = configur.getfloat(section, 'timeout')

        cls.using = using
        cls.timeout = timeout

        # 初始化 database
        database_names = pymilvus.db.list_database(using, timeout)
        # 先删除
        if TEST_DATABASE_NAME in database_names:
            pymilvus.db.using_database(TEST_DATABASE_NAME, using)
            # 清空所有 collection
            collection_names = pymilvus.utility.list_collections(timeout, using)
            for collection_name in collection_names:
                pymilvus.utility.drop_collection(collection_name, timeout, using)

            pymilvus.db.drop_database(TEST_DATABASE_NAME, using, timeout)

        # 重建
        pymilvus.db.create_database(TEST_DATABASE_NAME, using, timeout)
        pymilvus.db.using_database(TEST_DATABASE_NAME, using)

    @classmethod
    def tearDownClass(cls):
        """
        所有测试执行完后关闭连接
        """
        disconnect()

    def setUp(self):
        """
        删除 collection
        """
        self.dropTestCollection()

    def tearDown(self):
        pass

    def test_int8_pk(self):
        """
        primary key must be int64 or varchar, and int8 should fail
        """
        sql_param_list_expr = '{  "num_shards": 2 }'
        sql_field_list_expr = '(field_1 INT8 primary key )'
        sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} WITH {sql_param_list_expr};'
        print(f"executing sql: {sql_expr}")

        parsed_data = parse(sql_expr)
        print(f'parsed data: {parsed_data}')

        with self.assertRaises(pymilvus.exceptions.PrimaryKeyException) as cm:
            CREATE_COLLECTION_FUNC(parsed_data)

    def test_int64_pk_no_vector(self):
        """
        valid primary key, but no vector field is defined, should fail
        """
        sql_param_list_expr = '{ "description": "test desc", "enable_dynamic_field": 1 }'
        sql_field_list_expr = '(field_1 INT64 primary key auto id description ("field_1"))'
        sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} WITH {sql_param_list_expr};'
        print(f"executing sql: {sql_expr}")

        parsed_data = parse(sql_expr)
        print(f'parsed data: {parsed_data}')

        with self.assertRaises(pymilvus.exceptions.SchemaNotReadyException) as cm:
            CREATE_COLLECTION_FUNC(parsed_data)

    def test_int64_pk_with_vector_scalar_partition(self):
        """
        valid primary key(INT64), vector field(FLOAT VECTOR), scalar field(INT8)
        """
        sql_param_list_expr = (' { "description": "test desc", '
                               ' "enable_dynamic_field": 1, '
                               ' "num_shards": 2, '
                               ' "num_partitions": 5 }')
        sql_field_list_expr = ('('
                               'field_1 INT64 primary key auto id description ("field_1"),'
                               'field_2 FLOAT VECTOR(2) description ("field_2"),'
                               'field_3 INT8 description ("field_3"),'
                               'field_4 INT64 pArtItIOn kEY'
                               ')')
        sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} WITH {sql_param_list_expr};'
        print(f"executing sql: {sql_expr}")

        parsed_data = parse(sql_expr)
        print(f'parsed data: {parsed_data}')

        CREATE_COLLECTION_FUNC(parsed_data)

        self.assertTrue(pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout),
                        "Failed to create collection.")
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)

        # expected values
        expected_schema = {
            'auto_id': True,
            'description': 'test desc',
            'enable_dynamic_field': True,
            'fields': [
                {'name': 'field_1', 'description': 'field_1', 'type': pymilvus.DataType.INT64, 'is_primary': True,
                 'auto_id': True},
                {'name': 'field_2', 'description': 'field_2', 'type': pymilvus.DataType.FLOAT_VECTOR,
                 'params': {'dim': 2}},
                {'name': 'field_3', 'description': 'field_3', 'type': pymilvus.DataType.INT8},
                {'name': 'field_4', 'description': '', 'type': pymilvus.DataType.INT64, 'is_partition_key': True}
            ]
        }
        expected_num_shards = 2
        expected_name = TEST_COLLECTION_NAME
        expected_num_partitions = 5

        print(collection)
        self.assertDictEqual(collection.schema.to_dict(), expected_schema,
                             "Collection is created, but its schema does not meet expectations.")
        self.assertEqual(collection.num_shards, expected_num_shards,
                         "Collection is created, but its shards num does not meet expectations.")
        self.assertEqual(collection.describe()['num_partitions'], expected_num_partitions,
                         "Collection is created, but its num_partitions does not meet expectations.")
        self.assertEqual(collection.name, expected_name,
                         "Collection is created, but its name does not meet expectations.")

    def test_int64_pk_field(self):
        """
        valid primary key(INT64), vector field(FLOAT VECTOR)
        """
        sql_param_list_expr = ('{'
                               ' "description": "test desc", '
                               ' "enable_dynamic_field": 1, '
                               ' "num_shards": 2, '
                               ' "primary_field": "field_1" '
                               '}')
        sql_field_list_expr = ('('
                               'field_1 INT64 description ("field_1"),'
                               'field_2 FLOAT VECTOR(2) description ("field_2")'
                               ')')
        sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} WITH {sql_param_list_expr};'
        print(f"executing sql: {sql_expr}")

        parsed_data = parse(sql_expr)
        print(f'parsed data: {parsed_data}')

        CREATE_COLLECTION_FUNC(parsed_data)

        self.assertTrue(pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout),
                        "Failed to create collection.")
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)

        # expected values
        expected_schema = {
            'auto_id': False,
            'description': 'test desc',
            'enable_dynamic_field': True,
            'fields': [
                {'name': 'field_1', 'description': 'field_1', 'type': pymilvus.DataType.INT64, 'is_primary': True,
                 'auto_id': False},
                {'name': 'field_2', 'description': 'field_2', 'type': pymilvus.DataType.FLOAT_VECTOR,
                 'params': {'dim': 2}},
            ]
        }
        expected_num_shards = 2
        expected_name = TEST_COLLECTION_NAME
        expected_num_partitions = 1

        print(collection)
        print(collection.describe())
        self.assertDictEqual(collection.schema.to_dict(), expected_schema,
                             "Collection is created, but its schema does not meet expectations.")
        self.assertEqual(collection.num_shards, expected_num_shards,
                         "Collection is created, but its shards num does not meet expectations.")
        self.assertEqual(collection.describe()['num_partitions'], expected_num_partitions,
                         "Collection is created, but its num_partitions does not meet expectations.")
        self.assertEqual(collection.name, expected_name,
                         "Collection is created, but its name does not meet expectations.")

    def test_varchar(self):
        """
        valid primary key(INT64), vector field(BINARY VECTOR), varchar field
        """
        sql_param_list_expr = '{  "num_shards": 2 }'
        sql_field_list_expr = ('('
                               'field_1 INT64 primary key,'
                               'field_2 binary vector(16),'
                               'field_3 varchar(20)'
                               ')')
        sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} WITH {sql_param_list_expr};'
        print(f"executing sql: {sql_expr}")

        parsed_data = parse(sql_expr)
        print(f'parsed data: {parsed_data}')

        CREATE_COLLECTION_FUNC(parsed_data)
        self.assertTrue(pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout),
                        "Failed to create collection.")
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)

        expected_field = {
            'name': 'field_3',
            'description': '',
            'type': pymilvus.DataType.VARCHAR,
            'params': {'max_length': 20}
        }

        print(collection)
        self.assertDictEqual(collection.schema.to_dict()['fields'][2], expected_field,
                             "Collection is created, but its schema does not meet expectations.")

    def test_scalar_type_basic(self):
        """
        valid primary key(INT64), vector field(BINARY VECTOR), scalar type field
        """

        field_schema_sub_dicts = [
            ('INT8', {'type': pymilvus.DataType.INT8}),
            ('INT16', {'type': pymilvus.DataType.INT16}),
            ('INT32', {'type': pymilvus.DataType.INT32}),
            ('INT64', {'type': pymilvus.DataType.INT64}),
            ('BOOL', {'type': pymilvus.DataType.BOOL}),
            ('Float', {'type': pymilvus.DataType.FLOAT}),
            ('doUbLE', {'type': pymilvus.DataType.DOUBLE}),
            ('JSON', {'type': pymilvus.DataType.JSON}),
            ('varchar(63333)', {'type': pymilvus.DataType.VARCHAR, 'params': {'max_length': 63333}}),
        ]

        for type_name, field_schema_sub_dict in field_schema_sub_dicts:
            sql_field_list_expr = ('('
                                   'field_1 INT64 primary key,'
                                   'field_2 binary vector(16),'
                                   f'field_3 {type_name}'
                                   ')')
            sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr};'
            print(f"executing sql: {sql_expr}")

            parsed_data = parse(sql_expr)
            print(f'parsed data: {parsed_data}')

            CREATE_COLLECTION_FUNC(parsed_data)
            self.assertTrue(
                pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout),
                "Failed to create collection.")
            collection = pymilvus.Collection(TEST_COLLECTION_NAME)

            print(collection)
            created_field_schema = collection.schema.to_dict()['fields'][2]
            merged_field_schema = deep_merge_dicts(created_field_schema, field_schema_sub_dict)

            # subset check
            self.assertDictEqual(merged_field_schema, created_field_schema,
                                 "Collection is created, but its schema does not meet expectations.")
            self.dropTestCollection()

    def test_array(self):
        """
        valid primary key(INT64), vector field(BINARY VECTOR), array field
        """

        field_schema_sub_dicts = [
            ('INT8', {'element_type': pymilvus.DataType.INT8}),
            ('INT16', {'element_type': pymilvus.DataType.INT16}),
            ('INT32', {'element_type': pymilvus.DataType.INT32}),
            ('INT64', {'element_type': pymilvus.DataType.INT64}),
            ('varchar(63333)', {'element_type': pymilvus.DataType.VARCHAR, 'params': {'max_length': '63333'}}),
            ('BOOL', {'element_type': pymilvus.DataType.BOOL}),
            ('Float', {'element_type': pymilvus.DataType.FLOAT}),
            ('doUbLE', {'element_type': pymilvus.DataType.DOUBLE}),
        ]

        for type_name, field_schema_sub_dict in field_schema_sub_dicts:
            sql_field_list_expr = ('('
                                   'field_1 INT64 primary key,'
                                   'field_2 binary vector(16),'
                                   f'field_3 {type_name} array(1234)'
                                   ')')
            sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr};'
            print(f"executing sql: {sql_expr}")

            parsed_data = parse(sql_expr)
            print(f'parsed data: {parsed_data}')

            CREATE_COLLECTION_FUNC(parsed_data)
            self.assertTrue(
                pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout),
                "Failed to create collection.")
            collection = pymilvus.Collection(TEST_COLLECTION_NAME)

            print(collection)

            created_field_schema = collection.schema.to_dict()['fields'][2]
            merged_field_schema = deep_merge_dicts(created_field_schema, field_schema_sub_dict)

            self.assertEqual(created_field_schema['type'], pymilvus.DataType.ARRAY,
                             "Wrong field type.")
            # why string?
            self.assertEqual(created_field_schema['params']['max_capacity'], '1234',
                             "Wrong array capacity.")
            # subset check
            self.assertDictEqual(merged_field_schema, created_field_schema,
                                 "Collection is created, but its schema does not meet expectations.")
            self.dropTestCollection()

    def test_complex_sql(self):
        """
        with clause(true/false)
        field_1: int64 or varchar(20), primary key, auto id(true/false)
        field_2: float vector(4) or binary vector(32)
        field_3: every scalar type
        field_4: every array type
        field_5: int64 or varchar(30), partition key
        """

        field1_int64_base_dict = {'type': pymilvus.DataType.INT64,
                                  'is_primary': True,
                                  'auto_id': True,
                                  'description': "field 1"}
        field1_varchar_base_dict = {'type': pymilvus.DataType.VARCHAR,
                                    'is_primary': True,
                                    'auto_id': True,
                                    'description': "field 1",
                                    'params': {'max_length': 22222}}
        field1_expr_and_subdicts = [
            ('int64 primary key', {'type': pymilvus.DataType.INT64, 'is_primary': True}),
            ('int64 primary key auto id description("field 1")', field1_int64_base_dict),
            ('int64 primary key description("field 1") auto id ', field1_int64_base_dict),
            ('int64 auto id primary key description("field 1")', field1_int64_base_dict),
            ('int64 auto id description("field 1") primary key ', field1_int64_base_dict),
            ('int64 description("field 1") auto id primary key ', field1_int64_base_dict),
            ('int64 description("field 1") primary key auto id ', field1_int64_base_dict),
            ('varchar(22222) primary key', {'type': pymilvus.DataType.VARCHAR, 'is_primary': True}),
            ('varchar(22222) primary key auto id description("field 1")', field1_varchar_base_dict),
            ('varchar(22222) primary key description("field 1") auto id ', field1_varchar_base_dict),
            ('varchar(22222) auto id primary key description("field 1")', field1_varchar_base_dict),
            ('varchar(22222) auto id description("field 1") primary key ', field1_varchar_base_dict),
            ('varchar(22222) description("field 1") auto id primary key ', field1_varchar_base_dict),
            ('varchar(22222) description("field 1") primary key auto id ', field1_varchar_base_dict),
        ]

        field2_expr_and_subdicts = [
            ("float vector(4)", {'type': pymilvus.DataType.FLOAT_VECTOR, 'params': {'dim': 4}}),
            ("binary vector(32)", {'type': pymilvus.DataType.BINARY_VECTOR, 'params': {'dim': 32}}),
        ]

        field3_expr_and_subdicts = [
            ('INT8', {'type': pymilvus.DataType.INT8}),
            ('INT16', {'type': pymilvus.DataType.INT16}),
            ('INT32', {'type': pymilvus.DataType.INT32}),
            ('INT64', {'type': pymilvus.DataType.INT64}),
            ('BOOL', {'type': pymilvus.DataType.BOOL}),
            ('Float', {'type': pymilvus.DataType.FLOAT}),
            ('doUbLE', {'type': pymilvus.DataType.DOUBLE}),
            ('JSON', {'type': pymilvus.DataType.JSON}),
            ('varchar(11222)', {'type': pymilvus.DataType.VARCHAR, 'params': {'max_length': 11222}}),
        ]

        field4_expr_and_subdicts = [
            ('INT8', {'element_type': pymilvus.DataType.INT8}),
            ('INT16', {'element_type': pymilvus.DataType.INT16}),
            ('INT32', {'element_type': pymilvus.DataType.INT32}),
            ('INT64', {'element_type': pymilvus.DataType.INT64}),
            ('varchar(63333)', {'element_type': pymilvus.DataType.VARCHAR, 'params': {'max_length': '63333'}}),
            ('BOOL', {'element_type': pymilvus.DataType.BOOL}),
            ('Float', {'element_type': pymilvus.DataType.FLOAT}),
            ('doUbLE', {'element_type': pymilvus.DataType.DOUBLE}),
        ]

        field5_expr_and_subdicts = [
            ('int64 partition key', {'type': pymilvus.DataType.INT64,
                                     'is_partition_key': True,
                                     }),
            ('varchar(20) partition key', {'type': pymilvus.DataType.VARCHAR,
                                           'is_partition_key': True,
                                           'params': {'max_length': 20}
                                           }),
        ]

        for with_clause_exists in [True, False]:
            for field1_expr, field1_subdict in field1_expr_and_subdicts:
                for field2_expr, field2_subdict in field2_expr_and_subdicts:
                    for field3_expr, field3_subdict in field3_expr_and_subdicts:
                        for field4_expr, field4_subdict in field4_expr_and_subdicts:
                            for field5_expr, field5_subdict in field5_expr_and_subdicts:
                                with_clause = 'with {"num_shards": 12}' if with_clause_exists else ''
                                sql_field_list_expr = ('('
                                                       f'field_1 {field1_expr},'
                                                       f'field_2 {field2_expr} description("field 2"),'
                                                       f'field_3 {field3_expr} description("field 3"),'
                                                       f'field_4 {field4_expr} array(1234) description("field 4"),'
                                                       f'field_5 {field5_expr} description("field 5")'
                                                       ')')
                                sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} {with_clause} ;'
                                print(f"executing sql: {sql_expr}")

                                parsed_data = parse(sql_expr)
                                print(f'parsed data: {parsed_data}')

                                CREATE_COLLECTION_FUNC(parsed_data)
                                self.assertTrue(
                                    pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using,
                                                                    timeout=self.timeout),
                                    "Failed to create collection.")
                                collection = pymilvus.Collection(TEST_COLLECTION_NAME)
                                print(collection)

                                # check with clause
                                if with_clause_exists:
                                    self.assertEqual(collection.num_shards, 12, "Unexpected num_shards.")

                                # check field 1
                                created_field1 = collection.schema.to_dict()['fields'][0]
                                merged_field1 = deep_merge_dicts(created_field1, field1_subdict)
                                self.assertDictEqual(merged_field1, created_field1, "Unexpected field_1.")

                                # check field 2
                                created_field2 = collection.schema.to_dict()['fields'][1]
                                merged_field2 = deep_merge_dicts(created_field2, field2_subdict)
                                merged_field2 = deep_merge_dicts(merged_field2, {'description': 'field 2'})
                                self.assertDictEqual(merged_field2, created_field2, "Unexpected field_2.")

                                # check field 3
                                created_field3 = collection.schema.to_dict()['fields'][2]
                                merged_field3 = deep_merge_dicts(created_field3, field3_subdict)
                                merged_field3 = deep_merge_dicts(merged_field3, {'description': 'field 3'})
                                self.assertDictEqual(merged_field3, created_field3, "Unexpected field_3.")

                                # check field 4
                                created_field4 = collection.schema.to_dict()['fields'][3]
                                merged_field4 = deep_merge_dicts(created_field4, field4_subdict)
                                merged_field4 = deep_merge_dicts(merged_field4, {'description': 'field 4',
                                                                                 'type': pymilvus.DataType.ARRAY,
                                                                                 'params': {'max_capacity': '1234'}})
                                self.assertDictEqual(merged_field4, created_field4, "Unexpected field_4.")

                                # check field 5
                                created_field5 = collection.schema.to_dict()['fields'][4]
                                merged_field5 = deep_merge_dicts(created_field5, field5_subdict)
                                merged_field5 = deep_merge_dicts(merged_field5, {'description': 'field 5'})
                                self.assertDictEqual(merged_field5, created_field5, "Unexpected field_5.")

                                self.dropTestCollection()

    def disabled_test_default_value(self):
        test_field = pymilvus.FieldSchema(
            name='test_field',
            dtype=pymilvus.DataType.INT64,
            default_value=10
        )
        print(test_field)
        book_id = pymilvus.FieldSchema(
            name="book_id",
            dtype=pymilvus.DataType.INT64,
            is_primary=True,
        )
        book_name = pymilvus.FieldSchema(
            name="book_name",
            dtype=pymilvus.DataType.VARCHAR,
            max_length=200,
            # The default value will be used if this field is left empty during data inserts or upserts.
            # The data type of `default_value` must be the same as that specified in `dtype`.
            default_value="Unknown"
        )
        word_count = pymilvus.FieldSchema(
            name="word_count",
            dtype=pymilvus.DataType.INT64,
            # The default value will be used if this field is left empty during data inserts or upserts.
            # The data type of `default_value` must be the same as that specified in `dtype`.
            default_value=9999,
            is_partition_key=True
        )
        book_intro = pymilvus.FieldSchema(
            name="book_intro",
            dtype=pymilvus.DataType.FLOAT_VECTOR,
            dim=2
        )
        schema = pymilvus.CollectionSchema(
            fields=[book_id, book_name, word_count, book_intro],
            description="Test book search",
            enable_dynamic_field=True
        )
        collection = pymilvus.Collection(
            name=TEST_COLLECTION_NAME,
            schema=schema,
            using='default',
            num_shards=2,
            num_partitions=2
        )
        print(collection)
        print(collection.describe())

        data = [
            [i for i in range(2000)],
            [None for i in range(2000)],
            [],
            [[1.0 for _ in range(2)] for _ in range(2000)],
        ]
