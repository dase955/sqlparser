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


class TestCollection(unittest.TestCase):
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
        if pymilvus.utility.has_collection(TEST_COLLECTION_NAME, self.using, self.timeout):
            pymilvus.utility.drop_collection(TEST_COLLECTION_NAME, self.timeout, self.using)

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
        sql_param_list_expr = '{ "description": "test desc", "enable_dynamic_field": 1, "num_shards": 2 }'
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

        pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout)
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

        print(collection)
        self.assertDictEqual(collection.schema.to_dict(), expected_schema,
                             "Collection is created, but its schema does not meet expectations.")
        self.assertEqual(collection.num_shards, expected_num_shards,
                         "Collection is created, but its shards num does not meet expectations.")
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

        pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout)
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

        print(collection)
        self.assertDictEqual(collection.schema.to_dict(), expected_schema,
                             "Collection is created, but its schema does not meet expectations.")
        self.assertEqual(collection.num_shards, expected_num_shards,
                         "Collection is created, but its shards num does not meet expectations.")
        self.assertEqual(collection.name, expected_name,
                         "Collection is created, but its name does not meet expectations.")

    def test_varchar(self):
        """
        valid primary key(INT64), vector field(FLOAT VECTOR), varchar field
        """
        sql_param_list_expr = '{  "num_shards": 2 }'
        sql_field_list_expr = ('('
                               'field_1 INT64 primary key,'
                               'field_2 float vector(4),'
                               'field_3 varchar(20)'
                               ')')
        sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} WITH {sql_param_list_expr};'
        print(f"executing sql: {sql_expr}")

        parsed_data = parse(sql_expr)
        print(f'parsed data: {parsed_data}')

        CREATE_COLLECTION_FUNC(parsed_data)
