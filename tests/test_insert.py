# -*- coding: utf-8 -*-

import unittest
from configparser import ConfigParser

from sqlparser import parse
from caller.call_conn import connect, disconnect
from caller import caller
import pymilvus
import os

# 测试使用的 database
TEST_DATABASE_NAME = "TEST_DATABASE"
# 测试使用的 collection
TEST_COLLECTION_NAME = "TEST_COLLECTION"
# 测试使用的 partition
TEST_PARTITION_NAME = "TEST_PARTITION"

INSERT_FUNC = caller.func_map['insert']
UPSERT_FUNC = caller.func_map['upsert']


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


def call_by_parsed_data(parsed_data):
    caller.func_map[parsed_data['type']](parsed_data)


class TestInsert(unittest.TestCase):
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

    def create_book_collection(self, pk_auto_id=False):
        book_id = pymilvus.FieldSchema(
            name="book_id",
            dtype=pymilvus.DataType.INT64,
            is_primary=True,
            auto_id=pk_auto_id
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
        )
        # build index
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024}
        }
        collection.create_index(
            field_name="book_intro",
            index_params=index_params
        )

    def create_book_collection_json(self):
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
        json_col = pymilvus.FieldSchema(
            name="json_col",
            dtype=pymilvus.DataType.JSON,
        )
        book_intro = pymilvus.FieldSchema(
            name="book_intro",
            dtype=pymilvus.DataType.FLOAT_VECTOR,
            dim=2
        )
        schema = pymilvus.CollectionSchema(
            fields=[book_id, book_name, json_col, book_intro],
            description="Test book search",
            enable_dynamic_field=True
        )
        collection = pymilvus.Collection(
            name=TEST_COLLECTION_NAME,
            schema=schema,
            using='default',
        )
        # build index
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024}
        }
        collection.create_index(
            field_name="book_intro",
            index_params=index_params
        )

    def create_custom_collection(self, custom_field_schema: pymilvus.FieldSchema):
        id_field = pymilvus.FieldSchema(
            name="id_field",
            dtype=pymilvus.DataType.INT64,
            is_primary=True,
        )
        vector_field = pymilvus.FieldSchema(
            name="vector_field",
            dtype=pymilvus.DataType.BINARY_VECTOR,
            dim=16
        )
        schema = pymilvus.CollectionSchema(
            fields=[id_field, vector_field, custom_field_schema],
            description="custom field collection",
        )
        collection = pymilvus.Collection(
            name=TEST_COLLECTION_NAME,
            schema=schema,
            using='default',
            num_shards=2,
        )
        # build index
        index_params = {
            "metric_type": "HAMMING",
            "index_type": "BIN_FLAT",
            "params": {"nlist": 1024}
        }
        collection.create_index(
            field_name="vector_field",
            index_params=index_params
        )

    def test_simple_insert_parse_only(self):
        params = [("book_id, book_intro", "(1, [1.0, 2.0])"),
                  ("book_id, book_intro", "(1, [1.0, 2.0]), (2, [3.0, 2.0])"),
                  ("book_id, book_intro", "(1, [true]), (2, [false])"),
                  ("book_id, book_intro", "(1, {'name': 'name1'}), (2, {'name': 'name2'})"),
                  ("book_id, book_intro", "(1, {'nested': {'name': 'name1', 'list': []}}),"
                                          "(2, {'nested': {'name': 'name2', 'list': ['1']}})"),
                  ]
        for cols, tuples in params:
            sql = f'insert into {TEST_COLLECTION_NAME}({cols}) values {tuples};'
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')

    def test_simple_insert_book(self):
        self.create_book_collection()
        params = [("book_id, book_intro, book_name, word_count", "(1, [1.0, 2.0], 'name1', 323)"),
                  ("book_id, book_intro,book_name, word_count", '(2, [1.0, 2.0], "name2", 324),'
                                                                ' (3, [3.0, 2.0], "name3", 325)'),
                  ]
        for insert_type_str in ['insert', 'upsert']:
            for cols, tuples in params:
                sql = f'{insert_type_str} into {TEST_COLLECTION_NAME}({cols}) values {tuples};'
                print(f'sql: {sql}')
                parsed_data = parse(sql)
                print(f'parsed result: {parsed_data}')
                call_by_parsed_data(parsed_data)

            collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
            collection.load()
            query_result = collection.query(expr="", limit=100, output_fields=['book_id', 'book_name',
                                                                               'book_intro', 'word_count'])
            print(f'query result: {query_result}')

        self.dropTestCollection()

    def test_simple_insert_book_auto_id_dynamic_schema(self):
        self.create_book_collection(pk_auto_id=True)
        params = [
            ("book_intro, book_name, word_count", "([1.0, 2.0], 'name1', 323)"),
            ("book_intro, book_name, word_count", '([1.0, 2.0], "name2", 324),'
                                                  '([3.0, 2.0], "name3", 325)'),
            ("book_intro, book_name, word_count, dyn_field", "([1.0, 2.0], 'name4', 323, 4)"),
        ]
        for cols, tuples in params:
            sql = f'insert into {TEST_COLLECTION_NAME}({cols}) values {tuples};'
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed result: {parsed_data}')
            call_by_parsed_data(parsed_data)

            collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
            collection.load()
            query_result = collection.query(expr="", limit=100, output_fields=['book_id', 'book_name',
                                                                               'book_intro', 'word_count', 'dyn_field'])
            print(f'query result: {query_result}')

        self.dropTestCollection()

    def test_insert_json(self):
        self.create_book_collection_json()
        params = [("book_id, book_intro, book_name, json_col", "(1, [1.0, 2.0], 'name1', {'name': 'name1'})"),
                  ("book_id, book_intro, book_name, json_col", '(2, [1.0, 2.0], "name2", {"name": "name1"}),'
                                                               "(3, [3.0, 2.0], 'name3', {'name': 'name1', 'list': [2], 'nested': {'name': 'name2', 'list': [{'name': 'name2'}, {'name': 'name2'}]}}),"
                                                               "(4, [3.0, 2.0], 'name3', {'name': 'name1', 'list': [2], 'nested': {'name': 'name2', 'list': [[], []]}})"
                   ),
                  ]

        for insert_type_str in ['insert', 'upsert']:
            for cols, tuples in params:
                sql = f'{insert_type_str} into {TEST_COLLECTION_NAME}({cols}) values {tuples};'
                print(f'sql: {sql}')
                parsed_data = parse(sql)
                print(f'parsed result: {parsed_data}')
                call_by_parsed_data(parsed_data)

            collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
            collection.load()
            query_result = collection.query(expr="", limit=100, output_fields=['book_id', 'book_name',
                                                                               'book_intro', 'json_col'])
            print(f'query result: {query_result}')

        self.dropTestCollection()

    def test_simple_insert_book_partition(self):
        self.create_book_collection()
        collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
        partition = collection.create_partition(TEST_PARTITION_NAME)
        params = [("book_id, book_intro, book_name, word_count", "(1, [1.0, 2.0], 'name1', 323)"),
                  ("book_id, book_intro,book_name, word_count", '(2, [1.0, 2.0], "name2", 324),'
                                                                ' (3, [3.0, 2.0], "name3", 325)'),
                  ]
        for insert_type_str in ['insert', 'upsert']:
            for cols, tuples in params:
                sql = f'{insert_type_str} into partition {TEST_PARTITION_NAME} on {TEST_COLLECTION_NAME}({cols}) values {tuples};'
                print(f'sql: {sql}')
                parsed_data = parse(sql)
                print(f'parsed result: {parsed_data}')
                call_by_parsed_data(parsed_data)

            collection.load()
            query_result = partition.query(expr="", limit=100, output_fields=['book_id', 'book_name',
                                                                              'book_intro', 'word_count'])
            print(f'query result: {query_result}')

        self.dropTestCollection()

    def test_insert_binary_vector_scalar_types(self):
        field_name = 'custom'
        # tuples for test, (field_schema, data_for_insertion, dict_to_check)
        field_tuples = [
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.INT8), '1', {field_name: 1}),
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.INT16), '1', {field_name: 1}),
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.INT32), '1', {field_name: 1}),
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.INT64), '1', {field_name: 1}),
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.BOOL), 'true', {field_name: True}),
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.FLOAT), '1.5', {field_name: 1.5}),
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.DOUBLE), '3.2', {field_name: 3.2}),
            (pymilvus.FieldSchema(field_name, pymilvus.DataType.JSON), '{"key": "value"}', {field_name: {"key": "value"}}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.VARCHAR, max_length=63333), '"test"', {field_name: "test"}),
        ]

        for field_schema, value_str, subdict in field_tuples:
            self.create_custom_collection(field_schema)
            collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
            for insert_type_str in ['insert', 'upsert']:
                cols = f'id_field, vector_field, {field_name}'
                values = f'1, [1, 255], {value_str}'
                sql = f'{insert_type_str} into {TEST_COLLECTION_NAME}({cols}) values ({values});'
                print(f'sql: {sql}')
                parsed_data = parse(sql)
                print(f'parsed result: {parsed_data}')
                call_by_parsed_data(parsed_data)

                collection.load()
                query_result = collection.query(expr="", limit=100, output_fields=['id_field', 'vector_field', field_name])
                print(f'query result: {query_result}')

                self.assertEqual(len(query_result), 1, "Incorrect result length.")
                merged_dict = deep_merge_dicts(query_result[0], {'id_field': 1, 'vector_field': [b'\x01\xff']} | subdict)
                self.assertDictEqual(merged_dict, query_result[0], "Incorrect query result.")

            self.dropTestCollection()

    def test_insert_binary_vector_array_types(self):
        field_name = 'custom'
        # tuples for test, (field_schema, data_for_insertion, dict_to_check)
        field_tuples = [
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.INT8), '[1]', {field_name: [1]}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.INT16), '[1]', {field_name: [1]}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.INT32), '[1]', {field_name: [1]}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.INT64), '[1]', {field_name: [1]}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.BOOL), '[true]', {field_name: [True]}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.FLOAT), '[1.5]', {field_name: [1.5]}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.DOUBLE), '[3.2]', {field_name: [3.2]}),
            (pymilvus.FieldSchema(name=field_name, dtype=pymilvus.DataType.ARRAY, max_capacity=4000, element_type=pymilvus.DataType.VARCHAR, max_length=63333), '["test"]', {field_name: ["test"]}),
        ]

        for field_schema, value_str, subdict in field_tuples:
            self.create_custom_collection(field_schema)
            collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
            for insert_type_str in ['insert', 'upsert']:
                cols = f'id_field, vector_field, {field_name}'
                values = f'1, [1, 255], {value_str}'
                sql = f'{insert_type_str} into {TEST_COLLECTION_NAME}({cols}) values ({values});'
                print(f'sql: {sql}')
                parsed_data = parse(sql)
                print(f'parsed result: {parsed_data}')
                call_by_parsed_data(parsed_data)

                collection.load()
                query_result = collection.query(expr="", limit=100, output_fields=['id_field', 'vector_field', field_name])
                print(f'query result: {query_result}')

                self.assertEqual(len(query_result), 1, "Incorrect result length.")
                merged_dict = deep_merge_dicts(query_result[0], {'id_field': 1, 'vector_field': [b'\x01\xff']} | subdict)
                self.assertDictEqual(merged_dict, query_result[0], "Incorrect query result.")

            self.dropTestCollection()
