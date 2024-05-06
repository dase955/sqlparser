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


def call_by_parsed_data(parsed_data):
    return caller.func_map[parsed_data['type']](parsed_data)


class TestQuery(unittest.TestCase):
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
        if 'config.ini' not in os.listdir():
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

    @staticmethod
    def create_book_collection():
        book_id = pymilvus.FieldSchema(
            name="book_id",
            dtype=pymilvus.DataType.INT64,
            is_primary=True,
        )
        book_name = pymilvus.FieldSchema(
            name="book_name",
            dtype=pymilvus.DataType.VARCHAR,
            max_length=200,
        )
        word_count = pymilvus.FieldSchema(
            name="word_count",
            dtype=pymilvus.DataType.INT64,
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
        collection.load()
        data = [
            [i for i in range(5)],
            [str(i) for i in range(5)],
            [i for i in range(10000, 10005)],
            [[1.0, 2.0] for _ in range(5)],
        ]
        collection.insert(data)

    @staticmethod
    def create_json_array_collection():
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
        array_field = pymilvus.FieldSchema(
            name="array_field",
            dtype=pymilvus.DataType.ARRAY,
            element_type=pymilvus.DataType.INT64,
            max_capacity=1000,
        )
        json_field = pymilvus.FieldSchema(
            name="json_field",
            dtype=pymilvus.DataType.JSON,
        )
        float_field = pymilvus.FieldSchema(
            name="float_field",
            dtype=pymilvus.DataType.FLOAT,
        )
        schema = pymilvus.CollectionSchema(
            fields=[id_field, vector_field, array_field, json_field, float_field],
            description="json + array test collection",
        )
        collection = pymilvus.Collection(
            name=TEST_COLLECTION_NAME,
            schema=schema,
            using='default',
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
        collection.load()
        data = [
            [i for i in range(5)],
            [b'\x01\xff' for _ in range(5)],
            [[1], [1, 2], [1, 2, 3], [4], [4, 5]],
            [{"x": [1, 2, 3], "y": True},
             {"x": [1, 2], "y": False},
             {"z": [3], "x": "str"},
             {"x": [4], "y": {}},
             {"x": [4, 5], "y": 4.5}],
            [2.0 for _ in range(5)],
        ]
        collection.insert(data)

    def test_simple_comparison(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('book_id > 3', [4]),
            ('book_id = 4', [4]),
            ('book_id < 1', [0]),
            ('book_id >= 3', [3, 4]),
            ('book_id <= 1', [0, 1]),
            ('book_id != 0', [1, 2, 3, 4]),
            ('book_id <> 0', [1, 2, 3, 4]),
        ]

        self.create_book_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['book_id'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_simple_between_and(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('book_id between 3 and 4', [3, 4]),
            ('book_id not between 3 and 4', [0, 1, 2]),
            ('not book_id not between 3 and 4', [3, 4]),
        ]

        self.create_book_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['book_id'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_simple_like(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('book_name like "0"', [0]),
            ('book_name like "0%"', [0]),
            ('book_name not like "0%"', [1, 2, 3, 4]),
            ('not book_name like "0%"', [1, 2, 3, 4]),
            ('book_name like "%"', [0, 1, 2, 3, 4]),
        ]

        self.create_book_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['book_id'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_simple_in(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('book_id in [3, 4]', [3, 4]),
            ('book_id not in [3, 4]', [0, 1, 2]),
            ('not book_id not in [3, 4]', [3, 4]),
        ]

        self.create_book_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['book_id'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_functions(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('array_length(array_field) = 3', [2]),
            ('array_contains(array_field, 1)', [0, 1, 2]),
            ('array_contains_all(array_field, [1, 2])', [1, 2]),
            ('array_contains_any(array_field, [4, 5])', [3, 4]),
            ('json_contains(json_field["x"], 4)', [3, 4]),
            ('json_contains_all(json_field["z"], [3])', [2]),
            ('json_contains_any(json_field["x"], [1, 2, 3])', [0, 1]),
        ]

        self.create_json_array_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['id_field'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_simple_and_or(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('book_id = 3 or book_id = 4', [3, 4]),
            ('book_id = 3 and book_id = 4', []),
            ('book_id in [3, 4] and book_id in [3] or book_id in [0, 1] and book_id in [0]', [0, 3]),
        ]

        self.create_book_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['book_id'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_simple_arithmetic(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('book_id = 0 + 2', [2]),
            ('book_id = 4 - 2', [2]),
            ('book_id = -2 * -2', [4]),
            ('book_id = 4 / 2', [2]),
            ('book_id = 4 % 2', [0]),
        ]

        self.create_book_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['book_id'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_subscript(self):
        # (expr, expected_id_list)
        testcase_tuples = [
            ('array_field[0] = 1', [0, 1, 2]),
            ('json_field["x"][0] = 1', [0, 1]),
            ('json_field["y"] = false', [1]),
        ]

        self.create_json_array_collection()
        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        collection.flush()

        for expr, expected_list in testcase_tuples:
            sql = f"select * from {TEST_COLLECTION_NAME} where {expr};"
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed data: {parsed_data}')

            query_result = call_by_parsed_data(parsed_data)
            print(f'query result: {query_result}')

            remaining_ids = [row['id_field'] for row in query_result]
            self.assertListEqual(expected_list, remaining_ids,
                                 "The query result list is not equal to the expected list")

    def test_partition(self):
        # (expr, remaining_id_list)
        self.create_json_array_collection()
        collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
        collection.create_partition("test")
        sql = f"select * from partition test on {TEST_COLLECTION_NAME} where true = true;"
        print(f'sql: {sql}')
        parsed_data = parse(sql)
        print(f'parsed data: {parsed_data}')

        query_result = call_by_parsed_data(parsed_data)

        print(f'query result: {query_result}')

        expected_list = []
        remaining_ids = [row['id_field'] for row in query_result]
        self.assertListEqual(expected_list, remaining_ids,
                             "The query result list is not equal to the expected list")

    def test_limit_offset_fields(self):
        # (expr, remaining_id_list)
        self.create_book_collection()
        collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
        collection.flush()

        sql = f"select book_id,book_name from {TEST_COLLECTION_NAME} limit 2 offset 2 where book_id in [0, 1, 2, 3, 4];"
        print(f'sql: {sql}')
        parsed_data = parse(sql)
        print(f'parsed data: {parsed_data}')

        query_result = call_by_parsed_data(parsed_data)

        print(f'query result: {query_result}')

        expected_list = [2, 3]
        remaining_ids = [row['book_id'] for row in query_result]
        self.assertListEqual(expected_list, remaining_ids,
                             "The query result list is not equal to the expected list")
        for result in query_result:
            self.assertEqual(str(result['book_id']), result['book_name'], "Incorrect query result.")

    def test_count_all(self):
        # (expr, remaining_id_list)
        self.create_book_collection()
        collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
        collection.flush()

        sql = f"select count(*) from {TEST_COLLECTION_NAME} where book_id in [0, 1, 2, 3, 4];"
        print(f'sql: {sql}')
        parsed_data = parse(sql)
        print(f'parsed data: {parsed_data}')

        query_result = call_by_parsed_data(parsed_data)

        print(f'query result: {query_result}')

        expected = [{'count(*)': 5}]
        self.assertListEqual(query_result, expected,
                             "The query result list is not equal to the expected list")
