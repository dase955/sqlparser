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

INSERT_FUNC = caller.func_map['insert']
UPSERT_FUNC = caller.func_map['upsert']


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

    def create_book_collection(self):
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
        for cols, tuples in params:
            sql = f'insert into {TEST_COLLECTION_NAME}({cols}) values {tuples};'
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed result: {parsed_data}')
            INSERT_FUNC(parsed_data)

        collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
        collection.load()
        query_result = collection.query(expr="", limit=100, output_fields=['book_id', 'book_name',
                                                                           'book_intro', 'word_count'])
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
        for cols, tuples in params:
            sql = f'insert into {TEST_COLLECTION_NAME}({cols}) values {tuples};'
            print(f'sql: {sql}')
            parsed_data = parse(sql)
            print(f'parsed result: {parsed_data}')
            INSERT_FUNC(parsed_data)

        collection = pymilvus.Collection(name=TEST_COLLECTION_NAME)
        collection.load()
        query_result = collection.query(expr="", limit=100, output_fields=['book_id', 'book_name',
                                                                           'book_intro', 'json_col'])
        print(f'query result: {query_result}')

        self.dropTestCollection()
