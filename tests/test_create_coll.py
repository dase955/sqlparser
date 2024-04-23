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

    def test_simple(self):
        result = parse("select * from blog;")
        print(result)

    def test_int8_pk(self):
        # sql_param_list_expr = '{ "description": "test desc", "enable_dynamic_field": 1, "shards_num": 2 }'
        sql_param_list_expr = '{  "shards_num": 2 }'
        # sql_field_list_expr = '(field_1 INT8 primary key auto id description "field_1 desc")'
        sql_field_list_expr = '(field_1 INT8 primary key )'
        # sql_field_list_expr = '(field_1 INT8 )'
        sql_expr = f'create collection {TEST_COLLECTION_NAME} {sql_field_list_expr} WITH {sql_param_list_expr};'
        print(f"executing sql: {sql_expr}")

        parsed_data = parse(sql_expr)
        CREATE_COLLECTION_FUNC(parsed_data)

        self.assertTrue(pymilvus.utility.has_collection(TEST_COLLECTION_NAME, using=self.using, timeout=self.timeout),
                        msg="Failed to create collection.")

        collection = pymilvus.Collection(TEST_COLLECTION_NAME)
        print(collection)
