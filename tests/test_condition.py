# -*- coding: utf-8 -*-

import unittest
from configparser import ConfigParser

from sqlparser import parse
from caller.call_conn import connect, disconnect
import pymilvus
import os

# 测试使用的 database
TEST_DATABASE_NAME = "TEST_DATABASE"
# 测试使用的 collection
TEST_COLLECTION_NAME = "TEST_COLLECTION"


class TestCondition(unittest.TestCase):
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

    def test_simple_comparison(self):
        for comparator in ['=', '>', '<', '>=', '<=', '!=', '<>']:
            for value in [3, 5.0]:
                for direction in [True, False]:
                    if direction:
                        lvalue, rvalue = 'book_id', value
                    else:
                        rvalue, lvalue = 'book_id', value
                    sql = f"delete from {TEST_COLLECTION_NAME} where {lvalue} {comparator} {rvalue};"
                    print(f'sql: {sql}')
                    result = parse(sql)
                    print(f'result: {result}')

    def test_simple_between_and(self):
        tuples = [(3, 5.0), (3.0, 5)]
        for lvalue, rvalue in tuples:
            for not_expr in ["not", ""]:
                sql = f"delete from {TEST_COLLECTION_NAME} where book_id {not_expr} between {lvalue} and {rvalue};"
                print(f'sql: {sql}')
                result = parse(sql)
                print(f'result: {result}')

    def test_simple_like(self):
        for not_expr in ["not", ""]:
            sql = f'delete from {TEST_COLLECTION_NAME} where book_id {not_expr} like "prefix%";'
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')

    def test_simple_in(self):
        lists = ["[1, 2.0, 3]", '["abc", "def", "\'"]', r'[""]']
        for in_list in lists:
            for not_expr in ["not", ""]:
                sql = f'delete from {TEST_COLLECTION_NAME} where book_id {not_expr} in {in_list};'
                print(f'sql: {sql}')
                result = parse(sql)
                print(f'result: {result}')

    def test_simple_not(self):
        expr_list = ['book_id > 5', 'book_id = 7', 'ARRAY_LENGTH(book_id) = 8', 'ARRAY_CONTAINS(book_id, 2)',
                     'book_id not between 1 and 5']
        for expr in expr_list:
            sql = f'delete from {TEST_COLLECTION_NAME} where not {expr};'
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')

    def test_simple_function(self):
        conditions = ["ARRAY_LENGTH(book_id) = 5",
                      'ARRAY_CONTAINS(book_id, 1)',
                      'ARRAY_CONTAINS_ALL(book_id, [1, 2, 8])',
                      'ARRAY_CONTAINS_ANY(book_id, [6, 9])',
                      'JSON_CONTAINS(book_id, 1)',
                      'JSON_CONTAINS_ALL(book_id, [1, 2, 8])',
                      'JSON_CONTAINS_ANY(book_id, [6, 9])',
                      ]
        for condition in conditions:
            sql = f'delete from {TEST_COLLECTION_NAME} where {condition};'
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')

    def test_simple_and_or(self):
        exprs = ['and', 'or']
        for expr in exprs:
            sql = f"delete from {TEST_COLLECTION_NAME} where book_id = 5 {expr} book_id =6;"
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')

    def test_both_and_or(self):
        sql = f"delete from {TEST_COLLECTION_NAME} where book_id = 4 and book_id = 5 or book_id = 6 and book_id = 7;"
        print(f'sql: {sql}')
        result = parse(sql)
        print(f'result: {result}')

    def test_complex_conditions(self):
        lists = ["[1, 2.0, 3]", '["abc", "def", "\'"]']
        for in_list in lists:
            sql = f'delete from {TEST_COLLECTION_NAME} where book_id in {in_list};'
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')

    def test_simple_arithmetic(self):
        exprs = ['30 / 2 - 8', '(5*8/8)', '5*(8/8)', '5**(8/8)', '30 / 2 + 8', '-2 * -(2 - 4)']
        for expr in exprs:
            sql = f"delete from {TEST_COLLECTION_NAME} where book_id = {expr};"
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')

    def test_subscript(self):
        exprs = ['arr[0]', 'obj["key"]', 'obj["key"][0]']
        for expr in exprs:
            sql = f"delete from {TEST_COLLECTION_NAME} where {expr} = 5;"
            print(f'sql: {sql}')
            result = parse(sql)
            print(f'result: {result}')
