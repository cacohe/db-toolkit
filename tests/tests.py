"""
Database Toolkit 测试套件
"""

import unittest
import os
import sys

# 添加父目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_toolkit import (
    create_client,
    ClientFactory,
    ConfigManager,
    QueryBuilder,
    SQLiteClient,
    BaseClient,
)
from db_toolkit.mixins import BatchOperationsMixin, TransactionMixin
from db_toolkit.exceptions import (
    ConfigurationError,
    ConnectionError,
    QueryError,
)


class TestSQLiteClient(unittest.TestCase):
    """SQLite客户端测试"""
    
    def setUp(self):
        """测试前准备"""
        self.config = {'database': ':memory:'}
        self.client = create_client('sqlite', self.config)
        self.client.connect()
        
        # 创建测试表
        self.client.execute('''
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                email TEXT,
                age INTEGER
            )
        ''')
    
    def tearDown(self):
        """测试后清理"""
        self.client.disconnect()
    
    def test_connection(self):
        """测试连接"""
        self.assertTrue(self.client.is_connected())
    
    def test_insert(self):
        """测试插入"""
        user_id = self.client.insert('test_users', {
            'username': 'test_user',
            'email': 'test@example.com',
            'age': 25
        })
        self.assertIsNotNone(user_id)
        self.assertGreater(user_id, 0)
    
    def test_select_all(self):
        """测试查询所有"""
        # 插入测试数据
        for i in range(3):
            self.client.insert('test_users', {
                'username': f'user{i}',
                'email': f'user{i}@example.com',
                'age': 20 + i
            })
        
        users = self.client.select('test_users')
        self.assertEqual(len(users), 3)
    
    def test_select_with_condition(self):
        """测试条件查询"""
        self.client.insert('test_users', {
            'username': 'alice',
            'email': 'alice@example.com',
            'age': 30
        })
        
        users = self.client.select('test_users', condition={'username': 'alice'})
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]['username'], 'alice')
    
    def test_select_with_fields(self):
        """测试字段选择"""
        self.client.insert('test_users', {
            'username': 'bob',
            'email': 'bob@example.com',
            'age': 28
        })
        
        users = self.client.select('test_users', fields=['username', 'email'])
        self.assertIn('username', users[0])
        self.assertIn('email', users[0])
        self.assertNotIn('age', users[0])
    
    def test_select_with_limit(self):
        """测试限制数量"""
        for i in range(10):
            self.client.insert('test_users', {
                'username': f'user{i}',
                'email': f'user{i}@example.com',
                'age': 20
            })
        
        users = self.client.select('test_users', limit=5)
        self.assertEqual(len(users), 5)
    
    def test_update(self):
        """测试更新"""
        user_id = self.client.insert('test_users', {
            'username': 'charlie',
            'email': 'charlie@example.com',
            'age': 35
        })
        
        updated = self.client.update(
            'test_users',
            {'age': 36},
            {'id': user_id}
        )
        self.assertEqual(updated, 1)
        
        # 验证更新
        users = self.client.select('test_users', condition={'id': user_id})
        self.assertEqual(users[0]['age'], 36)
    
    def test_delete(self):
        """测试删除"""
        user_id = self.client.insert('test_users', {
            'username': 'dave',
            'email': 'dave@example.com',
            'age': 40
        })
        
        deleted = self.client.delete('test_users', {'id': user_id})
        self.assertEqual(deleted, 1)
        
        # 验证删除
        users = self.client.select('test_users', condition={'id': user_id})
        self.assertEqual(len(users), 0)
    
    def test_count(self):
        """测试计数"""
        for i in range(5):
            self.client.insert('test_users', {
                'username': f'user{i}',
                'email': f'user{i}@example.com',
                'age': 20
            })
        
        count = self.client.count('test_users')
        self.assertEqual(count, 5)
    
    def test_exists(self):
        """测试存在性检查"""
        self.client.insert('test_users', {
            'username': 'eve',
            'email': 'eve@example.com',
            'age': 22
        })
        
        exists = self.client.exists('test_users', {'username': 'eve'})
        self.assertTrue(exists)
        
        not_exists = self.client.exists('test_users', {'username': 'frank'})
        self.assertFalse(not_exists)


class TestClientFactory(unittest.TestCase):
    """客户端工厂测试"""
    
    def test_create_sqlite_client(self):
        """测试创建SQLite客户端"""
        config = {'database': ':memory:'}
        client = ClientFactory.create('sqlite', config)
        self.assertIsInstance(client, SQLiteClient)
    
    def test_invalid_database_type(self):
        """测试无效的数据库类型"""
        with self.assertRaises(ConfigurationError):
            ClientFactory.create('invalid_db', {})
    
    def test_register_custom_client(self):
        """测试注册自定义客户端"""
        
        class MockClient(BaseClient):
            def connect(self):
                self._connected = True
                return True
            
            def disconnect(self):
                self._connected = False
                return True
            
            def is_connected(self):
                return self._connected
            
            def execute(self, query, params=None):
                return []
            
            def insert(self, table, data):
                return 1
            
            def update(self, table, data, condition):
                return 1
            
            def delete(self, table, condition):
                return 1
            
            def select(self, table, fields=None, condition=None, limit=None, offset=None, order_by=None):
                return []
        
        ClientFactory.register('mock', MockClient)
        client = ClientFactory.create('mock', {})
        self.assertIsInstance(client, MockClient)
    
    def test_list_available(self):
        """测试列出可用数据库"""
        available = ClientFactory.list_available()
        self.assertIn('sqlite', available)
        self.assertIn('mysql', available)
        self.assertIn('postgresql', available)


class TestQueryBuilder(unittest.TestCase):
    """查询构建器测试"""
    
    def test_simple_select(self):
        """测试简单查询"""
        builder = QueryBuilder()
        query = builder.table('users').select('id', 'name').build()
        self.assertEqual(query, "SELECT id, name FROM users")
    
    def test_select_with_where(self):
        """测试WHERE条件"""
        builder = QueryBuilder()
        query = (builder
                 .table('users')
                 .select('*')
                 .where("age > 18")
                 .where("status = 'active'")
                 .build())
        self.assertIn("WHERE", query)
        self.assertIn("age > 18", query)
        self.assertIn("status = 'active'", query)
    
    def test_select_with_join(self):
        """测试JOIN"""
        builder = QueryBuilder()
        query = (builder
                 .table('orders')
                 .select('*')
                 .join('users', 'orders.user_id = users.id')
                 .build())
        self.assertIn("INNER JOIN users ON orders.user_id = users.id", query)
    
    def test_select_with_left_join(self):
        """测试LEFT JOIN"""
        builder = QueryBuilder()
        query = (builder
                 .table('orders')
                 .select('*')
                 .left_join('users', 'orders.user_id = users.id')
                 .build())
        self.assertIn("LEFT JOIN users ON orders.user_id = users.id", query)
    
    def test_select_with_group_by(self):
        """测试GROUP BY"""
        builder = QueryBuilder()
        query = (builder
                 .table('orders')
                 .select('user_id', 'COUNT(*) as count')
                 .group_by('user_id')
                 .build())
        self.assertIn("GROUP BY user_id", query)
    
    def test_select_with_having(self):
        """测试HAVING"""
        builder = QueryBuilder()
        query = (builder
                 .table('orders')
                 .select('user_id', 'COUNT(*) as count')
                 .group_by('user_id')
                 .having('COUNT(*) > 5')
                 .build())
        self.assertIn("HAVING COUNT(*) > 5", query)
    
    def test_select_with_order_and_limit(self):
        """测试ORDER BY和LIMIT"""
        builder = QueryBuilder()
        query = (builder
                 .table('products')
                 .select('*')
                 .order_by('price', 'DESC')
                 .limit(10)
                 .build())
        self.assertIn("ORDER BY price DESC", query)
        self.assertIn("LIMIT 10", query)
    
    def test_paginate(self):
        """测试分页"""
        builder = QueryBuilder()
        query = (builder
                 .table('users')
                 .select('*')
                 .paginate(page=2, per_page=10)
                 .build())
        self.assertIn("LIMIT 10", query)
        self.assertIn("OFFSET 10", query)
    
    def test_builder_reset(self):
        """测试重置"""
        builder = QueryBuilder()
        query1 = builder.table('users').select('*').build()
        
        builder.reset()
        query2 = builder.table('products').select('*').build()
        
        self.assertIn("users", query1)
        self.assertIn("products", query2)


class TestBatchOperations(unittest.TestCase):
    """批量操作测试"""
    
    def setUp(self):
        """测试前准备"""
        from db_toolkit.clients.sqlite import SQLiteClient
        
        class ExtendedClient(SQLiteClient, BatchOperationsMixin):
            pass
        
        self.config = {'database': ':memory:'}
        self.client = ExtendedClient(self.config)
        self.client.connect()
        
        self.client.execute('''
            CREATE TABLE test_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER
            )
        ''')
    
    def tearDown(self):
        """测试后清理"""
        self.client.disconnect()
    
    def test_batch_insert(self):
        """测试批量插入"""
        items = [
            {'name': f'Item {i}', 'value': i * 10}
            for i in range(5)
        ]
        
        results = self.client.batch_insert('test_items', items)
        self.assertEqual(len(results), 5)
        
        # 验证插入
        all_items = self.client.select('test_items')
        self.assertEqual(len(all_items), 5)
    
    def test_batch_update(self):
        """测试批量更新"""
        # 先插入数据
        for i in range(3):
            self.client.insert('test_items', {'name': f'Item {i}', 'value': i})
        
        # 批量更新
        updates = [
            {'data': {'value': i * 100}, 'condition': {'name': f'Item {i}'}}
            for i in range(3)
        ]
        
        count = self.client.batch_update('test_items', updates)
        self.assertEqual(count, 3)
    
    def test_batch_delete(self):
        """测试批量删除"""
        # 先插入数据
        for i in range(5):
            self.client.insert('test_items', {'name': f'Item {i}', 'value': i})
        
        # 批量删除
        conditions = [{'name': f'Item {i}'} for i in range(3)]
        
        count = self.client.batch_delete('test_items', conditions)
        self.assertEqual(count, 3)
        
        # 验证删除
        remaining = self.client.select('test_items')
        self.assertEqual(len(remaining), 2)


class TestConfigManager(unittest.TestCase):
    """配置管理器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.config_file = 'test_config.json'
        
        # 创建测试配置
        manager = ConfigManager(self.config_file)
        manager.add('test_db', 'sqlite', {'database': ':memory:'}, set_as_default=True)
    
    def tearDown(self):
        """测试后清理"""
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
    
    def test_load_config(self):
        """测试加载配置"""
        manager = ConfigManager(self.config_file)
        configs = manager.list()
        self.assertIn('test_db', configs)
    
    def test_get_default_client(self):
        """测试获取默认客户端"""
        manager = ConfigManager(self.config_file)
        client = manager.get_client()
        self.assertIsInstance(client, SQLiteClient)
        client.disconnect()
    
    def test_add_config(self):
        """测试添加配置"""
        manager = ConfigManager(self.config_file)
        manager.add('new_db', 'sqlite', {'database': 'new.db'})
        
        configs = manager.list()
        self.assertIn('new_db', configs)
    
    def test_remove_config(self):
        """测试删除配置"""
        manager = ConfigManager(self.config_file)
        manager.add('temp_db', 'sqlite', {'database': 'temp.db'})
        
        success = manager.remove('temp_db')
        self.assertTrue(success)
        
        configs = manager.list()
        self.assertNotIn('temp_db', configs)


def run_tests():
    """运行所有测试"""
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 添加测试
    suite.addTests(loader.loadTestsFromTestCase(TestSQLiteClient))
    suite.addTests(loader.loadTestsFromTestCase(TestClientFactory))
    suite.addTests(loader.loadTestsFromTestCase(TestQueryBuilder))
    suite.addTests(loader.loadTestsFromTestCase(TestBatchOperations))
    suite.addTests(loader.loadTestsFromTestCase(TestConfigManager))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)
