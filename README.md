# Database Toolkit

一个强大、易用、可扩展的Python数据库客户端工具包，支持多种数据库类型，提供统一的API接口。

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## ✨ 特性

- 🔌 **多数据库支持**: MySQL、PostgreSQL、SQLite、MongoDB、Redis、Supabase
- 🎯 **统一API**: 所有数据库使用相同的接口方法
- 🏗️ **模块化设计**: 清晰的代码结构，易于维护和扩展
- 🔒 **自动连接管理**: 支持上下文管理器
- 🏭 **工厂模式**: 便捷的客户端创建
- 📦 **批量操作**: 内置批量插入、更新、删除功能
- 💾 **事务支持**: 完整的事务管理
- 🔧 **查询构建器**: 流畅的SQL构建API
- ⚙️ **配置管理**: JSON配置文件支持
- 🔌 **可扩展**: 支持自定义数据库客户端
- 🛡️ **类型安全**: 完整的类型注解
- 📝 **异常处理**: 详细的异常类型和错误信息

## 📦 安装

```bash
# 克隆仓库
git clone https://github.com/your/db-toolkit.git
cd db-toolkit

# 安装依赖（根据需要选择）
# MySQL
pip install mysql-connector-python

# PostgreSQL
pip install psycopg2-binary

# MongoDB
pip install pymongo

# Redis
pip install redis

# Supabase
pip install supabase

# SQLite (Python内置，无需安装)
```

## 🚀 快速开始

### 基础用法

```python
from db_toolkit import create_client

# 连接SQLite数据库
config = {'database': 'myapp.db'}

with create_client('sqlite', config) as client:
    # 插入数据
    user_id = client.insert('users', {
        'name': 'Alice',
        'email': 'alice@example.com',
        'age': 25
    })
    
    # 查询数据
    users = client.select('users', condition={'age': 25})
    
    # 更新数据
    client.update('users', {'age': 26}, {'id': user_id})
    
    # 删除数据
    client.delete('users', {'id': user_id})
```

## 📚 详细文档

### 项目结构

```
db_toolkit/
├── __init__.py           # 包初始化，导出公共API
├── core/                 # 核心模块
│   ├── __init__.py
│   ├── base.py          # 抽象基类
│   └── sql_base.py      # SQL数据库基类
├── clients/             # 数据库客户端实现
│   ├── __init__.py
│   ├── mysql.py         # MySQL客户端
│   ├── postgresql.py    # PostgreSQL客户端
│   ├── sqlite.py        # SQLite客户端
│   ├── mongodb.py       # MongoDB客户端
│   ├── redis.py         # Redis客户端
│   └── supabase.py      # Supabase客户端
├── utils/               # 工具类
│   ├── __init__.py
│   ├── factory.py       # 客户端工厂
│   ├── config.py        # 配置管理器
│   └── query_builder.py # 查询构建器
├── mixins/              # 混入类
│   ├── __init__.py
│   ├── batch_ops.py     # 批量操作
│   └── transaction.py   # 事务管理
└── exceptions/          # 自定义异常
    └── __init__.py
```

### 支持的数据库

#### 1. MySQL

```python
config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'password',
    'database': 'mydb'
}

with create_client('mysql', config) as client:
    users = client.select('users', limit=10)
```

#### 2. PostgreSQL

```python
config = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'password',
    'database': 'mydb'
}

with create_client('postgresql', config) as client:
    count = client.count('users')
    exists = client.exists('users', {'email': 'user@example.com'})
```

#### 3. SQLite

```python
config = {
    'database': 'myapp.db'  # 或 ':memory:' 用于内存数据库
}

with create_client('sqlite', config) as client:
    # SQLite特有功能
    client.execute_script('''
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users VALUES (1, 'Alice');
    ''')
```

#### 4. MongoDB

```python
# 方式1: 连接字符串
config = {
    'connection_string': 'mongodb://localhost:27017/',
    'database': 'mydb'
}

# 方式2: 主机/端口
config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'mydb'
}

with create_client('mongodb', config) as client:
    # MongoDB特有功能
    doc_ids = client.insert_many('users', [
        {'name': 'Alice', 'age': 25},
        {'name': 'Bob', 'age': 30}
    ])
    
    # 聚合查询
    results = client.aggregate('orders', [
        {'$match': {'status': 'completed'}},
        {'$group': {'_id': '$user_id', 'total': {'$sum': '$amount'}}}
    ])
```

#### 5. Redis

```python
config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None
}

with create_client('redis', config) as client:
    # 字符串操作
    client.set('key', 'value', ex=3600)
    value = client.get('key')
    
    # Hash操作
    client.hset('user:1', mapping={'name': 'Alice', 'age': '25'})
    user = client.hgetall('user:1')
    
    # 通用接口
    client.insert('users', {'id': '1', 'name': 'Alice'})
```

#### 6. Supabase

```python
config = {
    'url': 'https://your-project.supabase.co',
    'key': 'your-anon-key'
}

with create_client('supabase', config) as client:
    # Supabase特有功能
    data = client.upsert('users', {
        'id': 1,
        'name': 'Alice',
        'email': 'alice@example.com'
    })
    
    # 调用RPC函数
    result = client.rpc('get_user_stats', {'user_id': 1})
```

### 高级功能

#### 配置管理

```python
from db_toolkit import ConfigManager

# 创建配置管理器
manager = ConfigManager('db_config.json')

# 添加配置
manager.add('production', 'postgresql', {
    'host': 'prod-server.com',
    'user': 'app',
    'password': 'secret',
    'database': 'prod_db'
}, set_as_default=True)

manager.add('development', 'sqlite', {
    'database': 'dev.db'
})

# 使用配置
with manager.get_client('production') as client:
    # 使用生产数据库
    users = client.select('users')

# 使用默认配置
with manager.get_client() as client:
    # 自动使用production（设置为默认）
    pass
```

#### 查询构建器

```python
from db_toolkit import QueryBuilder

builder = QueryBuilder()

# 构建复杂查询
query = (builder
    .table('orders')
    .select('users.name', 'orders.total', 'orders.created_at')
    .left_join('users', 'orders.user_id = users.id')
    .where("orders.status = 'completed'")
    .where("orders.total > 100")
    .group_by('users.name')
    .having('SUM(orders.total) > 1000')
    .order_by('orders.total', 'DESC')
    .limit(10)
    .build())

print(query)
# SELECT users.name, orders.total, orders.created_at FROM orders
# LEFT JOIN users ON orders.user_id = users.id
# WHERE orders.status = 'completed' AND orders.total > 100
# GROUP BY users.name
# HAVING SUM(orders.total) > 1000
# ORDER BY orders.total DESC
# LIMIT 10

# 使用查询
with client:
    results = client.execute(query)
```

#### 批量操作

```python
from db_toolkit.clients.sqlite import SQLiteClient
from db_toolkit.mixins import BatchOperationsMixin

# 创建扩展客户端
class ExtendedClient(SQLiteClient, BatchOperationsMixin):
    pass

config = {'database': 'app.db'}

with ExtendedClient(config) as client:
    # 批量插入
    users = [
        {'name': f'User{i}', 'email': f'user{i}@example.com'}
        for i in range(100)
    ]
    results = client.batch_insert('users', users, chunk_size=20)
    
    # 批量更新
    updates = [
        {'data': {'status': 'active'}, 'condition': {'id': i}}
        for i in range(1, 51)
    ]
    count = client.batch_update('users', updates)
    
    # 批量删除
    conditions = [{'id': i} for i in range(51, 101)]
    count = client.batch_delete('users', conditions)
    
    # Upsert (插入或更新)
    client.upsert('users', {
        'email': 'alice@example.com',
        'name': 'Alice Updated',
        'status': 'active'
    }, unique_fields=['email'])
```

#### 事务管理

```python
from db_toolkit.clients.postgresql import PostgreSQLClient
from db_toolkit.mixins import TransactionMixin

class TransactionalClient(PostgreSQLClient, TransactionMixin):
    pass

config = {
    'host': 'localhost',
    'user': 'postgres',
    'password': 'password',
    'database': 'mydb'
}

with TransactionalClient(config) as client:
    # 方式1: 使用上下文管理器
    try:
        with client.transaction():
            client.insert('orders', {'user_id': 1, 'total': 100})
            client.update('users', {'balance': 900}, {'id': 1})
            # 自动提交
    except Exception as e:
        # 自动回滚
        print(f"Transaction failed: {e}")
    
    # 方式2: 手动控制
    try:
        client.begin()
        client.insert('logs', {'message': 'Order created'})
        client.insert('logs', {'message': 'Balance updated'})
        client.commit()
    except Exception:
        client.rollback()
    
    # 方式3: 使用保存点 (PostgreSQL)
    client.begin()
    client.savepoint('sp1')
    try:
        client.insert('temp_data', {'value': 'test'})
        # 出错...
        raise Exception("Something went wrong")
    except Exception:
        client.rollback_to_savepoint('sp1')
    client.commit()
```

### 扩展自定义客户端

```python
from db_toolkit import BaseClient, ClientFactory

class CustomDBClient(BaseClient):
    """自定义数据库客户端"""
    
    def connect(self) -> bool:
        # 实现连接逻辑
        self._connected = True
        return True
    
    def disconnect(self) -> bool:
        self._connected = False
        return True
    
    def is_connected(self) -> bool:
        return self._connected
    
    def execute(self, query: str, params=None):
        # 实现查询执行
        return []
    
    def insert(self, table: str, data: dict):
        # 实现插入
        return 1
    
    def update(self, table: str, data: dict, condition: dict):
        return 1
    
    def delete(self, table: str, condition: dict):
        return 1
    
    def select(self, table: str, fields=None, condition=None, 
               limit=None, offset=None, order_by=None):
        return []

# 注册自定义客户端
ClientFactory.register('custom_db', CustomDBClient)

# 使用自定义客户端
with create_client('custom_db', {}) as client:
    results = client.select('my_table')
```

### 异常处理

```python
from db_toolkit import create_client
from db_toolkit.exceptions import (
    ConnectionError,
    QueryError,
    ConfigurationError,
    TransactionError
)

try:
    # 配置错误
    client = create_client('invalid_type', {})
except ConfigurationError as e:
    print(f"Configuration error: {e}")

try:
    # 连接错误
    config = {'host': 'invalid', 'user': 'user', 'password': 'pass', 'database': 'db'}
    client = create_client('mysql', config)
    client.connect()
except ConnectionError as e:
    print(f"Connection error: {e}")

try:
    # 查询错误
    with create_client('sqlite', {'database': ':memory:'}) as client:
        client.select('nonexistent_table')
except QueryError as e:
    print(f"Query error: {e}")
```

## 🧪 测试

```bash
# 运行所有测试
python tests.py

# 运行示例
python examples.py
```

## 📋 配置文件示例

```json
{
  "databases": {
    "production": {
      "type": "postgresql",
      "config": {
        "host": "prod-server.com",
        "port": 5432,
        "user": "app_user",
        "password": "secure_password",
        "database": "prod_db"
      }
    },
    "development": {
      "type": "sqlite",
      "config": {
        "database": "./dev.db"
      }
    },
    "cache": {
      "type": "redis",
      "config": {
        "host": "localhost",
        "port": 6379,
        "db": 0
      }
    }
  },
  "default": "development"
}
```

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 许可证

MIT License

## 📞 联系方式

- GitHub: [your-repo](https://github.com/your/db-toolkit)
- Email: your-email@example.com

## 🗺️ 路线图

- [ ] 连接池支持
- [ ] 异步操作支持
- [ ] 数据迁移工具
- [ ] ORM功能
- [ ] 更多数据库支持（CockroachDB、Cassandra等）
- [ ] 性能监控和分析
- [ ] 查询缓存

## 📝 更新日志

### v2.0.0 (2024-02-04)
- ✨ 完全重构的模块化架构
- 🏗️ 清晰的项目结构
- 📦 批量操作支持
- 💾 事务管理
- 🔧 查询构建器
- ⚙️ 配置管理器
- 🛡️ 完整的异常处理
- 📝 完整的类型注解
- ✅ 30个单元测试

### v1.0.0 (2024-01-01)
- 🎉 初始版本
- 支持6种数据库
- 基础CRUD操作