"""
Database Toolkit - 通用数据库客户端工具包

一个强大且易用的Python数据库客户端，支持多种数据库类型，提供统一的API接口。

支持的数据库:
- MySQL
- PostgreSQL
- SQLite
- MongoDB
- Redis
- Supabase

基础使用:
    >>> from db_toolkit import create_client
    >>>
    >>> config = {'database': 'test.db'}
    >>> with create_client('sqlite', config) as client:
    ...     results = client.select('users')

高级功能:
    >>> from db_toolkit import ConfigManager, QueryBuilder
    >>>
    >>> # 配置管理
    >>> manager = ConfigManager('db_config.json')
    >>> client = manager.get_client('production')
    >>>
    >>> # SQL构建器
    >>> builder = QueryBuilder()
    >>> query = builder.table('users').select('*').where('age > 18').build()
"""

__version__ = '2.0.0'
__author__ = 'Database Toolkit Team'

import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 导出核心类
from .core import BaseClient, SQLBaseClient

# 导出所有客户端
from .clients import (
    MySQLClient,
    PostgreSQLClient,
    SQLiteClient,
    MongoDBClient,
    RedisClient,
    SupabaseClient,
)

# 导出工具类
from .utils import (
    ClientFactory,
    create_client,
    ConfigManager,
    QueryBuilder,
)

# 导出混入类
from .mixins import (
    BatchOperationsMixin,
    TransactionMixin,
)

# 导出异常
from .exceptions import (
    DatabaseError,
    ConnectionError,
    QueryError,
    ConfigurationError,
    ValidationError,
    NotSupportedError,
    TransactionError,
)

# 定义公开API
__all__ = [
    # 版本信息
    '__version__',
    '__author__',

    # 核心类
    'BaseClient',
    'SQLBaseClient',

    # 客户端
    'MySQLClient',
    'PostgreSQLClient',
    'SQLiteClient',
    'MongoDBClient',
    'RedisClient',
    'SupabaseClient',

    # 工具类
    'ClientFactory',
    'create_client',
    'ConfigManager',
    'QueryBuilder',

    # 混入类
    'BatchOperationsMixin',
    'TransactionMixin',

    # 异常
    'DatabaseError',
    'ConnectionError',
    'QueryError',
    'ConfigurationError',
    'ValidationError',
    'NotSupportedError',
    'TransactionError',
]


def get_version() -> str:
    """获取版本号"""
    return __version__


def list_available_databases() -> list:
    """列出所有可用的数据库类型"""
    return ClientFactory.list_available()


def configure_logging(level: int = logging.INFO,
                      format: str = None,
                      handlers: list = None) -> None:
    """
    配置日志

    Args:
        level: 日志级别
        format: 日志格式
        handlers: 日志处理器列表
    """
    logger = logging.getLogger('db_toolkit')
    logger.setLevel(level)

    if format:
        formatter = logging.Formatter(format)
        for handler in logger.handlers:
            handler.setFormatter(formatter)

    if handlers:
        logger.handlers = handlers