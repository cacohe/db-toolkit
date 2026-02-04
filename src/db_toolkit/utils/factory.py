"""
数据库客户端工厂
用于创建和管理数据库客户端实例
"""

from typing import Dict, Any, Type
import logging

from ..core.base import BaseClient
from ..clients import (
    MySQLClient,
    PostgreSQLClient,
    SQLiteClient,
    MongoDBClient,
    RedisClient,
    SupabaseClient,
)
from ..exceptions import ConfigurationError


logger = logging.getLogger(__name__)


class ClientFactory:
    """
    数据库客户端工厂类
    
    负责创建和注册数据库客户端
    """
    
    # 内置客户端注册表
    _clients: Dict[str, Type[BaseClient]] = {
        'mysql': MySQLClient,
        'postgresql': PostgreSQLClient,
        'postgres': PostgreSQLClient,  # 别名
        'sqlite': SQLiteClient,
        'sqlite3': SQLiteClient,  # 别名
        'mongodb': MongoDBClient,
        'mongo': MongoDBClient,  # 别名
        'redis': RedisClient,
        'supabase': SupabaseClient,
    }
    
    @classmethod
    def create(cls, db_type: str, config: Dict[str, Any]) -> BaseClient:
        """
        创建数据库客户端
        
        Args:
            db_type: 数据库类型 (mysql, postgresql, sqlite, mongodb, redis, supabase)
            config: 数据库配置字典
            
        Returns:
            BaseClient: 数据库客户端实例
            
        Raises:
            ConfigurationError: 当数据库类型不支持或配置无效时
            
        Examples:
            >>> config = {'host': 'localhost', 'user': 'root', 'password': 'pass', 'database': 'test'}
            >>> client = ClientFactory.create('mysql', config)
        """
        db_type = db_type.lower().strip()
        
        if db_type not in cls._clients:
            available = ', '.join(sorted(set(cls._clients.keys())))
            raise ConfigurationError(
                f"不支持的数据库类型: '{db_type}'\n"
                f"支持的类型: {available}"
            )
        
        client_class = cls._clients[db_type]
        
        try:
            logger.info(f"创建数据库客户端: {db_type}")
            return client_class(config)
        except Exception as e:
            logger.error(f"创建数据库客户端失败: {e}")
            raise ConfigurationError(f"创建数据库客户端失败: {e}")
    
    @classmethod
    def register(cls, db_type: str, client_class: Type[BaseClient]) -> None:
        """
        注册自定义数据库客户端
        
        Args:
            db_type: 数据库类型标识符
            client_class: 客户端类（必须继承BaseClient）
            
        Raises:
            ValueError: 当client_class不是BaseClient的子类时
            
        Examples:
            >>> class CustomClient(BaseClient):
            ...     pass
            >>> ClientFactory.register('custom', CustomClient)
        """
        if not issubclass(client_class, BaseClient):
            raise ValueError(f"{client_class.__name__} 必须继承 BaseClient")
        
        db_type = db_type.lower().strip()
        cls._clients[db_type] = client_class
        logger.info(f"注册自定义数据库客户端: {db_type} -> {client_class.__name__}")
    
    @classmethod
    def unregister(cls, db_type: str) -> bool:
        """
        注销数据库客户端
        
        Args:
            db_type: 数据库类型标识符
            
        Returns:
            bool: 是否成功注销
        """
        db_type = db_type.lower().strip()
        if db_type in cls._clients:
            del cls._clients[db_type]
            logger.info(f"注销数据库客户端: {db_type}")
            return True
        return False
    
    @classmethod
    def list_available(cls) -> list:
        """
        列出所有可用的数据库类型
        
        Returns:
            list: 数据库类型列表
        """
        return sorted(set(cls._clients.keys()))
    
    @classmethod
    def get_client_class(cls, db_type: str) -> Type[BaseClient]:
        """
        获取数据库客户端类
        
        Args:
            db_type: 数据库类型
            
        Returns:
            Type[BaseClient]: 客户端类
            
        Raises:
            ConfigurationError: 当数据库类型不存在时
        """
        db_type = db_type.lower().strip()
        if db_type not in cls._clients:
            raise ConfigurationError(f"数据库类型不存在: {db_type}")
        return cls._clients[db_type]


# 便捷函数
def create_client(db_type: str, config: Dict[str, Any]) -> BaseClient:
    """
    创建数据库客户端的便捷函数
    
    Args:
        db_type: 数据库类型
        config: 配置字典
        
    Returns:
        BaseClient: 数据库客户端实例
        
    Examples:
        >>> from db_toolkit import create_client
        >>> config = {'database': 'test.db'}
        >>> client = create_client('sqlite', config)
        >>> with client:
        ...     results = client.select('users')
    """
    return ClientFactory.create(db_type, config)
