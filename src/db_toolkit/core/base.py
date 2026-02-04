"""
数据库客户端抽象基类
定义了所有数据库客户端必须实现的接口
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


logger = logging.getLogger(__name__)


class BaseClient(ABC):
    """
    数据库客户端抽象基类
    
    所有数据库客户端都应继承此类并实现抽象方法
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据库客户端
        
        Args:
            config: 数据库配置字典
        """
        self.config = config
        self.connection = None
        self._connected = False
        self._validate_config()
    
    def _validate_config(self) -> None:
        """
        验证配置参数
        子类可以重写此方法以实现特定的配置验证
        """
        if not isinstance(self.config, dict):
            raise ValueError("配置必须是字典类型")
    
    @abstractmethod
    def connect(self) -> bool:
        """
        连接到数据库
        
        Returns:
            bool: 连接是否成功
            
        Raises:
            ConnectionError: 连接失败时抛出
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        断开数据库连接
        
        Returns:
            bool: 断开是否成功
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        检查是否已连接
        
        Returns:
            bool: 是否已连接
        """
        pass
    
    @abstractmethod
    def execute(self, query: str, params: Optional[Union[tuple, dict]] = None) -> Any:
        """
        执行原生查询
        
        Args:
            query: 查询语句
            params: 查询参数
            
        Returns:
            Any: 查询结果
            
        Raises:
            QueryError: 查询执行失败时抛出
        """
        pass
    
    @abstractmethod
    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """
        插入单条数据
        
        Args:
            table: 表名或集合名
            data: 要插入的数据
            
        Returns:
            Any: 插入的记录ID或结果
            
        Raises:
            QueryError: 插入失败时抛出
        """
        pass
    
    @abstractmethod
    def update(self, table: str, data: Dict[str, Any], 
               condition: Dict[str, Any]) -> int:
        """
        更新数据
        
        Args:
            table: 表名或集合名
            data: 要更新的数据
            condition: 更新条件
            
        Returns:
            int: 受影响的行数
            
        Raises:
            QueryError: 更新失败时抛出
        """
        pass
    
    @abstractmethod
    def delete(self, table: str, condition: Dict[str, Any]) -> int:
        """
        删除数据
        
        Args:
            table: 表名或集合名
            condition: 删除条件
            
        Returns:
            int: 受影响的行数
            
        Raises:
            QueryError: 删除失败时抛出
        """
        pass
    
    @abstractmethod
    def select(self, table: str, 
               fields: Optional[List[str]] = None,
               condition: Optional[Dict[str, Any]] = None,
               limit: Optional[int] = None,
               offset: Optional[int] = None,
               order_by: Optional[List[tuple]] = None) -> List[Dict]:
        """
        查询数据
        
        Args:
            table: 表名或集合名
            fields: 要查询的字段列表
            condition: 查询条件
            limit: 限制返回记录数
            offset: 偏移量
            order_by: 排序规则，格式为 [('field', 'ASC/DESC'), ...]
            
        Returns:
            List[Dict]: 查询结果列表
            
        Raises:
            QueryError: 查询失败时抛出
        """
        pass
    
    def ping(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            bool: 连接是否正常
        """
        try:
            return self.is_connected()
        except Exception:
            return False
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
        return False
    
    def __repr__(self) -> str:
        """返回对象的字符串表示"""
        return f"<{self.__class__.__name__} connected={self._connected}>"
