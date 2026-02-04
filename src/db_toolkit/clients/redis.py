"""
Redis数据库客户端
"""

from typing import Any, Dict, List, Optional, Union
import logging
import json

from ..core.base import BaseClient
from ..exceptions import ConnectionError, QueryError, NotSupportedError

logger = logging.getLogger(__name__)


class RedisClient(BaseClient):
    """Redis数据库客户端实现"""
    
    def _validate_config(self) -> None:
        """验证Redis配置"""
        super()._validate_config()
        # Redis的配置比较灵活，不强制要求特定字段
    
    def connect(self) -> bool:
        """连接Redis数据库"""
        try:
            import redis
            
            self.connection = redis.Redis(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 6379),
                password=self.config.get('password'),
                db=self.config.get('db', 0),
                decode_responses=True,
                **self.config.get('options', {})
            )
            
            # 测试连接
            self.connection.ping()
            
            self._connected = True
            logger.info(f"Redis连接成功: {self.config.get('host', 'localhost')}:{self.config.get('port', 6379)}")
            return True
            
        except ImportError:
            raise ConnectionError("未安装redis库，请运行: pip install redis")
        except Exception as e:
            self._connected = False
            logger.error(f"Redis连接失败: {e}")
            raise ConnectionError(f"Redis连接失败: {e}")
    
    def disconnect(self) -> bool:
        """断开Redis连接"""
        if self.connection:
            try:
                self.connection.close()
                self._connected = False
                logger.info("Redis连接已关闭")
                return True
            except Exception as e:
                logger.error(f"关闭Redis连接失败: {e}")
                return False
        return True
    
    def is_connected(self) -> bool:
        """检查Redis连接状态"""
        if not self.connection:
            return False
        try:
            self.connection.ping()
            return True
        except Exception:
            self._connected = False
            return False
    
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        Redis不支持SQL查询
        
        Raises:
            NotSupportedError: 总是抛出此异常
        """
        raise NotSupportedError("Redis不支持SQL查询，请使用专用方法")
    
    # ============ 基础键值操作 ============
    
    def get(self, key: str) -> Optional[str]:
        """
        获取键值
        
        Args:
            key: 键名
            
        Returns:
            Optional[str]: 值，不存在返回None
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            return self.connection.get(key)
        except Exception as e:
            logger.error(f"获取键值失败: {e}")
            raise QueryError(f"获取键值失败: {e}")
    
    def set(self, key: str, value: str, ex: Optional[int] = None, 
            nx: bool = False, xx: bool = False) -> bool:
        """
        设置键值
        
        Args:
            key: 键名
            value: 值
            ex: 过期时间（秒）
            nx: 只在键不存在时设置
            xx: 只在键存在时设置
            
        Returns:
            bool: 是否设置成功
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            return self.connection.set(key, value, ex=ex, nx=nx, xx=xx)
        except Exception as e:
            logger.error(f"设置键值失败: {e}")
            raise QueryError(f"设置键值失败: {e}")
    
    def delete_key(self, *keys: str) -> int:
        """
        删除键
        
        Args:
            *keys: 要删除的键
            
        Returns:
            int: 删除的键数量
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            return self.connection.delete(*keys)
        except Exception as e:
            logger.error(f"删除键失败: {e}")
            raise QueryError(f"删除键失败: {e}")
    
    def exists_key(self, *keys: str) -> int:
        """
        检查键是否存在
        
        Args:
            *keys: 要检查的键
            
        Returns:
            int: 存在的键数量
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            return self.connection.exists(*keys)
        except Exception as e:
            logger.error(f"检查键存在失败: {e}")
            raise QueryError(f"检查键存在失败: {e}")
    
    # ============ Hash操作 ============
    
    def hget(self, name: str, key: str) -> Optional[str]:
        """获取Hash字段值"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            return self.connection.hget(name, key)
        except Exception as e:
            logger.error(f"获取Hash字段失败: {e}")
            raise QueryError(f"获取Hash字段失败: {e}")
    
    def hset(self, name: str, key: str = None, value: str = None, 
             mapping: Dict[str, Any] = None) -> int:
        """设置Hash字段"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            if mapping:
                return self.connection.hset(name, mapping=mapping)
            else:
                return self.connection.hset(name, key, value)
        except Exception as e:
            logger.error(f"设置Hash字段失败: {e}")
            raise QueryError(f"设置Hash字段失败: {e}")
    
    def hgetall(self, name: str) -> Dict[str, str]:
        """获取Hash所有字段"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            return self.connection.hgetall(name)
        except Exception as e:
            logger.error(f"获取Hash所有字段失败: {e}")
            raise QueryError(f"获取Hash所有字段失败: {e}")
    
    # ============ 实现基类抽象方法（使用Hash存储） ============
    
    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """
        使用Hash存储数据
        
        Args:
            table: 表名（用作键前缀）
            data: 数据，必须包含'id'字段
            
        Returns:
            str: 键名
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        # 获取或生成ID
        if 'id' in data:
            record_id = data['id']
        else:
            record_id = self.connection.incr(f'{table}:id')
            data['id'] = str(record_id)
        
        key = f"{table}:{record_id}"
        
        # 将数据转换为字符串存储
        str_data = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) 
                    for k, v in data.items()}
        
        try:
            self.hset(key, mapping=str_data)
            logger.debug(f"插入数据成功，键: {key}")
            return key
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            raise QueryError(f"插入数据失败: {e}")
    
    def update(self, table: str, data: Dict[str, Any], 
               condition: Dict[str, Any]) -> int:
        """更新Hash数据"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        if 'id' not in condition:
            raise ValueError("Redis更新操作需要提供id条件")
        
        key = f"{table}:{condition['id']}"
        
        if not self.exists_key(key):
            return 0
        
        # 转换数据
        str_data = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) 
                    for k, v in data.items()}
        
        try:
            self.hset(key, mapping=str_data)
            logger.debug(f"更新数据成功，键: {key}")
            return 1
        except Exception as e:
            logger.error(f"更新数据失败: {e}")
            raise QueryError(f"更新数据失败: {e}")
    
    def delete(self, table: str, condition: Dict[str, Any]) -> int:
        """删除Hash数据"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        if 'id' not in condition:
            raise ValueError("Redis删除操作需要提供id条件")
        
        key = f"{table}:{condition['id']}"
        
        try:
            count = self.delete_key(key)
            logger.debug(f"删除了 {count} 条数据")
            return count
        except Exception as e:
            logger.error(f"删除数据失败: {e}")
            raise QueryError(f"删除数据失败: {e}")
    
    def select(self, table: str,
               fields: Optional[List[str]] = None,
               condition: Optional[Dict[str, Any]] = None,
               limit: Optional[int] = None,
               offset: Optional[int] = None,
               order_by: Optional[List[tuple]] = None) -> List[Dict]:
        """查询Hash数据"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        # Redis简化实现，只支持通过id查询
        if not condition or 'id' not in condition:
            logger.warning("Redis select操作建议提供id条件以提高性能")
            return []
        
        key = f"{table}:{condition['id']}"
        
        try:
            data = self.hgetall(key)
            if not data:
                return []
            
            # 尝试解析JSON
            result = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except (json.JSONDecodeError, ValueError):
                    result[k] = v
            
            return [result]
            
        except Exception as e:
            logger.error(f"查询数据失败: {e}")
            raise QueryError(f"查询数据失败: {e}")
