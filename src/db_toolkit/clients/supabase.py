"""
Supabase数据库客户端
"""

from typing import Any, Dict, List, Optional, Union
import logging

from ..core.base import BaseClient
from ..exceptions import ConnectionError, QueryError, NotSupportedError

logger = logging.getLogger(__name__)


class SupabaseClient(BaseClient):
    """Supabase数据库客户端实现"""
    
    def _validate_config(self) -> None:
        """验证Supabase配置"""
        super()._validate_config()
        required = ['url', 'key']
        missing = [key for key in required if key not in self.config]
        if missing:
            raise ValueError(f"Supabase配置缺少必需字段: {', '.join(missing)}")
    
    def connect(self) -> bool:
        """连接Supabase"""
        try:
            from supabase import create_client, Client
            
            url = self.config['url']
            key = self.config['key']
            
            self.connection: Client = create_client(url, key)
            self._connected = True
            logger.info(f"Supabase连接成功: {url}")
            return True
            
        except ImportError:
            raise ConnectionError("未安装supabase库，请运行: pip install supabase")
        except Exception as e:
            self._connected = False
            logger.error(f"Supabase连接失败: {e}")
            raise ConnectionError(f"Supabase连接失败: {e}")
    
    def disconnect(self) -> bool:
        """断开Supabase连接"""
        # Supabase客户端不需要显式断开连接
        self._connected = False
        logger.info("Supabase会话结束")
        return True
    
    def is_connected(self) -> bool:
        """检查Supabase连接状态"""
        # Supabase使用REST API，只要有客户端实例就认为是连接的
        return self.connection is not None and self._connected
    
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        Supabase不直接支持SQL查询
        
        Raises:
            NotSupportedError: 总是抛出此异常
        """
        raise NotSupportedError("Supabase使用REST API，请使用insert/update/delete/select方法")
    
    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """插入数据到Supabase"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            result = self.connection.table(table).insert(data).execute()
            logger.debug(f"插入数据成功: {result.data}")
            return result.data
            
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            raise QueryError(f"插入数据失败: {e}")
    
    def insert_many(self, table: str, data_list: List[Dict[str, Any]]) -> List[Dict]:
        """
        批量插入数据
        
        Args:
            table: 表名
            data_list: 数据列表
            
        Returns:
            List[Dict]: 插入的数据
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            result = self.connection.table(table).insert(data_list).execute()
            logger.debug(f"批量插入 {len(result.data)} 条数据")
            return result.data
            
        except Exception as e:
            logger.error(f"批量插入数据失败: {e}")
            raise QueryError(f"批量插入数据失败: {e}")
    
    def update(self, table: str, data: Dict[str, Any], 
               condition: Dict[str, Any]) -> int:
        """更新Supabase数据"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            query = self.connection.table(table).update(data)
            
            # 添加条件
            for key, value in condition.items():
                query = query.eq(key, value)
            
            result = query.execute()
            count = len(result.data)
            logger.debug(f"更新了 {count} 条记录")
            return count
            
        except Exception as e:
            logger.error(f"更新数据失败: {e}")
            raise QueryError(f"更新数据失败: {e}")
    
    def delete(self, table: str, condition: Dict[str, Any]) -> int:
        """删除Supabase数据"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            query = self.connection.table(table).delete()
            
            # 添加条件
            for key, value in condition.items():
                query = query.eq(key, value)
            
            result = query.execute()
            count = len(result.data)
            logger.debug(f"删除了 {count} 条记录")
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
        """查询Supabase数据"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            # 构建字段选择
            fields_str = ','.join(fields) if fields else '*'
            query = self.connection.table(table).select(fields_str)
            
            # 添加条件
            if condition:
                for key, value in condition.items():
                    query = query.eq(key, value)
            
            # 排序
            if order_by:
                for field, direction in order_by:
                    desc = direction.upper() == 'DESC'
                    query = query.order(field, desc=desc)
            
            # 分页
            if offset is not None:
                query = query.range(offset, offset + (limit or 999999))
            elif limit is not None:
                query = query.limit(limit)
            
            result = query.execute()
            logger.debug(f"查询到 {len(result.data)} 条记录")
            return result.data
            
        except Exception as e:
            logger.error(f"查询数据失败: {e}")
            raise QueryError(f"查询数据失败: {e}")
    
    def upsert(self, table: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Any:
        """
        插入或更新数据（如果存在则更新）
        
        Args:
            table: 表名
            data: 单条或多条数据
            
        Returns:
            插入或更新的数据
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            result = self.connection.table(table).upsert(data).execute()
            logger.debug(f"Upsert操作成功: {result.data}")
            return result.data
            
        except Exception as e:
            logger.error(f"Upsert操作失败: {e}")
            raise QueryError(f"Upsert操作失败: {e}")
    
    def rpc(self, function_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        调用Supabase存储过程
        
        Args:
            function_name: 函数名
            params: 参数
            
        Returns:
            函数返回结果
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            result = self.connection.rpc(function_name, params or {}).execute()
            logger.debug(f"调用RPC函数成功: {function_name}")
            return result.data
            
        except Exception as e:
            logger.error(f"调用RPC函数失败: {e}")
            raise QueryError(f"调用RPC函数失败: {e}")
