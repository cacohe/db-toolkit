"""
SQLite数据库客户端
"""

from typing import Any, Dict, List, Optional
import logging
import sqlite3

from ..core.sql_base import SQLBaseClient
from ..exceptions import ConnectionError, QueryError

logger = logging.getLogger(__name__)


class SQLiteClient(SQLBaseClient):
    """SQLite数据库客户端实现"""
    
    @property
    def placeholder(self) -> str:
        return "?"
    
    def _validate_config(self) -> None:
        """验证SQLite配置"""
        super()._validate_config()
        if 'database' not in self.config:
            raise ValueError("SQLite配置缺少必需字段: database")
    
    def connect(self) -> bool:
        """连接SQLite数据库"""
        try:
            database = self.config.get('database', ':memory:')
            self.connection = sqlite3.connect(database)
            self.connection.row_factory = sqlite3.Row
            self._connected = True
            logger.info(f"SQLite连接成功: {database}")
            return True
            
        except Exception as e:
            self._connected = False
            logger.error(f"SQLite连接失败: {e}")
            raise ConnectionError(f"SQLite连接失败: {e}")
    
    def disconnect(self) -> bool:
        """断开SQLite连接"""
        if self.connection:
            try:
                self.connection.close()
                self._connected = False
                logger.info("SQLite连接已关闭")
                return True
            except Exception as e:
                logger.error(f"关闭SQLite连接失败: {e}")
                return False
        return True
    
    def is_connected(self) -> bool:
        """检查SQLite连接状态"""
        if not self.connection:
            return False
        try:
            # 尝试执行一个简单的查询
            self.connection.execute("SELECT 1")
            return True
        except Exception:
            self._connected = False
            return False
    
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """执行SQLite查询"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            
            if query.strip().upper().startswith('SELECT'):
                result = [dict(row) for row in cursor.fetchall()]
            else:
                self.connection.commit()
                result = cursor.rowcount
            
            cursor.close()
            return result
            
        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            raise QueryError(f"查询执行失败: {e}")
    
    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """插入数据到SQLite"""
        query, params = self._build_insert_query(table, data)
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            last_id = cursor.lastrowid
            cursor.close()
            logger.debug(f"插入数据成功，ID: {last_id}")
            return last_id
            
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            raise QueryError(f"插入数据失败: {e}")
    
    def update(self, table: str, data: Dict[str, Any], 
               condition: Dict[str, Any]) -> int:
        """更新SQLite数据"""
        query, params = self._build_update_query(table, data, condition)
        result = self.execute(query, params)
        logger.debug(f"更新了 {result} 条记录")
        return result
    
    def delete(self, table: str, condition: Dict[str, Any]) -> int:
        """删除SQLite数据"""
        query, params = self._build_delete_query(table, condition)
        result = self.execute(query, params)
        logger.debug(f"删除了 {result} 条记录")
        return result
    
    def select(self, table: str,
               fields: Optional[List[str]] = None,
               condition: Optional[Dict[str, Any]] = None,
               limit: Optional[int] = None,
               offset: Optional[int] = None,
               order_by: Optional[List[tuple]] = None) -> List[Dict]:
        """查询SQLite数据"""
        query, params = self._build_select_query(
            table, fields, condition, limit, offset, order_by
        )
        result = self.execute(query, params)
        logger.debug(f"查询到 {len(result)} 条记录")
        return result
    
    def execute_script(self, script: str) -> None:
        """
        执行SQL脚本
        
        Args:
            script: SQL脚本内容
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            self.connection.executescript(script)
            self.connection.commit()
            logger.debug("SQL脚本执行成功")
        except Exception as e:
            logger.error(f"SQL脚本执行失败: {e}")
            raise QueryError(f"SQL脚本执行失败: {e}")
