"""
PostgreSQL数据库客户端
"""

from typing import Any, Dict, List, Optional
import logging

from ..core.sql_base import SQLBaseClient
from ..exceptions import ConnectionError, QueryError

logger = logging.getLogger(__name__)


class PostgreSQLClient(SQLBaseClient):
    """PostgreSQL数据库客户端实现"""
    
    @property
    def placeholder(self) -> str:
        return "%s"
    
    def _validate_config(self) -> None:
        """验证PostgreSQL配置"""
        super()._validate_config()
        required = ['host', 'user', 'password', 'database']
        missing = [key for key in required if key not in self.config]
        if missing:
            raise ValueError(f"PostgreSQL配置缺少必需字段: {', '.join(missing)}")
    
    def connect(self) -> bool:
        """连接PostgreSQL数据库"""
        try:
            import psycopg2
            
            self.connection = psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432),
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                **self.config.get('options', {})
            )
            self._connected = True
            logger.info(f"PostgreSQL连接成功: {self.config['database']}@{self.config['host']}")
            return True
            
        except ImportError:
            raise ConnectionError("未安装psycopg2库，请运行: pip install psycopg2-binary")
        except Exception as e:
            self._connected = False
            logger.error(f"PostgreSQL连接失败: {e}")
            raise ConnectionError(f"PostgreSQL连接失败: {e}")
    
    def disconnect(self) -> bool:
        """断开PostgreSQL连接"""
        if self.connection:
            try:
                self.connection.close()
                self._connected = False
                logger.info("PostgreSQL连接已关闭")
                return True
            except Exception as e:
                logger.error(f"关闭PostgreSQL连接失败: {e}")
                return False
        return True
    
    def is_connected(self) -> bool:
        """检查PostgreSQL连接状态"""
        if not self.connection:
            return False
        try:
            # PostgreSQL使用closed属性检查连接
            return not self.connection.closed
        except Exception:
            self._connected = False
            return False
    
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """执行PostgreSQL查询"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            import psycopg2.extras
            
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
        """插入数据到PostgreSQL"""
        query, params = self._build_insert_query(table, data)
        # PostgreSQL使用RETURNING获取插入的ID
        query += " RETURNING id"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            
            # 获取返回的ID
            result = cursor.fetchone()
            last_id = result[0] if result else None
            
            cursor.close()
            logger.debug(f"插入数据成功，ID: {last_id}")
            return last_id
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"插入数据失败: {e}")
            raise QueryError(f"插入数据失败: {e}")
    
    def update(self, table: str, data: Dict[str, Any], 
               condition: Dict[str, Any]) -> int:
        """更新PostgreSQL数据"""
        query, params = self._build_update_query(table, data, condition)
        result = self.execute(query, params)
        logger.debug(f"更新了 {result} 条记录")
        return result
    
    def delete(self, table: str, condition: Dict[str, Any]) -> int:
        """删除PostgreSQL数据"""
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
        """查询PostgreSQL数据"""
        query, params = self._build_select_query(
            table, fields, condition, limit, offset, order_by
        )
        result = self.execute(query, params)
        logger.debug(f"查询到 {len(result)} 条记录")
        return result
