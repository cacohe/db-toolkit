"""
MySQL数据库客户端
"""

from typing import Any, Dict, List, Optional
import logging

from ..core.sql_base import SQLBaseClient
from ..exceptions import ConnectionError, QueryError

logger = logging.getLogger(__name__)


class MySQLClient(SQLBaseClient):
    """MySQL数据库客户端实现"""

    @property
    def placeholder(self) -> str:
        return "%s"

    def _validate_config(self) -> None:
        """验证MySQL配置"""
        super()._validate_config()
        required = ['host', 'user', 'password', 'database']
        missing = [key for key in required if key not in self.config]
        if missing:
            raise ValueError(f"MySQL配置缺少必需字段: {', '.join(missing)}")

    def connect(self) -> bool:
        """连接MySQL数据库"""
        try:
            import mysql.connector

            self.connection = mysql.connector.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 3306),
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                **self.config.get('options', {})
            )
            self._connected = True
            logger.info(f"MySQL连接成功: {self.config['database']}@{self.config['host']}")
            return True

        except ImportError:
            raise ConnectionError("未安装mysql-connector-python库，请运行: pip install mysql-connector-python")
        except Exception as e:
            self._connected = False
            logger.error(f"MySQL连接失败: {e}")
            raise ConnectionError(f"MySQL连接失败: {e}")

    def disconnect(self) -> bool:
        """断开MySQL连接"""
        if self.connection:
            try:
                self.connection.close()
                self._connected = False
                logger.info("MySQL连接已关闭")
                return True
            except Exception as e:
                logger.error(f"关闭MySQL连接失败: {e}")
                return False
        return True

    def is_connected(self) -> bool:
        """检查MySQL连接状态"""
        if not self.connection:
            return False
        try:
            self.connection.ping(reconnect=False)
            return True
        except Exception:
            self._connected = False
            return False

    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """执行MySQL查询"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")

        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())

            if query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
            else:
                self.connection.commit()
                result = cursor.rowcount

            cursor.close()
            return result

        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            raise QueryError(f"查询执行失败: {e}")

    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """插入数据到MySQL"""
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
        """更新MySQL数据"""
        query, params = self._build_update_query(table, data, condition)
        result = self.execute(query, params)
        logger.debug(f"更新了 {result} 条记录")
        return result

    def delete(self, table: str, condition: Dict[str, Any]) -> int:
        """删除MySQL数据"""
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
        """查询MySQL数据"""
        query, params = self._build_select_query(
            table, fields, condition, limit, offset, order_by
        )
        result = self.execute(query, params)
        logger.debug(f"查询到 {len(result)} 条记录")
        return result
