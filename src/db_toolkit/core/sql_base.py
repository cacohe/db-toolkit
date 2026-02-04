"""
SQL数据库客户端基类
提供关系型数据库的通用功能
"""

from abc import abstractmethod
from typing import Any, Dict, List, Optional

from .base import BaseClient


class SQLBaseClient(BaseClient):
    """
    SQL数据库客户端基类
    为关系型数据库提供通用的SQL构建方法
    """

    @property
    @abstractmethod
    def placeholder(self) -> str:
        """
        SQL参数占位符
        不同数据库使用不同的占位符：
        - MySQL/PostgreSQL: %s
        - SQLite: ?
        
        Returns:
            str: 占位符字符
        """
        pass

    def _build_insert_query(self, table: str, data: Dict[str, Any]) -> tuple:
        """
        构建INSERT查询
        
        Args:
            table: 表名
            data: 要插入的数据
            
        Returns:
            tuple: (查询语句, 参数元组)
        """
        fields = ', '.join(data.keys())
        placeholders = ', '.join([self.placeholder] * len(data))
        query = f"INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        return query, tuple(data.values())

    def _build_update_query(self, table: str, data: Dict[str, Any],
                            condition: Dict[str, Any]) -> tuple:
        """
        构建UPDATE查询
        
        Args:
            table: 表名
            data: 要更新的数据
            condition: 更新条件
            
        Returns:
            tuple: (查询语句, 参数元组)
        """
        set_clause = ', '.join([f"{k} = {self.placeholder}" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = {self.placeholder}" for k in condition.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = tuple(data.values()) + tuple(condition.values())
        return query, params

    def _build_delete_query(self, table: str, condition: Dict[str, Any]) -> tuple:
        """
        构建DELETE查询
        
        Args:
            table: 表名
            condition: 删除条件
            
        Returns:
            tuple: (查询语句, 参数元组)
        """
        where_clause = ' AND '.join([f"{k} = {self.placeholder}" for k in condition.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"
        return query, tuple(condition.values())

    def _build_select_query(self, table: str,
                            fields: Optional[List[str]] = None,
                            condition: Optional[Dict[str, Any]] = None,
                            limit: Optional[int] = None,
                            offset: Optional[int] = None,
                            order_by: Optional[List[tuple]] = None) -> tuple:
        """
        构建SELECT查询
        
        Args:
            table: 表名
            fields: 字段列表
            condition: 查询条件
            limit: 限制数量
            offset: 偏移量
            order_by: 排序规则
            
        Returns:
            tuple: (查询语句, 参数元组)
        """
        # SELECT子句
        fields_str = ', '.join(fields) if fields else '*'
        query = f"SELECT {fields_str} FROM {table}"
        params = []

        # WHERE子句
        if condition:
            where_clause = ' AND '.join([f"{k} = {self.placeholder}" for k in condition.keys()])
            query += f" WHERE {where_clause}"
            params.extend(condition.values())

        # ORDER BY子句
        if order_by:
            order_parts = [f"{field} {direction}" for field, direction in order_by]
            query += " ORDER BY " + ", ".join(order_parts)

        # LIMIT子句
        if limit is not None:
            query += f" LIMIT {limit}"

        # OFFSET子句
        if offset is not None:
            query += f" OFFSET {offset}"

        return query, tuple(params)

    def count(self, table: str, condition: Optional[Dict[str, Any]] = None) -> int:
        """
        计数查询
        
        Args:
            table: 表名
            condition: 查询条件
            
        Returns:
            int: 记录数量
        """
        query = f"SELECT COUNT(*) as count FROM {table}"
        params = ()

        if condition:
            where_clause = ' AND '.join([f"{k} = {self.placeholder}" for k in condition.keys()])
            query += f" WHERE {where_clause}"
            params = tuple(condition.values())

        result = self.execute(query, params)
        if result and len(result) > 0:
            return result[0].get('count', 0)
        return 0

    def exists(self, table: str, condition: Dict[str, Any]) -> bool:
        """
        检查记录是否存在
        
        Args:
            table: 表名
            condition: 查询条件
            
        Returns:
            bool: 是否存在
        """
        return self.count(table, condition) > 0
