"""
SQL查询构建器
提供流畅的API来构建SQL查询
"""

from typing import Any, List, Optional, Union
import logging


logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    SQL查询构建器
    
    使用流畅接口（Fluent Interface）模式构建SQL查询
    """
    
    def __init__(self):
        """初始化查询构建器"""
        self.reset()
    
    def reset(self) -> 'QueryBuilder':
        """
        重置查询构建器
        
        Returns:
            QueryBuilder: 返回自身以支持链式调用
        """
        self._table: Optional[str] = None
        self._fields: List[str] = ['*']
        self._conditions: List[str] = []
        self._joins: List[tuple] = []
        self._group_by: List[str] = []
        self._having: List[str] = []
        self._order_by: List[tuple] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._distinct: bool = False
        return self
    
    def table(self, table_name: str) -> 'QueryBuilder':
        """
        设置表名
        
        Args:
            table_name: 表名
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._table = table_name
        return self
    
    def select(self, *fields: str) -> 'QueryBuilder':
        """
        选择字段
        
        Args:
            *fields: 字段列表
            
        Returns:
            QueryBuilder: 返回自身
            
        Examples:
            >>> builder.select('id', 'name', 'email')
            >>> builder.select('COUNT(*) as total')
        """
        self._fields = list(fields) if fields else ['*']
        return self
    
    def distinct(self) -> 'QueryBuilder':
        """
        使用DISTINCT
        
        Returns:
            QueryBuilder: 返回自身
        """
        self._distinct = True
        return self
    
    def where(self, condition: str) -> 'QueryBuilder':
        """
        添加WHERE条件
        
        Args:
            condition: 条件表达式
            
        Returns:
            QueryBuilder: 返回自身
            
        Examples:
            >>> builder.where("age > 18")
            >>> builder.where("status = 'active'")
        """
        self._conditions.append(condition)
        return self
    
    def where_in(self, field: str, values: List[Any]) -> 'QueryBuilder':
        """
        添加IN条件
        
        Args:
            field: 字段名
            values: 值列表
            
        Returns:
            QueryBuilder: 返回自身
        """
        if not values:
            return self
        
        values_str = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
        self._conditions.append(f"{field} IN ({values_str})")
        return self
    
    def where_between(self, field: str, start: Any, end: Any) -> 'QueryBuilder':
        """
        添加BETWEEN条件
        
        Args:
            field: 字段名
            start: 起始值
            end: 结束值
            
        Returns:
            QueryBuilder: 返回自身
        """
        start_val = f"'{start}'" if isinstance(start, str) else start
        end_val = f"'{end}'" if isinstance(end, str) else end
        self._conditions.append(f"{field} BETWEEN {start_val} AND {end_val}")
        return self
    
    def where_like(self, field: str, pattern: str) -> 'QueryBuilder':
        """
        添加LIKE条件
        
        Args:
            field: 字段名
            pattern: 匹配模式
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._conditions.append(f"{field} LIKE '{pattern}'")
        return self
    
    def join(self, table: str, condition: str, join_type: str = 'INNER') -> 'QueryBuilder':
        """
        添加JOIN
        
        Args:
            table: 要JOIN的表
            condition: JOIN条件
            join_type: JOIN类型 (INNER, LEFT, RIGHT, FULL)
            
        Returns:
            QueryBuilder: 返回自身
            
        Examples:
            >>> builder.join('orders', 'users.id = orders.user_id')
            >>> builder.join('profiles', 'users.id = profiles.user_id', 'LEFT')
        """
        self._joins.append((join_type.upper(), table, condition))
        return self
    
    def left_join(self, table: str, condition: str) -> 'QueryBuilder':
        """LEFT JOIN的便捷方法"""
        return self.join(table, condition, 'LEFT')
    
    def right_join(self, table: str, condition: str) -> 'QueryBuilder':
        """RIGHT JOIN的便捷方法"""
        return self.join(table, condition, 'RIGHT')
    
    def group_by(self, *fields: str) -> 'QueryBuilder':
        """
        添加GROUP BY
        
        Args:
            *fields: 分组字段
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._group_by.extend(fields)
        return self
    
    def having(self, condition: str) -> 'QueryBuilder':
        """
        添加HAVING条件
        
        Args:
            condition: HAVING条件
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._having.append(condition)
        return self
    
    def order_by(self, field: str, direction: str = 'ASC') -> 'QueryBuilder':
        """
        添加ORDER BY
        
        Args:
            field: 排序字段
            direction: 排序方向 (ASC/DESC)
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._order_by.append((field, direction.upper()))
        return self
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """
        设置LIMIT
        
        Args:
            limit: 限制数量
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> 'QueryBuilder':
        """
        设置OFFSET
        
        Args:
            offset: 偏移量
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._offset = offset
        return self
    
    def paginate(self, page: int, per_page: int = 10) -> 'QueryBuilder':
        """
        分页查询
        
        Args:
            page: 页码（从1开始）
            per_page: 每页数量
            
        Returns:
            QueryBuilder: 返回自身
        """
        self._limit = per_page
        self._offset = (page - 1) * per_page
        return self
    
    def build(self) -> str:
        """
        构建SQL查询
        
        Returns:
            str: 完整的SQL查询语句
            
        Raises:
            ValueError: 当缺少必需参数时
        """
        if not self._table:
            raise ValueError("必须指定表名")
        
        # SELECT子句
        distinct_keyword = 'DISTINCT ' if self._distinct else ''
        fields = ', '.join(self._fields)
        query = f"SELECT {distinct_keyword}{fields} FROM {self._table}"
        
        # JOIN子句
        for join_type, table, condition in self._joins:
            query += f" {join_type} JOIN {table} ON {condition}"
        
        # WHERE子句
        if self._conditions:
            query += " WHERE " + " AND ".join(self._conditions)
        
        # GROUP BY子句
        if self._group_by:
            query += " GROUP BY " + ", ".join(self._group_by)
        
        # HAVING子句
        if self._having:
            query += " HAVING " + " AND ".join(self._having)
        
        # ORDER BY子句
        if self._order_by:
            order_parts = [f"{field} {direction}" for field, direction in self._order_by]
            query += " ORDER BY " + ", ".join(order_parts)
        
        # LIMIT子句
        if self._limit is not None:
            query += f" LIMIT {self._limit}"
        
        # OFFSET子句
        if self._offset is not None:
            query += f" OFFSET {self._offset}"
        
        logger.debug(f"构建SQL: {query}")
        return query
    
    def __str__(self) -> str:
        """返回构建的SQL语句"""
        try:
            return self.build()
        except ValueError:
            return "<QueryBuilder: incomplete>"
    
    def __repr__(self) -> str:
        """返回对象表示"""
        return f"<QueryBuilder table={self._table}>"
