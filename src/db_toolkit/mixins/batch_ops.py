"""
批量操作混入类
为数据库客户端提供批量操作功能
"""

from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class BatchOperationsMixin:
    """
    批量操作混入类
    
    为数据库客户端添加批量插入、更新、删除功能
    """

    def batch_insert(self, table: str, data_list: List[Dict[str, Any]],
                     chunk_size: int = 100) -> List[Any]:
        """
        批量插入数据
        
        Args:
            table: 表名
            data_list: 数据列表
            chunk_size: 每批次大小
            
        Returns:
            List[Any]: 插入结果列表
            
        Examples:
            >>> data = [
            ...     {'name': 'Alice', 'age': 25},
            ...     {'name': 'Bob', 'age': 30}
            ... ]
            >>> results = client.batch_insert('users', data)
        """
        if not data_list:
            logger.warning("批量插入数据列表为空")
            return []

        results = []
        total = len(data_list)

        # 分批处理
        for i in range(0, total, chunk_size):
            chunk = data_list[i:i + chunk_size]

            for data in chunk:
                try:
                    result = self.insert(table, data)
                    results.append(result)
                except Exception as e:
                    logger.error(f"批量插入失败: {e}")
                    results.append(None)

            logger.debug(f"批量插入进度: {min(i + chunk_size, total)}/{total}")

        success_count = sum(1 for r in results if r is not None)
        logger.info(f"批量插入完成: {success_count}/{total} 成功")

        return results

    def batch_update(self, table: str, updates: List[Dict[str, Any]],
                     chunk_size: int = 100) -> int:
        """
        批量更新数据
        
        Args:
            table: 表名
            updates: 更新列表，每项包含 {'data': {...}, 'condition': {...}}
            chunk_size: 每批次大小
            
        Returns:
            int: 总共更新的行数
            
        Examples:
            >>> updates = [
            ...     {'data': {'age': 26}, 'condition': {'name': 'Alice'}},
            ...     {'data': {'age': 31}, 'condition': {'name': 'Bob'}}
            ... ]
            >>> count = client.batch_update('users', updates)
        """
        if not updates:
            logger.warning("批量更新列表为空")
            return 0

        total_count = 0
        total = len(updates)

        # 分批处理
        for i in range(0, total, chunk_size):
            chunk = updates[i:i + chunk_size]

            for update_item in chunk:
                try:
                    data = update_item.get('data', {})
                    condition = update_item.get('condition', {})

                    if not data or not condition:
                        logger.warning(f"更新项缺少data或condition: {update_item}")
                        continue

                    count = self.update(table, data, condition)
                    total_count += count

                except Exception as e:
                    logger.error(f"批量更新失败: {e}")

            logger.debug(f"批量更新进度: {min(i + chunk_size, total)}/{total}")

        logger.info(f"批量更新完成: {total_count} 条记录")
        return total_count

    def batch_delete(self, table: str, conditions: List[Dict[str, Any]],
                     chunk_size: int = 100) -> int:
        """
        批量删除数据
        
        Args:
            table: 表名
            conditions: 删除条件列表
            chunk_size: 每批次大小
            
        Returns:
            int: 总共删除的行数
            
        Examples:
            >>> conditions = [
            ...     {'name': 'Alice'},
            ...     {'name': 'Bob'}
            ... ]
            >>> count = client.batch_delete('users', conditions)
        """
        if not conditions:
            logger.warning("批量删除条件列表为空")
            return 0

        total_count = 0
        total = len(conditions)

        # 分批处理
        for i in range(0, total, chunk_size):
            chunk = conditions[i:i + chunk_size]

            for condition in chunk:
                try:
                    count = self.delete(table, condition)
                    total_count += count
                except Exception as e:
                    logger.error(f"批量删除失败: {e}")

            logger.debug(f"批量删除进度: {min(i + chunk_size, total)}/{total}")

        logger.info(f"批量删除完成: {total_count} 条记录")
        return total_count

    def upsert(self, table: str, data: Dict[str, Any],
               unique_fields: List[str]) -> Any:
        """
        插入或更新（如果存在则更新，否则插入）
        
        Args:
            table: 表名
            data: 数据
            unique_fields: 用于判断唯一性的字段
            
        Returns:
            插入或更新结果
            
        Examples:
            >>> data = {'email': 'user@example.com', 'name': 'Alice', 'age': 25}
            >>> client.upsert('users', data, ['email'])
        """
        # 构建查询条件
        condition = {field: data[field] for field in unique_fields if field in data}

        if not condition:
            logger.warning("upsert条件为空，执行插入操作")
            return self.insert(table, data)

        # 检查记录是否存在
        existing = self.select(table, condition=condition, limit=1)

        if existing:
            # 更新
            logger.debug(f"记录已存在，执行更新: {condition}")
            self.update(table, data, condition)
            return condition
        else:
            # 插入
            logger.debug(f"记录不存在，执行插入: {condition}")
            return self.insert(table, data)
