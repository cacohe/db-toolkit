"""
事务混入类
为支持事务的数据库客户端提供事务管理功能
"""

import logging
from contextlib import contextmanager

from ..exceptions import TransactionError


logger = logging.getLogger(__name__)


class TransactionMixin:
    """
    事务混入类
    
    为数据库客户端添加事务管理功能
    注意：仅适用于支持事务的数据库（如MySQL, PostgreSQL, SQLite）
    """
    
    def begin(self) -> None:
        """
        开始事务
        
        Raises:
            TransactionError: 开始事务失败时
        """
        if not hasattr(self, 'connection') or not self.connection:
            raise TransactionError("数据库未连接")
        
        try:
            # 不同数据库的事务开始方式
            if hasattr(self.connection, 'begin'):
                self.connection.begin()
            elif hasattr(self.connection, 'autocommit'):
                self.connection.autocommit = False
            else:
                # 对于不显式支持begin的数据库，通常自动开始事务
                pass
            
            logger.debug("事务已开始")
            
        except Exception as e:
            logger.error(f"开始事务失败: {e}")
            raise TransactionError(f"开始事务失败: {e}")
    
    def commit(self) -> None:
        """
        提交事务
        
        Raises:
            TransactionError: 提交事务失败时
        """
        if not hasattr(self, 'connection') or not self.connection:
            raise TransactionError("数据库未连接")
        
        try:
            if hasattr(self.connection, 'commit'):
                self.connection.commit()
                logger.debug("事务已提交")
            else:
                logger.warning("数据库不支持事务提交")
                
        except Exception as e:
            logger.error(f"提交事务失败: {e}")
            raise TransactionError(f"提交事务失败: {e}")
    
    def rollback(self) -> None:
        """
        回滚事务
        
        Raises:
            TransactionError: 回滚事务失败时
        """
        if not hasattr(self, 'connection') or not self.connection:
            raise TransactionError("数据库未连接")
        
        try:
            if hasattr(self.connection, 'rollback'):
                self.connection.rollback()
                logger.debug("事务已回滚")
            else:
                logger.warning("数据库不支持事务回滚")
                
        except Exception as e:
            logger.error(f"回滚事务失败: {e}")
            raise TransactionError(f"回滚事务失败: {e}")
    
    @contextmanager
    def transaction(self):
        """
        事务上下文管理器
        
        Yields:
            self: 数据库客户端实例
            
        Examples:
            >>> with client.transaction():
            ...     client.insert('users', {'name': 'Alice'})
            ...     client.insert('users', {'name': 'Bob'})
            # 如果没有异常，自动提交；如果有异常，自动回滚
        """
        try:
            self.begin()
            yield self
            self.commit()
            logger.info("事务成功提交")
            
        except Exception as e:
            self.rollback()
            logger.error(f"事务回滚: {e}")
            raise
    
    def execute_in_transaction(self, *operations):
        """
        在事务中执行多个操作
        
        Args:
            *operations: 操作函数列表，每个函数接收client作为参数
            
        Returns:
            执行结果列表
            
        Examples:
            >>> def op1(client):
            ...     return client.insert('users', {'name': 'Alice'})
            >>> def op2(client):
            ...     return client.insert('users', {'name': 'Bob'})
            >>> results = client.execute_in_transaction(op1, op2)
        """
        results = []
        
        try:
            self.begin()
            
            for operation in operations:
                if callable(operation):
                    result = operation(self)
                    results.append(result)
                else:
                    logger.warning(f"跳过非可调用对象: {operation}")
            
            self.commit()
            logger.info(f"事务成功完成 {len(results)} 个操作")
            return results
            
        except Exception as e:
            self.rollback()
            logger.error(f"事务执行失败: {e}")
            raise TransactionError(f"事务执行失败: {e}")
    
    def savepoint(self, name: str) -> None:
        """
        创建保存点
        
        Args:
            name: 保存点名称
            
        Note:
            仅PostgreSQL和部分数据库支持保存点
        """
        try:
            if hasattr(self, 'execute'):
                self.execute(f"SAVEPOINT {name}")
                logger.debug(f"创建保存点: {name}")
            else:
                logger.warning("数据库不支持保存点")
        except Exception as e:
            logger.error(f"创建保存点失败: {e}")
            raise TransactionError(f"创建保存点失败: {e}")
    
    def rollback_to_savepoint(self, name: str) -> None:
        """
        回滚到保存点
        
        Args:
            name: 保存点名称
        """
        try:
            if hasattr(self, 'execute'):
                self.execute(f"ROLLBACK TO SAVEPOINT {name}")
                logger.debug(f"回滚到保存点: {name}")
            else:
                logger.warning("数据库不支持保存点")
        except Exception as e:
            logger.error(f"回滚到保存点失败: {e}")
            raise TransactionError(f"回滚到保存点失败: {e}")
    
    def release_savepoint(self, name: str) -> None:
        """
        释放保存点
        
        Args:
            name: 保存点名称
        """
        try:
            if hasattr(self, 'execute'):
                self.execute(f"RELEASE SAVEPOINT {name}")
                logger.debug(f"释放保存点: {name}")
            else:
                logger.warning("数据库不支持保存点")
        except Exception as e:
            logger.error(f"释放保存点失败: {e}")
            raise TransactionError(f"释放保存点失败: {e}")
