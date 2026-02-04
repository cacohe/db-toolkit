"""
数据库工具包的自定义异常
"""


class DatabaseError(Exception):
    """数据库基础异常类"""
    pass


class ConnectionError(DatabaseError):
    """连接异常"""
    pass


class QueryError(DatabaseError):
    """查询执行异常"""
    pass


class ConfigurationError(DatabaseError):
    """配置异常"""
    pass


class ValidationError(DatabaseError):
    """数据验证异常"""
    pass


class NotSupportedError(DatabaseError):
    """不支持的操作异常"""
    pass


class TransactionError(DatabaseError):
    """事务异常"""
    pass
