"""
核心模块
包含基类和抽象接口
"""

from .base import BaseClient
from .sql_base import SQLBaseClient

__all__ = ['BaseClient', 'SQLBaseClient']