"""
混入模块
提供可组合的功能扩展
"""

from .batch_ops import BatchOperationsMixin
from .transaction import TransactionMixin

__all__ = [
    'BatchOperationsMixin',
    'TransactionMixin',
]