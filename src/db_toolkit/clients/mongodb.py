"""
MongoDB数据库客户端
"""

from typing import Any, Dict, List, Optional
import logging

from ..core.base import BaseClient
from ..exceptions import ConnectionError, QueryError, NotSupportedError

logger = logging.getLogger(__name__)


class MongoDBClient(BaseClient):
    """MongoDB数据库客户端实现"""
    
    def __init__(self, config: Dict[str, Any]):
        """初始化MongoDB客户端"""
        super().__init__(config)
        self.client = None
        self.db = None
    
    def _validate_config(self) -> None:
        """验证MongoDB配置"""
        super()._validate_config()
        if 'database' not in self.config:
            raise ValueError("MongoDB配置缺少必需字段: database")
        
        # 需要connection_string或host
        if 'connection_string' not in self.config and 'host' not in self.config:
            raise ValueError("MongoDB配置需要提供connection_string或host")
    
    def connect(self) -> bool:
        """连接MongoDB数据库"""
        try:
            from pymongo import MongoClient
            
            connection_string = self.config.get('connection_string')
            
            if connection_string:
                self.client = MongoClient(connection_string)
            else:
                self.client = MongoClient(
                    host=self.config.get('host', 'localhost'),
                    port=self.config.get('port', 27017),
                    username=self.config.get('user'),
                    password=self.config.get('password'),
                    **self.config.get('options', {})
                )
            
            self.db = self.client[self.config['database']]
            self.connection = self.db  # 兼容性
            
            # 测试连接
            self.client.server_info()
            
            self._connected = True
            logger.info(f"MongoDB连接成功: {self.config['database']}")
            return True
            
        except ImportError:
            raise ConnectionError("未安装pymongo库，请运行: pip install pymongo")
        except Exception as e:
            self._connected = False
            logger.error(f"MongoDB连接失败: {e}")
            raise ConnectionError(f"MongoDB连接失败: {e}")
    
    def disconnect(self) -> bool:
        """断开MongoDB连接"""
        if self.client:
            try:
                self.client.close()
                self._connected = False
                logger.info("MongoDB连接已关闭")
                return True
            except Exception as e:
                logger.error(f"关闭MongoDB连接失败: {e}")
                return False
        return True
    
    def is_connected(self) -> bool:
        """检查MongoDB连接状态"""
        if not self.client:
            return False
        try:
            self.client.server_info()
            return True
        except Exception:
            self._connected = False
            return False
    
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        MongoDB不支持SQL查询
        
        Raises:
            NotSupportedError: 总是抛出此异常
        """
        raise NotSupportedError("MongoDB不支持SQL查询，请使用insert/update/delete/select方法")
    
    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """插入文档到MongoDB"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            collection = self.db[table]
            result = collection.insert_one(data)
            doc_id = str(result.inserted_id)
            logger.debug(f"插入文档成功，ID: {doc_id}")
            return doc_id
            
        except Exception as e:
            logger.error(f"插入文档失败: {e}")
            raise QueryError(f"插入文档失败: {e}")
    
    def insert_many(self, table: str, data_list: List[Dict[str, Any]]) -> List[str]:
        """
        批量插入文档
        
        Args:
            table: 集合名
            data_list: 文档列表
            
        Returns:
            List[str]: 插入的文档ID列表
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            collection = self.db[table]
            result = collection.insert_many(data_list)
            doc_ids = [str(id) for id in result.inserted_ids]
            logger.debug(f"批量插入 {len(doc_ids)} 个文档")
            return doc_ids
            
        except Exception as e:
            logger.error(f"批量插入文档失败: {e}")
            raise QueryError(f"批量插入文档失败: {e}")
    
    def update(self, table: str, data: Dict[str, Any], 
               condition: Dict[str, Any]) -> int:
        """更新MongoDB文档"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            collection = self.db[table]
            result = collection.update_many(condition, {'$set': data})
            count = result.modified_count
            logger.debug(f"更新了 {count} 个文档")
            return count
            
        except Exception as e:
            logger.error(f"更新文档失败: {e}")
            raise QueryError(f"更新文档失败: {e}")
    
    def delete(self, table: str, condition: Dict[str, Any]) -> int:
        """删除MongoDB文档"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            collection = self.db[table]
            result = collection.delete_many(condition)
            count = result.deleted_count
            logger.debug(f"删除了 {count} 个文档")
            return count
            
        except Exception as e:
            logger.error(f"删除文档失败: {e}")
            raise QueryError(f"删除文档失败: {e}")
    
    def select(self, table: str,
               fields: Optional[List[str]] = None,
               condition: Optional[Dict[str, Any]] = None,
               limit: Optional[int] = None,
               offset: Optional[int] = None,
               order_by: Optional[List[tuple]] = None) -> List[Dict]:
        """查询MongoDB文档"""
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            collection = self.db[table]
            
            # 构建投影
            projection = {field: 1 for field in fields} if fields else None
            
            # 执行查询
            cursor = collection.find(condition or {}, projection)
            
            # 排序
            if order_by:
                # MongoDB排序: 1为升序，-1为降序
                sort_list = []
                for field, direction in order_by:
                    sort_direction = 1 if direction.upper() == 'ASC' else -1
                    sort_list.append((field, sort_direction))
                cursor = cursor.sort(sort_list)
            
            # 跳过和限制
            if offset:
                cursor = cursor.skip(offset)
            if limit:
                cursor = cursor.limit(limit)
            
            # 转换结果
            results = []
            for doc in cursor:
                # 将ObjectId转为字符串
                doc['_id'] = str(doc['_id'])
                results.append(doc)
            
            logger.debug(f"查询到 {len(results)} 个文档")
            return results
            
        except Exception as e:
            logger.error(f"查询文档失败: {e}")
            raise QueryError(f"查询文档失败: {e}")
    
    def aggregate(self, table: str, pipeline: List[Dict]) -> List[Dict]:
        """
        执行聚合查询
        
        Args:
            table: 集合名
            pipeline: 聚合管道
            
        Returns:
            List[Dict]: 聚合结果
        """
        if not self.is_connected():
            raise ConnectionError("数据库未连接")
        
        try:
            collection = self.db[table]
            results = list(collection.aggregate(pipeline))
            
            # 转换ObjectId
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            logger.debug(f"聚合查询返回 {len(results)} 个结果")
            return results
            
        except Exception as e:
            logger.error(f"聚合查询失败: {e}")
            raise QueryError(f"聚合查询失败: {e}")
