"""
配置管理器
用于管理数据库配置文件
"""

import json
import os
from typing import Dict, Any, Optional
import logging

from ..core.base import BaseClient
from .factory import ClientFactory
from ..exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    数据库配置管理器
    
    管理JSON格式的数据库配置文件，支持多个数据库配置
    """
    
    def __init__(self, config_file: str = 'db_config.json'):
        """
        初始化配置管理器
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.configs: Dict[str, Any] = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            Dict: 配置字典
        """
        if not os.path.exists(self.config_file):
            logger.warning(f"配置文件不存在: {self.config_file}，使用空配置")
            return {'databases': {}, 'default': None}
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                logger.info(f"加载配置文件成功: {self.config_file}")
                return config
        except json.JSONDecodeError as e:
            logger.error(f"配置文件JSON格式错误: {e}")
            raise ConfigurationError(f"配置文件格式错误: {e}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise ConfigurationError(f"加载配置文件失败: {e}")
    
    def _save_config(self) -> None:
        """保存配置文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.configs, f, indent=2, ensure_ascii=False)
            logger.info(f"保存配置文件成功: {self.config_file}")
        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")
            raise ConfigurationError(f"保存配置文件失败: {e}")
    
    def get_client(self, name: Optional[str] = None, auto_connect: bool = True) -> BaseClient:
        """
        获取数据库客户端
        
        Args:
            name: 数据库配置名称，None则使用默认配置
            auto_connect: 是否自动连接
            
        Returns:
            BaseClient: 数据库客户端实例
            
        Raises:
            ConfigurationError: 配置不存在或无效时
            
        Examples:
            >>> manager = ConfigManager()
            >>> with manager.get_client('mysql_prod') as client:
            ...     users = client.select('users')
        """
        if name is None:
            name = self.configs.get('default')
            if not name:
                raise ConfigurationError("未指定数据库名称且没有默认配置")
        
        db_config = self.configs.get('databases', {}).get(name)
        if not db_config:
            available = ', '.join(self.configs.get('databases', {}).keys())
            raise ConfigurationError(
                f"数据库配置不存在: '{name}'\n"
                f"可用配置: {available or '(无)'}"
            )
        
        db_type = db_config.get('type')
        config = db_config.get('config', {})
        
        if not db_type:
            raise ConfigurationError(f"数据库配置'{name}'缺少type字段")
        
        client = ClientFactory.create(db_type, config)
        
        if auto_connect:
            client.connect()
        
        return client
    
    def add(self, name: str, db_type: str, config: Dict[str, Any], 
            set_as_default: bool = False) -> None:
        """
        添加数据库配置
        
        Args:
            name: 配置名称
            db_type: 数据库类型
            config: 数据库配置
            set_as_default: 是否设置为默认配置
            
        Examples:
            >>> manager = ConfigManager()
            >>> manager.add('prod_db', 'mysql', {
            ...     'host': 'localhost',
            ...     'user': 'root',
            ...     'password': 'pass',
            ...     'database': 'prod'
            ... })
        """
        if 'databases' not in self.configs:
            self.configs['databases'] = {}
        
        self.configs['databases'][name] = {
            'type': db_type,
            'config': config
        }
        
        if set_as_default or not self.configs.get('default'):
            self.configs['default'] = name
        
        self._save_config()
        logger.info(f"添加数据库配置: {name} ({db_type})")
    
    def remove(self, name: str) -> bool:
        """
        删除数据库配置
        
        Args:
            name: 配置名称
            
        Returns:
            bool: 是否删除成功
        """
        if name not in self.configs.get('databases', {}):
            logger.warning(f"配置不存在: {name}")
            return False
        
        del self.configs['databases'][name]
        
        # 如果删除的是默认配置，清除默认设置
        if self.configs.get('default') == name:
            self.configs['default'] = None
        
        self._save_config()
        logger.info(f"删除数据库配置: {name}")
        return True
    
    def set_default(self, name: str) -> None:
        """
        设置默认数据库
        
        Args:
            name: 配置名称
            
        Raises:
            ConfigurationError: 配置不存在时
        """
        if name not in self.configs.get('databases', {}):
            raise ConfigurationError(f"配置不存在: {name}")
        
        self.configs['default'] = name
        self._save_config()
        logger.info(f"设置默认数据库: {name}")
    
    def list(self) -> Dict[str, str]:
        """
        列出所有配置
        
        Returns:
            Dict[str, str]: 配置名称到数据库类型的映射
        """
        databases = self.configs.get('databases', {})
        return {name: conf['type'] for name, conf in databases.items()}
    
    def get_default(self) -> Optional[str]:
        """
        获取默认配置名称
        
        Returns:
            Optional[str]: 默认配置名称
        """
        return self.configs.get('default')
    
    def export_config(self, output_file: str) -> None:
        """
        导出配置到文件
        
        Args:
            output_file: 输出文件路径
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.configs, f, indent=2, ensure_ascii=False)
            logger.info(f"导出配置到: {output_file}")
        except Exception as e:
            logger.error(f"导出配置失败: {e}")
            raise ConfigurationError(f"导出配置失败: {e}")
    
    def import_config(self, input_file: str, merge: bool = True) -> None:
        """
        从文件导入配置
        
        Args:
            input_file: 输入文件路径
            merge: 是否合并到现有配置（False则覆盖）
        """
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                imported = json.load(f)
            
            if merge:
                # 合并数据库配置
                if 'databases' in imported:
                    if 'databases' not in self.configs:
                        self.configs['databases'] = {}
                    self.configs['databases'].update(imported['databases'])
                
                # 如果没有默认配置，使用导入的默认配置
                if 'default' in imported and not self.configs.get('default'):
                    self.configs['default'] = imported['default']
            else:
                self.configs = imported
            
            self._save_config()
            logger.info(f"导入配置从: {input_file}")
            
        except Exception as e:
            logger.error(f"导入配置失败: {e}")
            raise ConfigurationError(f"导入配置失败: {e}")
