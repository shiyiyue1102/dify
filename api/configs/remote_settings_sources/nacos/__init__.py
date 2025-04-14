import asyncio
import os
from collections.abc import Mapping
from typing import Any

from pydantic.fields import FieldInfo
from v2.nacos import ConfigParam, NacosConfigService
from v2.nacos.common.client_config import GRPCConfig
from v2.nacos.common.client_config_builder import ClientConfigBuilder

from configs.remote_settings_sources.base import RemoteSettingsSource

from .utils import _parse_config

client_config = (ClientConfigBuilder()
                 .access_key(os.getenv('DIFY_ENV_NACOS_ACCESS_KEY'))
                 .secret_key(os.getenv('DIFY_ENV_NACOS_SECRET_KEY'))
                 .server_address(os.getenv('DIFY_ENV_NACOS_NACOS_SERVER_ADDR', 'localhost:8848'))
                 .log_level('INFO')
                 .grpc_config(GRPCConfig(grpc_timeout=5000))
                 .build())

class NacosSettingsSource(RemoteSettingsSource):
    def __init__(self, configs: Mapping[str, Any]):
        self.configs = configs
        self.client = None
        self.remote_configs=None
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_init())

    async def async_init(self):

        self.client =await NacosConfigService.create_config_service(client_config)
        try:
            assert await self.client.server_health()
        except AssertionError:
            raise RuntimeError("Nacos server is not healthy")
        data_id = "com.alibaba.nacos.test.config"
        group = "DEFAULT_GROUP"
        try:
            content = await self.client.get_config(ConfigParam(
                data_id=data_id,
                group=group,
            ))
            self.remote_configs = self._parse_config(content)
        except Exception as e:
            raise RuntimeError(f"Failed to get config from Nacos: {e}")

    def _parse_config(self, content: str) -> dict:
        if not content:
            return {}
        try:
            return _parse_config(self, content)
        except Exception as e:
            raise RuntimeError(f"Failed to parse config: {e}")
    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:

        if not isinstance(self.remote_configs, dict):
            raise ValueError(f"remote configs is not dict, but {type(self.remote_configs)}")

        field_value = self.remote_configs.get(field_name)
        if field_value is None:
            return None, field_name, False

        return field_value, field_name, False