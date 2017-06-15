# -*- coding: utf-8 -*-

from framework.redisutils.redis_storage import RedisStorage



def get_client(client_idx=None):
    """
    向后兼容的获取redis连接方法
    """
    return RedisStorage.get_conn('profile')


__all__ = ['get_client']
