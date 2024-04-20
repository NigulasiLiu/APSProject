# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2021/10/19 15:47
# @File           : production.py
# @IDE            : PyCharm
# @desc           : 数据库开发配置文件


"""
Redis 数据库配置

与接口是同一个数据库

格式："redis://:密码@地址:端口/数据库名称"
"""
REDIS_DB_ENABLE = True
REDIS_DB_URL = "redis://:123456@localhost:6379/1"

"""
MongoDB 数据库配置

与接口是同一个数据库

格式：mongodb://用户名:密码@地址:端口/?authSource=数据库名称
"""
MONGO_DB_ENABLE = True
MONGO_DB_NAME = "kinit"
MONGO_DB_URL = f"mongodb://kinit:123456@localhost:27017/?authSource={MONGO_DB_NAME}"

"""
MySQL 数据库配置

格式：mysql://用户名:密码@地址:端口/数据库名称
"""
MYSQL_DB_ENABLE = True
MYSQL_DB_NAME = "flaskdb"
MYSQL_DB_USER = "root"
MYSQL_DB_PASSWORD = "liubq"
MYSQL_DB_HOST = "localhost"
MYSQL_DB_PORT = 3306
MYSQL_DB_URL = f"mysql://{MYSQL_DB_USER}:{MYSQL_DB_PASSWORD}@{MYSQL_DB_HOST}:{MYSQL_DB_PORT}/{MYSQL_DB_NAME}"