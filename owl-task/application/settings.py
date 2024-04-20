#!/usr/bin/python
# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2023/6/21 13:39 
# @File           : settings.py
# @IDE            : PyCharm
# @desc           : 简要说明

import os

"""项目根目录"""
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


DEBUG = False


"""
引入数据库配置
"""
if DEBUG:
    from .config.production import *
else:
    from .config.production import *


"""
发布/订阅通道

与接口相互关联，请勿随意更改
"""
SUBSCRIBE = 'kinit_queue'

# Kafka 设置
KAFKA_BROKER_URL = 'localhost:9092'  # Kafka 服务器地址
KAFKA_TOPIC = 'client_tasks'  # Kafka 主题

"""
MongoDB 集合

与接口相互关联，相互查询，请勿随意更改
"""
# 用于存放任务调用日志
SCHEDULER_TASK_RECORD = "scheduler_task_record"
# 用于存放运行中的任务
SCHEDULER_TASK_JOBS = "scheduler_task_jobs"
# 用于存放任务信息
SCHEDULER_TASK = "scheduler_task"


"""
定时任务脚本目录
"""
TASKS_ROOT = "tasks"
