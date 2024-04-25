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


# Kafka 设置
KAFKA_BROKER_URL = 'localhost:9092'  # Kafka 服务器地址
KAFKA_TOPIC = 'client_tasks'  # Kafka 主题

"""
MongoDB 集合

与接口相互关联，相互查询，请勿随意更改
"""
# 用于存放运行中的任务
SCHEDULER_TASK_JOBS = "scheduler_task_jobs"
# 用于存放任务信息
SCHEDULER_TASK = "scheduler_task"

# 用于存放任务调用日志
SCHEDULER_TASK_RECORD = "scheduler_task_record"

"""
定时任务脚本目录,如果要调用task/test/main，
那么前端调用目标格式为：test.main.Test("kinit",1314, True),括号内为类初始化参数
参数/关键字参数，为调用函数需要的参数
"""
TASKS_ROOT = "tasks"
