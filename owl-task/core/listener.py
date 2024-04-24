#!/usr/bin/python
# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2023/6/21 14:42 
# @File           : listener.py
# @IDE            : PyCharm
# @desc           : 简要说明

import datetime
import json
from apscheduler.events import JobExecutionEvent, EVENT_JOB_REMOVED, EVENT_JOB_ADDED, EVENT_JOB_ERROR, \
    JobSubmissionEvent
from .mysql import get_database
import pytz
from .logger import logger
from application.settings import SCHEDULER_TASK_RECORD, SCHEDULER_TASK, SCHEDULER_TASK_JOBS

def safe_json_dumps(value):
    try:
        return json.dumps(value, default=str)  # 使用 default=str 来处理无法直接序列化的对象
    except TypeError as e:
        logger.error(f"JSON序列化错误: {e}")
        return str(value)  # 如果仍然失败，则将值转换为字符串

def on_job_added(event: EVENT_JOB_ADDED ):
    job_id = event.job_id
    if "-temp-" in job_id:
        job_id = job_id.split("-")[0]

    result = {
        "job_id": job_id,
    }
    print(f"adding job: {event.job_id}")
    db = get_database()
    try:
        task_exist = db.get_data(SCHEDULER_TASK, job_id=job_id)
        if task_exist:
            print(f"Task {job_id} 已经存在于SCHEDULER_TASK.")
            result.update({
                "status": "updated"
            })
            db.put_data(SCHEDULER_TASK, result)
        else:
            # 如果 SCHEDULER_TASK 中没有找到任务，则插入新条目
            result.update({
                "status": "created"
            })
            print("从SCHEDULER_TASK_JOBS将"+job_id+"部分同步到SCHEDULER_TASK")
            db.create_data(SCHEDULER_TASK, result)
    except Exception as e:
        logger.error(f"Error removing task {event.job_id}: {e}")


def before_job_execution(event: JobExecutionEvent):
    # print("在执行定时任务前执行的代码...")
    shanghai_tz = pytz.timezone("Asia/Shanghai")
    start_timestamp: datetime.datetime = event.scheduled_run_time.astimezone(shanghai_tz)
    end_timestamp = datetime.datetime.now(shanghai_tz)
    process_time = (end_timestamp - start_timestamp).total_seconds()
    job_id = event.job_id
    if "-temp-" in job_id:
        job_id = job_id.split("-")[0]
    print("任务标识符：", job_id)
    # print("本次开始执行时间：", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    # print("本次执行完成时间：", end_time.strftime("%Y-%m-%d %H:%M:%S"))
    # print("任务启动耗时（秒）：", process_time)
    # print("任务返回值：", event.retval)
    # print("异常信息：", event.exception)
    # print("堆栈跟踪：", event.traceback)
    result = {
        "job_id": job_id,
        "start_timestamp": start_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "process_time": process_time,
        "retval": safe_json_dumps(event.retval),
        "exception": safe_json_dumps(event.exception),
        #"traceback": safe_json_dumps(event.traceback)
    }
    db = get_database()
    try:
        task = db.get_data(SCHEDULER_TASK, job_id=job_id)

        if task:
            result.update({
                "update_timestamp": datetime.datetime.now(shanghai_tz),
                "status": "running",
                "exception": "-"
            })
            print("监听器尝试更新SCHEDULER_TASK:"+job_id)
            db.put_data(SCHEDULER_TASK, {'job_id': job_id}, result)

        else:
            print("SCHEDULER_TASK:"+job_id+"条目不存在，说明已经被删除")
            logger.info(f"任务 {job_id} 不在 SCHEDULER_TASK 表中，说明已经被删除")
            # print("SCHEDULER_TASK:"+job_id+"(SCHEDULER_TASK),插入该record")
            # db.create_data(SCHEDULER_TASK, result)
            # db.create_data(SCHEDULER_TASK, result)
    except Exception as e:
        logger.error(f"处理任务编号 {job_id} 时发生错误: {e}")
        result["exception"] = str(e)
        result["status"] = "error"
        task_exist = db.get_data(SCHEDULER_TASK, job_id=job_id)
        if task_exist:
            db.put_data(SCHEDULER_TASK, {'job_id': job_id}, result)
        else:
            print("SCHEDULER_TASK:"+job_id)
            db.create_data(SCHEDULER_TASK, result)

def on_job_removed(event: EVENT_JOB_REMOVED):
    print(f"Removing job: {event.job_id}")
    job_id = event.job_id
    db = get_database()
    try:
        # 首先尝试从SCHEDULER_TASK记录中移除任务
        task_exist = db.get_data(SCHEDULER_TASK, job_id=job_id)
        if task_exist:
            db.delete_data(SCHEDULER_TASK, job_id=job_id)
            print(f"Task {job_id} removed from SCHEDULER_TASK.")

        # # 接着尝试从SCHEDULER_TASK_RECORD中移除任务记录
        # record_exist = db.get_data(SCHEDULER_TASK_RECORD, job_id=job_id)
        # if record_exist:
        #     db.delete_data(SCHEDULER_TASK_RECORD, job_id=event.job_id)
        #     print(f"Task record {event.job_id} removed from SCHEDULER_TASK_RECORD.")
    except Exception as e:
        logger.error(f"Error removing task {event.job_id}: {e}")

# def on_job_added(event):
#     # print("在执行定时任务前执行的代码...")
#     shanghai_tz = pytz.timezone("Asia/Shanghai")
#     start_time: datetime.datetime = event.scheduled_run_time.astimezone(shanghai_tz)
#     end_time = datetime.datetime.now(shanghai_tz)
#     process_time = (end_time - start_time).total_seconds()
#     job_id = event.job_id
#     if "-temp-" in job_id:
#         job_id = job_id.split("-")[0]
#     print("任务标识符：", job_id)
#     # print("本次开始执行时间：", start_time.strftime("%Y-%m-%d %H:%M:%S"))
#     # print("本次执行完成时间：", end_time.strftime("%Y-%m-%d %H:%M:%S"))
#     # print("任务启动耗时（秒）：", process_time)
#     # print("任务返回值：", event.retval)
#     # print("异常信息：", event.exception)
#     # print("堆栈跟踪：", event.traceback)
#
#     result = {
#         "job_id": job_id,
#         "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
#         "process_time": process_time,
#         "retval": safe_json_dumps(event.retval),
#         "exception": safe_json_dumps(event.exception),
#         "traceback": safe_json_dumps(event.traceback)
#     }
#
#     db = get_database()
#     try:
#         task = db.get_data(SCHEDULER_TASK_JOBS, id=job_id)
#         if task:
#             result.update({
#                 "update_time": datetime.datetime.now(shanghai_tz),
#                 "status": "updated",
#                 "exception": "-"
#             })
#             print("监听器尝试更新SCHEDULER_TASK_RECORD:"+job_id)
#             record_exist = db.get_data(SCHEDULER_TASK_RECORD, job_id=job_id)
#             if record_exist:
#                 db.put_data(SCHEDULER_TASK_RECORD, {'job_id': job_id}, result)
#             else:
#                 print("SCHEDULER_TASK_RECORD中并没有:"+job_id+"(可能由于初次添加任务时并未同步到SCHEDULER_TASK_RECORD),插入该record")
#                 db.create_data(SCHEDULER_TASK_RECORD, result)
#         else:
#             print("SCHEDULER_TASK_JOBS:"+job_id+"条目不存在，说明已经被删除")
#             logger.info(f"任务 {job_id} 不在 SCHEDULER_TASK 表中，将创建新记录")
#             # db.create_data(SCHEDULER_TASK_RECORD, result)
#     except Exception as e:
#         logger.error(f"处理任务编号 {job_id} 时发生错误: {e}")
#         result["exception"] = str(e)
#         result["status"] = "error"
#         task_exist = db.get_data(SCHEDULER_TASK_JOBS, id=job_id)
#         if task_exist:
#             db.put_data(SCHEDULER_TASK_RECORD, {'job_id': job_id}, result)
#         else:
#             print("监听器插入error数据SCHEDULER_TASK_RECORD:"+job_id)
#             db.create_data(SCHEDULER_TASK_RECORD, result)


# def before_job_execution(event: JobExecutionEvent):
#     # print("在执行定时任务前执行的代码...")
#     shanghai_tz = pytz.timezone("Asia/Shanghai")
#     start_time: datetime.datetime = event.scheduled_run_time.astimezone(shanghai_tz)
#     end_time = datetime.datetime.now(shanghai_tz)
#     process_time = (end_time - start_time).total_seconds()
#     job_id = event.job_id
#     if "-temp-" in job_id:
#         job_id = job_id.split("-")[0]
#     print("任务标识符：", job_id)
#     # print("本次开始执行时间：", start_time.strftime("%Y-%m-%d %H:%M:%S"))
#     # print("本次执行完成时间：", end_time.strftime("%Y-%m-%d %H:%M:%S"))
#     # print("任务启动耗时（秒）：", process_time)
#     # print("任务返回值：", event.retval)
#     # print("异常信息：", event.exception)
#     # print("堆栈跟踪：", event.traceback)
#
#     result = {
#         "job_id": job_id,
#         "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
#         "process_time": process_time,
#         "retval": safe_json_dumps(event.retval),
#         "exception": safe_json_dumps(event.exception),
#         "traceback": safe_json_dumps(event.traceback)
#     }
#
#     db = get_database()
#     try:
#         task = db.get_data(SCHEDULER_TASK, id=job_id)
#         if task:
#             result.update({
#                 "update_time": datetime.datetime.now(shanghai_tz),
#                 "status": "updated",
#                 "exception": "-"
#             })
#             print("监听器尝试更新SCHEDULER_TASK_RECORD:"+job_id)
#             record_exist = db.get_data(SCHEDULER_TASK, job_id=job_id)
#             if record_exist:
#                 db.put_data(SCHEDULER_TASK_RECORD, {'job_id': job_id}, result)
#             else:
#                 print("SCHEDULER_TASK_RECORD中并没有:"+job_id+"(可能由于初次添加任务时并未同步到SCHEDULER_TASK_RECORD),插入该record")
#                 db.create_data(SCHEDULER_TASK_RECORD, result)
#         else:
#             print("SCHEDULER_TASK_JOBS:"+job_id+"条目不存在，说明已经被删除")
#             logger.info(f"任务 {job_id} 不在 SCHEDULER_TASK 表中，将创建新记录")
#             # db.create_data(SCHEDULER_TASK_RECORD, result)
#     except Exception as e:
#         logger.error(f"处理任务编号 {job_id} 时发生错误: {e}")
#         result["exception"] = str(e)
#         result["status"] = "error"
#         task_exist = db.get_data(SCHEDULER_TASK_JOBS, id=job_id)
#         if task_exist:
#             db.put_data(SCHEDULER_TASK_RECORD, {'job_id': job_id}, result)
#         else:
#             print("监听器插入error数据SCHEDULER_TASK_RECORD:"+job_id)
#             db.create_data(SCHEDULER_TASK_RECORD, result)
