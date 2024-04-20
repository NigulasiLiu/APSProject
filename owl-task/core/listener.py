#!/usr/bin/python
# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2023/6/21 14:42 
# @File           : listener.py
# @IDE            : PyCharm
# @desc           : 简要说明

import datetime
import json
from apscheduler.events import JobExecutionEvent
from .mysql import get_database
import pytz
from application.settings import SCHEDULER_TASK_RECORD, SCHEDULER_TASK, SCHEDULER_TASK_JOBS
from .logger import logger


def before_job_execution(event: JobExecutionEvent):
    # print("在执行定时任务前执行的代码...")
    shanghai_tz = pytz.timezone("Asia/Shanghai")
    start_time: datetime.datetime = event.scheduled_run_time.astimezone(shanghai_tz)
    end_time = datetime.datetime.now(shanghai_tz)
    process_time = (end_time - start_time).total_seconds()
    job_id = event.job_id
    if "-temp-" in job_id:
        job_id = job_id.split("-")[0]
    print("任务标识符：", event.job_id)
    print("任务开始执行时间：", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("任务执行完成时间：", end_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("任务耗时（秒）：", process_time)
    print("任务返回值：", event.retval)
    print("异常信息：", event.exception)
    print("堆栈跟踪：", event.traceback)

    result = {
        "job_id": job_id,
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "process_time": process_time,
        "retval": json.dumps(event.retval) if event.retval is not None else None,
        "exception": json.dumps(event.exception) if event.exception is not None else None,
        "traceback": json.dumps(event.traceback) if event.traceback is not None else None
    }

    db = get_database()
    try:
        task = db.get_data(SCHEDULER_TASK_RECORD, job_id=job_id)
        if task:
            result.update({
                # "task_name": task.get("task_name"),
                # "task_group": task.get("task_group", "default"),
                # "exec_strategy": task.get("exec_strategy"),
                # "expression": task.get("expression"),
                "update_time": datetime.datetime.now(shanghai_tz)
            })
            db.put_data(SCHEDULER_TASK_RECORD, {'job_id': job_id}, result)
        else:
            # 如果 SCHEDULER_TASK 中没有找到任务，则插入新条目
            result.update({
                "create_time": datetime.datetime.now(shanghai_tz)
            })
            logger.info(f"任务 {job_id} 不在 SCHEDULER_TASK 表中，将创建新记录")
            db.create_data(SCHEDULER_TASK_RECORD, result)
    except Exception as e:
        logger.error(f"处理任务编号 {job_id} 时发生错误: {e}")
        result["exception"] = str(e)
        db.create_data(SCHEDULER_TASK_RECORD, result)


