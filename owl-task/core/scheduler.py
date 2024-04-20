#!/usr/bin/python
# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2023/6/21 10:10 
# @File           : scheduler.py
# @IDE            : PyCharm
# @desc           : 简要说明

import datetime
import importlib
from typing import List
import re
from apscheduler.jobstores.base import JobLookupError
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.job import Job
from sqlalchemy import create_engine

from .listener import before_job_execution
from apscheduler.events import EVENT_JOB_EXECUTED
from application.settings import MONGO_DB_NAME, MONGO_DB_URL, REDIS_DB_URL, SUBSCRIBE, SCHEDULER_TASK, \
    SCHEDULER_TASK_RECORD, \
    MYSQL_DB_NAME, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_HOST, MYSQL_DB_PORT, MYSQL_DB_URL, \
    TASKS_ROOT, SCHEDULER_TASK_JOBS
#from .mongo import get_database

from .mysql import get_database


def get_sqlalchemy_engine():
    # 根据实际使用的 MySQL 驱动来调整 URL 格式
    url = f'mysql+pymysql://{MYSQL_DB_USER}:{MYSQL_DB_PASSWORD}@{MYSQL_DB_HOST}:{MYSQL_DB_PORT}/{MYSQL_DB_NAME}'
    engine = create_engine(url)
    return engine


class Scheduler:
    TASK_DIR = TASKS_ROOT
    COLLECTION = SCHEDULER_TASK_JOBS

    def __init__(self):
        self.scheduler = None
        self.db = None


    def __get_mysql_job_store(self) -> SQLAlchemyJobStore:
        #self.engine = get_sqlalchemy_engine()
        """
        获取 MySQL Job Store 使用 SQLAlchemy
        :return: SQLAlchemy Job Store
        """
        self.db = get_database()
        engine = self.db.get_engine(MYSQL_DB_HOST, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_NAME, MYSQL_DB_PORT)
        return SQLAlchemyJobStore(engine=engine, tablename='scheduler_task_jobs')


    def start(self, listener: bool = True) -> None:
        """
        创建调度器
        :param listener: 是否注册事件监听器
        :return:
        """
        self.scheduler = BackgroundScheduler()
        if listener:
            # 注册事件监听器
            self.scheduler.add_listener(before_job_execution, EVENT_JOB_EXECUTED)
        self.scheduler.add_jobstore(self.__get_mysql_job_store())
        self.scheduler.start()

    def __get_mongodb_job_store(self) -> MongoDBJobStore:
        """
        获取 MongoDB Job Store
        :return: MongoDB Job Store
        """
        self.db = get_database()
        return MongoDBJobStore(database=MONGO_DB_NAME, collection=self.COLLECTION, client=self.db.client)

    def add_data_interval_cron_job(
            self,
            job_class: str,
            trigger: CronTrigger | DateTrigger | IntervalTrigger,
            name: str = None,
            *args,
            **kwargs
    ) -> None | Job:
        """
        date触发器用于在指定的日期和时间触发一次任务。它适用于需要在特定时间点执行一次的任务，例如执行一次备份操作。
        :param job_class: 类路径
        :param trigger: 触发条件
        :param name: 任务名称
        :return:
        """
        class_instance = self.__import_module(job_class)
        if class_instance:
            return self.scheduler.add_job(class_instance.main, trigger=trigger, id=name,
                                          args=args, kwargs=kwargs, replace_existing=True)
        else:
            raise ValueError(f"添加任务失败，未找到该模块下的方法：{class_instance}")

    def add_cron_job(
            self,
            job_class: str,
            expression: str,
            start_date: str = None,
            end_date: str = None,
            timezone: str = "Asia/Shanghai",
            name: str = None,
            args: tuple = (),
            **kwargs
    ) -> None | Job:
        """
        通过 cron 表达式添加定时任务
        :param job_class: 类路径
        :param expression: cron 表达式，六位或七位，分别表示秒、分钟、小时、天、月、星期几、年
        :param start_date: 触发器的开始日期时间。可选参数，默认为 None。
        :param end_date: 触发器的结束日期时间。可选参数，默认为 None。
        :param timezone: 时区，表示触发器应用的时区。可选参数，默认为 None，使用上海默认时区。
        :param name: 任务名称
        :param args: 非关键字参数
        :return:
        """
        second, minute, hour, day, month, day_of_week, year = self.__parse_cron_expression(expression)
        trigger = CronTrigger(
            second=second,
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            year=year,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone
        )
        return self.add_data_interval_cron_job(job_class, trigger, name, *args, **kwargs)

    def add_date_job(self, job_class: str, expression: str, name: str = None, args: tuple = (), **kwargs) -> None | Job:
        """
        date触发器用于在指定的日期和时间触发一次任务。它适用于需要在特定时间点执行一次的任务，例如执行一次备份操作。
        :param job_class: 类路径
        :param expression: date
        :param name: 任务名称
        :param args: 非关键字参数
        :return:
        """
        trigger = DateTrigger(run_date=expression)
        return self.add_data_interval_cron_job(job_class, trigger, name, *args, **kwargs)

    def add_interval_job(
            self,
            job_class: str,
            expression: str,
            start_date: str | datetime.datetime = None,
            end_date: str | datetime.datetime = None,
            timezone: str = "Asia/Shanghai",
            jitter: int = None,
            name: str = None,
            args: tuple = (),
            **kwargs
    ) -> None | Job:
        """
        date触发器用于在指定的日期和时间触发一次任务。它适用于需要在特定时间点执行一次的任务，例如执行一次备份操作。
        :param job_class: 类路径
        :param expression：interval 表达式，分别为：秒、分、时、天、周，例如，设置 10 * * * * 表示每隔 10 秒执行一次任务。
        :param end_date: 表示任务的结束时间，可以设置为 datetime 对象或者字符串。
                         例如，设置 end_date='2023-06-23 10:00:00' 表示任务在 2023 年 6 月 23 日 10 点结束。
        :param start_date: 表示任务的起始时间，可以设置为 datetime 对象或者字符串。
                           例如，设置 start_date='2023-06-22 10:00:00' 表示从 2023 年 6 月 22 日 10 点开始执行任务。
        :param timezone：表示时区，可以设置为字符串或 pytz.timezone 对象。例如，设置 timezone='Asia/Shanghai' 表示使用上海时区。
        :param jitter：表示时间抖动，可以设置为整数或浮点数。例如，设置 jitter=2 表示任务的执行时间会在原定时间上随机增加 0~2 秒的时间抖动。
        :param name: 任务名称
        :param args: 非关键字参数
        :return:
        """
        second, minute, hour, day, week = self.__parse_interval_expression(expression)
        print("second:{}, minute:{}, hour:{}, day:{}, week:{}".format(second, minute, hour, day, week))

        trigger = IntervalTrigger(
            weeks=week,
            days=day,
            hours=hour,
            minutes=minute,
            seconds=second,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            jitter=jitter
        )
        return self.add_data_interval_cron_job(job_class, trigger, name, *args, **kwargs)

    def run_job(self, job_class: str, args: tuple = (), **kwargs) -> None:
        """
        立即执行一次任务，但不会执行监听器，只适合只需要执行任务，不需要记录的任务
        :param job_class: 类路径
        :param args: 类路径
        :return: 类实例
        """
        job_class = self.__import_module(job_class)[0]
        job_class.main(*args, **kwargs)

    def remove_job(self, name: str) -> None:
        """
        删除任务
        :param name: 任务名称
        :return:
        """
        try:
            self.scheduler.remove_job(name)
        except JobLookupError as e:
            raise ValueError(f"删除任务失败, 报错：{e}")

    def get_job(self, name: str) -> Job:
        """
        获取任务
        :param name: 任务名称
        :return:
        """
        return self.scheduler.get_job(name)

    def has_job(self, name: str) -> bool:
        """
        判断任务是否存在
        :param name: 任务名称
        :return:
        """
        if self.get_job(name):
            return True
        else:
            return False

    def get_jobs(self) -> List[Job]:
        """
        获取所有任务
        :return:
        """
        return self.scheduler.get_jobs()

    def get_job_names(self) -> List[str]:
        """
        获取所有任务
        :return:
        """
        jobs = self.scheduler.get_jobs()
        return [job.id for job in jobs]


    @staticmethod
    def __parse_cron_expression(expression: str) -> tuple:
        """
        解析 cron 表达式
        :param expression: cron 表达式，支持六位或七位，分别表示秒、分钟、小时、天、月、星期几、年
        :return: 解析后的秒、分钟、小时、天、月、星期几、年字段的元组
        """
        fields = expression.strip().split()

        if len(fields) not in (6, 7):
            raise ValueError("无效的 Cron 表达式")

        parsed_fields = [None if field in ('*', '?') else field for field in fields]
        if len(fields) == 6:
            parsed_fields.append(None)

        return tuple(parsed_fields)

    @staticmethod
    def __parse_interval_expression(expression: str) -> tuple:
        """
        解析 interval 表达式
        :param expression: interval 表达式，分别为：秒、分、时、天、周，例如，设置 10 * * * * 表示每隔 10 秒执行一次任务。
        :return:
        """
        # 将传入的 interval 表达式拆分为不同的字段
        fields = expression.strip().split()

        if len(fields) != 5:
            raise ValueError("无效的 interval 表达式")

        parsed_fields = [int(field) if field != '*' else 0 for field in fields]
        return tuple(parsed_fields)


    def __import_module(self, expression: str):
        """
        反射模块
        :param expression: 类路径
        :return: 类实例
        """
        module, args, kwargs = self.__parse_string_to_class(expression)
        module_pag = self.TASK_DIR + '.' + module[0:module.rindex(".")]
        module_class = module[module.rindex(".") + 1:]
        try:
            # 动态导入模块
            pag = importlib.import_module(module_pag)
            class_ref = getattr(pag, module_class)
            return class_ref(*args, **kwargs)  # 创建并返回类的实例
        except ModuleNotFoundError:
            raise ValueError(f"未找到该模块：{module_pag}")
        except AttributeError:
            raise ValueError(f"未找到该模块下的方法：{module_class}")
        except TypeError as e:
            raise ValueError(f"参数传递错误：{args}, 详情：{e}")

    @classmethod
    def __parse_string_to_class(cls, expression: str):
        """
        使用正则表达式匹配类路径、位置参数和关键字参数
        :param expression: 表达式
        :return: tuple (class_path, args, kwargs)
        """
        pattern = r'([\w.]+)\((.*)\)$'
        match = re.match(pattern, expression)
        args, kwargs = [], {}

        if match:
            class_path = match.group(1)
            arguments = match.group(2)

            print(f"解析类路径: {class_path}")  # 打印解析后的类路径
            print(f"初始化参数字符串: {arguments}")  # 打印原始参数字符串
            # Split the arguments on commas not inside brackets
            arguments = re.split(r',\s*(?![^()]*\))', arguments)
            if class_path:
                for argument in arguments:
                    if '=' in argument:
                        key, value = argument.split('=')
                        kwargs[key.strip()] = cls.__evaluate_argument(value.strip())
                    else:
                        args.append(cls.__evaluate_argument(argument.strip()))

                print(f"解析得到args: {args}")  # 打印原始参数字符串
                print(f"解析得到kwargs: {kwargs}")  # 打印原始参数字符串
                return class_path, args, kwargs
            else:
                # 添加错误日志或输出
                print("未能解析出类路径，请检查表达式格式是否正确:", expression)

        return None, [], {}

    @staticmethod
    def __evaluate_argument(argument):
        try:
            # This is a simplified evaluator that handles basic types
            return eval(argument, {"__builtins__": None}, {})
        except:
            return argument

    # @classmethod
    # def __parse_string_to_class(cls, expression: str) -> tuple:
    #     """
    #     使用正则表达式匹配类路径和参数
    #     :param expression: 表达式
    #     :return:
    #     """
    #     print(f"原始表达式: {expression}")  # 打印原始表达式
    #     pattern = r'([\w.]+)(?:\((.*)\))?'
    #     match = re.match(pattern, expression)
    #     args, kwargs = [], {}
    #
    #     if match:
    #         class_path = match.group(1)
    #         arguments = match.group(2)
    #         print(f"解析类路径: {class_path}")  # 打印解析后的类路径
    #         print(f"原始参数字符串: {arguments}")  # 打印原始参数字符串
    #
    #         if class_path:
    #             if arguments:
    #                 arguments = cls.__parse_arguments(arguments)
    #             else:
    #                 arguments = []
    #             return class_path, arguments
    #         else:
    #             # 添加错误日志或输出
    #             print("未能解析出类路径，请检查表达式格式是否正确:", expression)
    #     else:
    #         # 添加错误日志或输出
    #         print("正则表达式未匹配到任何内容，请检查表达式格式是否正确:", expression)
    #
    #     return None, []
    #
    # @staticmethod
    # def __parse_arguments(args_str) -> list:
    #     """
    #     解析类路径参数字符串
    #     :param args_str: 类参数字符串
    #     :return:
    #     """
    #     print(f"解析前的参数字符串: {args_str}")  # 打印接收到的原始参数字符串
    #     arguments = []
    #
    #     for arg in re.findall(r'"([^"]*)"|(\d+\.\d+)|(\d+)|([Tt]rue|[Ff]alse)', args_str):
    #         if arg[0]:
    #             # 字符串参数
    #             arguments.append(arg[0])
    #         elif arg[1]:
    #             # 浮点数参数
    #             arguments.append(float(arg[1]))
    #         elif arg[2]:
    #             # 整数参数
    #             arguments.append(int(arg[2]))
    #         elif arg[3]:
    #             # 布尔参数
    #             if arg[3].lower() == 'true':
    #                 arguments.append(True)
    #             else:
    #                 arguments.append(False)
    #
    #         print(f"参数解析结果: {arguments}")  # 打印每步解析后的参数
    #     return arguments

    def shutdown(self) -> None:
        """
        关闭调度器
        :return:
        """
        self.scheduler.shutdown()
