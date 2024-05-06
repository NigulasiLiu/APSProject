import atexit
import datetime
import json
from enum import Enum
from random import random

import pytz
from kafka import KafkaConsumer

from core.scheduler import Scheduler
from appconfig.settings import SCHEDULER_TASK, KAFKA_TOPIC, KAFKA_BROKER_URL, TASKS_ROOT
# from appconfig.config import MYSQL_DB_NAME, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_HOST, MYSQL_DB_PORT
from core.logger import logger
from core.mysql import get_database as get_mysql
from models import TaskDetail


class ScheduledTask:
    TASK_DIR = TASKS_ROOT

    class JobExecStrategy(Enum):
        interval = "interval"
        date = "date"
        cron = "cron"
        once = "once"

    def __init__(self, uuid):
        self.mongo = None
        self.mysql = None
        self.scheduler = None
        self.rd = None
        self.uuid = uuid

    def parse_message(self, message):
        """
        处理接收到的消息并根据消息内容添加定时任务
        :param message: 接收到的消息字典
        """
        if message.get("action") == "delete_task":
            self.delete_job(message.get("job_id"))
        elif message.get("action") == "pause_task":
            self.pend_job(message.get("job_id"))
        elif message.get("action") == "resume_task":
            self.restart_job(message.get("job_id"))
        else:
            shanghai_tz = pytz.timezone("Asia/Shanghai")
            try:

                exec_strategy = message.get("exec_strategy")
                job_params = {
                    "job_id": message.get("job_id"),
                    "job_class": message.get("job_class"),
                    "args": message.get("args"),
                    "kwargs": message.get("kwargs"),
                }
                task_detail = TaskDetail(
                    job_id=message.get("job_id"),
                    job_class=message.get("job_class"),
                    exec_strategy=message.get("exec_strategy"),
                    expression=message.get("expression"),
                    excute_times=message.get("excute_times"),
                    create_time=datetime.datetime.now(shanghai_tz),
                    start_time=message.get("start_date"),
                    end_time=message.get("end_date"),
                    taskDescription=message.get("taskDescription"),
                    exception="-",
                    retval="等待调度",
                    start_timestamp=None,
                    update_timestamp=None,
                    process_time=0,
                    status="waiting" if message.get("taskStatus") == "normal" else "pending"
                )
                print("尝试添加任务详情条目")
                # self.mysql.create_data(task_detail)
                self.create_job(task_detail)
                print("尝试添加任务详情")
                # 为不同的任务类型构建触发器并添加任务
                if exec_strategy == self.JobExecStrategy.cron.value:
                    job_params["expression"] = message.get("expression")
                    job_params["start_date"] = message.get("start_date")
                    job_params["end_date"] = message.get("end_date")
                    job_params["timezone"] = message.get("timezone")
                    self.scheduler.add_cron_job(**job_params)
                    print("添加Cron任务成功")
                elif exec_strategy == self.JobExecStrategy.interval.value:
                    job_params["expression"] = message.get("expression")
                    job_params["start_date"] = message.get("start_date")
                    job_params["end_date"] = message.get("end_date")
                    job_params["timezone"] = message.get("timezone")
                    job_params["jitter"] = message.get("jitter", None)
                    print("开始添加Interval任务")
                    self.scheduler.add_interval_job(**job_params)
                    print("添加Interval任务成功")
                elif exec_strategy == self.JobExecStrategy.date.value:
                    job_params["expression"] = message.get("executionTime")
                    print("开始添加Date任务")
                    self.scheduler.add_date_job(**job_params)
                    print("添加Date任务成功")
                elif exec_strategy == self.JobExecStrategy.once.value:
                    # 这种方式会自动执行事件监听器，用于保存执行任务完成后的日志
                    job_params["job_id"] = f"-{random.randint(1000, 9999)}" + job_params["job_id"]
                    self.scheduler.add_date_job(**job_params, expression=datetime.datetime.now())
                else:
                    raise ValueError("Unsupported execution strategy: {}".format(exec_strategy))

            except Exception as e:
                logger.error(f"Failed to process message: {str(e)}")
                print(f"处理消息失败: {str(e)}")

    def create_job(self, model_instance) -> None:
        # 添加任务详情条目
        try:
            job_id = model_instance.job_id
            task = self.mysql.get_data(TaskDetail, job_id=job_id)
            if task:
                self.mysql.put_data(TaskDetail, {"job_id": job_id}, model_instance)
            else:
                print(f":添加新任务{job_id}")
                # logger.info("任务 " + update_data["job_id"] + "不在 SCHEDULER_TASK 表中，将创建新记录")
                self.mysql.create_data(model_instance)
        except Exception as e:
            logger.error("处理任务编号 " + model_instance.job_id + " 时发生错误: {e}")
            model_instance.exception = str(e)
            self.mysql.create_data(model_instance)

    def delete_job(self, job_id: str) -> None:
        try:
            task = self.mysql.get_data(TaskDetail, job_id=job_id)
            if task:
                self.mysql.delete_data(TaskDetail, job_id=job_id)
                print(f"成功删除job_id为 {job_id} 的任务")
                self.scheduler.remove_job_by_jobid(job_id)
                # if task.status == "closing":
                #     self.mysql.delete_data(TaskDetail, job_id=job_id)
                #     print(f"成功删除job_id为 {job_id} 的任务")
                #     self.scheduler.remove_job_by_jobid(job_id)
            else:
                print(f"不存在job_id为 {job_id} 的任务,不需要删除")
        except Exception as e:
            print(f"删除任务 {job_id} 时发生错误: {e}")
            logger.error(f"删除任务 {job_id} 时发生错误: {e}")

    def pend_job(self, job_id: str) -> None:
        try:
            self.scheduler.pause_job_by_jobid(job_id)
            task = self.mysql.get_data(TaskDetail, job_id=job_id)
            # if task:
            #     if task.status == "running":
            #         # self.mysql.update_data(TaskDetail, {"job_id": "job_id"}, {"status": "pending"})
            #         # print(f"成功暂停job_id为 {job_id} 的任务")
            #         self.scheduler.pause_job_by_jobid(job_id)
            #     else:
            #         print(f"job_id为 {job_id} 的任务状态并不是running")
            # else:
            #     print(f"不存在job_id为 {job_id} 的任务，无需暂停")
        except Exception as e:
            print(f"暂停任务 {job_id} 时发生错误: {e}")
            logger.error(f"暂停任务 {job_id} 时发生错误: {e}")

    def restart_job(self, job_id: str) -> None:
        try:
            self.scheduler.resume_job_by_jobid(job_id)
            # task = self.mysql.get_data(TaskDetail, job_id=job_id)
            # if task:
            #     if task.status == "pending":
            #         # self.mysql.update_data(TaskDetail, {"job_id": "job_id"}, {"status": "running"})
            #         # print(f"成功恢复job_id为 {job_id} 的任务")
            #         self.scheduler.resume_job_by_jobid(job_id)
            #     else:
            #         print(f"job_id为 {job_id} 的任务状态并不是pending")
            # else:
            #     print(f"未恢复job_id为 {job_id} 的任务")
        except Exception as e:
            print(f"恢复任务 {job_id} 时发生错误: {e}")
            logger.error(f"恢复任务 {job_id} 时发生错误: {e}")

    def run(self) -> None:
        """
        启动监听订阅消息（阻塞）
        :return:
        """
        self.start_mysql()
        self.start_scheduler()
        self.start_kafka()

    def start_mysql(self) -> None:
        """
        启动 mysql
        :return:
        """
        self.mysql = get_mysql()
        if self.mysql:
            print("Scheduler 启动成功")
        # self.mysql.connect_to_database()#MYSQL_DB_HOST, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_NAME, MYSQL_DB_PORT

    def start_scheduler(self) -> None:
        """
        启动定时任务
        :return:
        """
        self.scheduler = Scheduler()
        self.scheduler.start()
        print("Scheduler 启动成功")

    def start_kafka(self) -> None:
        """
        启动 Kafka 消费者，监听并处理消息
        """
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER_URL,
            group_id='task_processing_group',  # 所有处理任务的消费者共享同一个 group_id
            auto_offset_reset='latest',  # 从最新的消息开始读取
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        logger.info("已成功启动程序，等待接收消息...")
        print("已成功启动程序，等待接收消息...")

        try:
            for message in consumer:
                data = message.value
                self.parse_message(data)
        except KeyboardInterrupt:
            print("程序终止")
        finally:
            consumer.close()

    def close(self) -> None:
        """
        # pycharm 执行停止，该函数无法正常被执行，怀疑是因为阻塞导致或 pycharm 的强制退出导致
        # 报错导致得退出，会被执行
        关闭程序
        :return:
        """
        self.mysql.close_all_sessions()
        # self.mysql.close_database_connection()
        # if self.scheduler:
        #     self.scheduler.shutdown()
        # if self.rd:
        #     self.rd.close_database_connection()

    # def start_mongo(self) -> None:
    #     """
    #     启动 mongo
    #     :return:
    #     """
    #     self.mongo = get_mongo()
    #     self.mongo.connect_to_database(MONGO_DB_URL, MONGO_DB_NAME)
    #
    # def start_redis(self) -> None:
    #     """
    #     启动 redis
    #     :return:
    #     """
    #     self.rd = get_redis()
    #     self.rd.connect_to_database(REDIS_DB_URL)

    # def run(self) -> None:
    #     """
    #     启动监听订阅消息（阻塞）
    #     :return:
    #     """
    #     self.start_mysql()
    #     self.start_scheduler()
    #     self.start_redis()
    #
    #     assert isinstance(self.rd, RedisManage)
    #
    #     pubsub = self.rd.subscribe(SUBSCRIBE)
    #
    #     logger.info("已成功启动程序，等待接收消息...")
    #     print("已成功启动程序，等待接收消息...")
    #
    #     # 处理接收到的消息
    #     for message in pubsub.listen():
    #         if message['type'] == 'message':
    #             data = json.loads(message['data'].decode('utf-8'))
    #             operation = data.get("operation")
    #             task = data.get("task")
    #             content = f"接收到任务：任务操作方式({operation})，任务详情：{task}"
    #             logger.info(content)
    #             print(content)
    #             getattr(self, operation)(**task)
    #         else:
    #             print("意外", message)

    # def error_record(self, name: str, error_info: str) -> None:
    #     """
    #     添加任务失败记录，并且将任务状态改为 False
    #     :param name: 任务编号
    #     :param error_info: 报错信息
    #     :return:
    #     """
    #     try:
    #         self.mongo.put_data(SCHEDULER_TASK, name, {"is_active": False})
    #         task = self.mongo.get_data(SCHEDULER_TASK, name)
    #         # 执行你想要在任务执行前执行的代码
    #         result = {
    #             "job_id": name,
    #             "job_class": task.get("job_class", None),
    #             "name": task.get("name", None),
    #             "group": task.get("group", None),
    #             "exec_strategy": task.get("exec_strategy", None),
    #             "expression": task.get("expression", None),
    #             "start_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #             "end_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #             "process_time": 0,
    #             "retval": "任务添加失败",
    #             "exception": error_info,
    #             "traceback": None
    #         }
    #         self.mongo.create_data(SCHEDULER_TASK_RECORD, result)
    #     except ValueError as e:
    #         logger.error(f"任务编号：{name}, 报错：{e}")


if __name__ == '__main__':
    agent_uuid = '6543210000'
    main = ScheduledTask(uuid=agent_uuid)
    atexit.register(main.close)
    main.run()
