import atexit
import datetime
import json
from enum import Enum

from kafka import KafkaConsumer

from core.scheduler import Scheduler
from core.mongo import get_database as get_mongo
from application.settings import MONGO_DB_NAME, MONGO_DB_URL, REDIS_DB_URL, SUBSCRIBE, SCHEDULER_TASK, \
    SCHEDULER_TASK_RECORD, \
    MYSQL_DB_NAME, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_HOST, MYSQL_DB_PORT, KAFKA_TOPIC, KAFKA_BROKER_URL, \
    TASKS_ROOT
from core.redis import get_database as get_redis
from core.logger import logger
from core.redis.redis_manage import RedisManage
from core.mysql import get_database as get_mysql


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

    # def process_message(self, message):
    #     """
    #     处理接收到的消息并根据消息内容添加定时任务
    #     :param message: 接收到的消息字典
    #     """
    #
    #     if message.get('uuid') == self.uuid:
    #         try:
    #             print("收到消息:"+message.get('uuid'))
    #             print("job_class:"+message.get("job_class"))
    #             print("name:"+message.get("name"))
    #             print("args:"+message.get("args"))
    #             print("strategyrgs:"+message.get("exec_strategy"))
    #             exec_strategy = message.get("exec_strategy")
    #             job_params = {
    #                 "job_class": message.get("job_class"),
    #                 "name": message.get("name"),
    #                 "args": message.get("args", ()),
    #                 "kwargs": message.get("kwargs", {})
    #             }
    #
    #             # 为不同的任务类型构建触发器并添加任务
    #             if exec_strategy == self.JobExecStrategy.cron.value:
    #                 job_params["expression"] = message.get("expression")
    #                 job_params["start_date"] = message.get("start_date")
    #                 job_params["end_date"] = message.get("end_date")
    #                 job_params["timezone"] = message.get("timezone")
    #                 self.scheduler.add_cron_job(**job_params)
    #             elif exec_strategy == self.JobExecStrategy.date.value:
    #                 job_params["expression"] = message.get("expression")
    #                 self.scheduler.add_date_job(**job_params)
    #             elif exec_strategy == self.JobExecStrategy.interval.value:
    #                 job_params["expression"] = message.get("expression")
    #                 job_params["start_date"] = message.get("start_date")
    #                 job_params["end_date"] = message.get("end_date")
    #                 job_params["timezone"] = message.get("timezone")
    #                 job_params["jitter"] = message.get("jitter", None)
    #                 self.scheduler.add_interval_job(**job_params)
    #             else:
    #                 raise ValueError("Unsupported execution strategy")
    #
    #         except Exception as e:
    #             logger.error(f"Failed to process message: {str(e)}")
    #             # 可以选择发送错误反馈到日志或其他错误处理机制
    #
    #         # 根据 operation 调用相应的任务处理函数
    #         # 例如: getattr(self, operation)(**task)

    def process_message(self, message):
        """
        处理接收到的消息并根据消息内容添加定时任务
        :param message: 接收到的消息字典
        """

        if message.get('uuid') == self.uuid:
            try:
                exec_strategy = message.get("exec_strategy")
                job_params = {
                    "job_class": message.get("job_class"),
                    "name": message.get("name"),
                    # "args": self.__import_module(message.get("job_class"))[1],
                    # "kwargs": self.__import_module(message.get("job_class"))[2]
                    "args": message.get("args"),
                    "kwargs": message.get("kwargs")
                }

                print("收到消息: UUID={}".format(message.get('uuid')))
                print("job_class: {}".format(message.get("job_class")))
                print("args: {}".format(job_params["args"]))
                print("kwargs: {}".format(job_params["kwargs"]))
                print("name: {}".format(message.get("name")))
                print("strategy: {}".format(message.get("exec_strategy")))
                print("expression: {}".format(message.get("expression")))
                print("start_date: {}".format(message.get("start_date")))
                print("end_date: {}".format(message.get("end_date")))
                print("timezone: {}".format(message.get("timezone")))
                # 为不同的任务类型构建触发器并添加任务
                if exec_strategy == self.JobExecStrategy.cron.value:
                    job_params["expression"] = message.get("expression")
                    job_params["start_date"] = message.get("start_date")
                    job_params["end_date"] = message.get("end_date")
                    job_params["timezone"] = message.get("timezone")
                    self.scheduler.add_cron_job(**job_params)
                    print("添加Cron任务成功")
                elif exec_strategy == self.JobExecStrategy.date.value:
                    job_params["expression"] = message.get("expression")
                    self.scheduler.add_date_job(**job_params)
                    print("添加Date任务成功")
                elif exec_strategy == self.JobExecStrategy.interval.value:
                    job_params["expression"] = message.get("expression")
                    job_params["start_date"] = message.get("start_date")
                    job_params["end_date"] = message.get("end_date")
                    job_params["timezone"] = message.get("timezone")
                    job_params["jitter"] = message.get("jitter", None)
                    print("开始添加Interval任务")
                    self.scheduler.add_interval_job(**job_params)
                    print("添加Interval任务成功")
                else:
                    raise ValueError("Unsupported execution strategy: {}".format(exec_strategy))

            except Exception as e:
                logger.error(f"Failed to process message: {str(e)}")
                print(f"处理消息失败: {str(e)}")

            # 根据 operation 调用相应的任务处理函数
            # 例如: getattr(self, operation)(**task)

    def run(self) -> None:
        """
        启动监听订阅消息（阻塞）
        :return:
        """
        self.start_mysql()
        self.start_scheduler()
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
                if data:
                    self.process_message(data)
        except KeyboardInterrupt:
            print("程序终止")
        finally:
            consumer.close()

    # def add_job(self, exec_strategy: str, job_params: dict) -> None:
    #     """
    #     添加定时任务
    #     :param exec_strategy: 执行策略
    #     :param job_params: 执行参数
    #     :return:
    #     """
    #     name = job_params.get("name", None)
    #     error_info = None
    #     try:
    #         if exec_strategy == self.JobExecStrategy.interval.value:
    #             self.scheduler.add_interval_job(**job_params)
    #         elif exec_strategy == self.JobExecStrategy.cron.value:
    #             self.scheduler.add_cron_job(**job_params)
    #         elif exec_strategy == self.JobExecStrategy.date.value:
    #             self.scheduler.add_date_job(**job_params)
    #         elif exec_strategy == self.JobExecStrategy.once.value:
    #             # 这种方式会自动执行事件监听器，用于保存执行任务完成后的日志
    #             job_params["name"] = f"{name}-temp-{random.randint(1000, 9999)}"
    #             self.scheduler.add_date_job(**job_params, expression=datetime.datetime.now())
    #         else:
    #             raise ValueError("无效的触发器")
    #     except ConflictingIdError as e:
    #         # 任务编号已存在，重复添加报错
    #         error_info = "任务编号已存在"
    #     except ValueError as e:
    #         error_info = e.__str__()
    #
    #     if error_info:
    #         logger.error(f"任务编号：{name}，报错：{error_info}")
    #         self.error_record(name, error_info)

    def error_record(self, name, error_info):
        """
        添加任务失败记录，并且将任务状态改为 False
        :param mysql_manager: MySQLManage 实例
        :param name: 任务编号
        :param error_info: 报错信息
        :return:
        """
        try:
            # 更新任务状态为不活跃
            self.mysql.put_data('scheduler_task_record', {'id': name}, {"is_active": False})

            # 获取任务详情
            task = self.mysql.get_data('scheduler_task_record', job_id=name)
            if task is None:
                raise ValueError("任务不存在")

            # 构造记录失败的任务信息
            result = {
                "job_id": name,
                # "job_class": task.get("job_class"),
                "task_name": task.get("task_name"),
                # "group": task.get("group"),
                "exec_strategy": task.get("exec_strategy"),
                "expression": task.get("expression"),
                "start_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "process_time": 0,
                "retval": "任务添加失败",
                "exception": error_info,
                "traceback": None,
                "create_datetime": datetime.datetime.now(),  # 根据create_data函数中的实现添加
                "update_datetime": datetime.datetime.now()  # 根据create_data函数中的实现添加
            }

            # 插入任务执行记录
            self.mysql.create_data(SCHEDULER_TASK_RECORD, result)
            print("任务失败记录已添加")

        except Exception as e:
            logger.error(f"任务编号：{name}, 报错：{e}")

    def start_mysql(self) -> None:
        """
        启动 mysql
        :return:
        """
        self.mysql = get_mysql()
        self.mysql.connect_to_database(MYSQL_DB_HOST, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_NAME, MYSQL_DB_PORT)

    def start_scheduler(self) -> None:
        """
        启动定时任务
        :return:
        """
        self.scheduler = Scheduler()
        self.scheduler.start()
        print("Scheduler 启动成功")

    def close(self) -> None:
        """
        # pycharm 执行停止，该函数无法正常被执行，怀疑是因为阻塞导致或 pycharm 的强制退出导致
        # 报错导致得退出，会被执行
        关闭程序
        :return:
        """
        self.mysql.close_database_connection()
        if self.scheduler:
            self.scheduler.shutdown()
        if self.rd:
            self.rd.close_database_connection()

    def start_mongo(self) -> None:
        """
        启动 mongo
        :return:
        """
        self.mongo = get_mongo()
        self.mongo.connect_to_database(MONGO_DB_URL, MONGO_DB_NAME)

    def start_redis(self) -> None:
        """
        启动 redis
        :return:
        """
        self.rd = get_redis()
        self.rd.connect_to_database(REDIS_DB_URL)

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
