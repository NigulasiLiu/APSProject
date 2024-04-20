import threading

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy.orm import declarative_base

from kafka import KafkaConsumer
import pytz
import json
import datetime
import logging

app = Flask(__name__)
Base = declarative_base()
#logging.basicConfig(level=logging.DEBUG)

# 配置调度器

# class ExtendedJob(Base):
#     __tablename__ = 'scheduled_tasks'  # 确保这个表名是你想要操作的表名
#
#     # 假设这是一个基本的 APScheduler Job 模型的扩展
#     id = Column(String(255), primary_key=True)
#     next_run_time = Column(Double, index=True)  # 下次运行时间，必须索引以优化查询性能
#     job_state = Column(LargeBinary)  # 存储任务的序列化状态
#
#     # 自定义字段
#     host_uuid = Column(String(255))
#     task_name = Column(String(255))
#     call_target = Column(String(255))
#     execution_strategy = Column(String(50))
#     task_description = Column(String(1024))
#     expression = Column(String(255))
#     start_time = Column(DateTime)
#     end_time = Column(DateTime)
#     execution_time = Column(DateTime)
#     task_status = Column(String(50))
#
# class CustomSQLAlchemyJobStore(SQLAlchemyJobStore):
#     def __init__(self, url):
#         engine = create_engine(url)
#         metadata = MetaData()  # 创建未绑定引擎的 MetaData 实例
#         metadata.reflect(bind=engine)  # 使用 reflect 方法加载现有的表结构
#
#         if 'scheduled_tasks' not in metadata.tables:
#             # 如果 'scheduled_tasks' 表不存在，则创建新表
#             Base.metadata.create_all(engine)
#
#         super().__init__(engine=engine)
#         self.jobs_t = Base.metadata.tables['scheduled_tasks']  # 确保此处正确引用已存在或新创建的表

#custom_jobstore = CustomSQLAlchemyJobStore(url='mysql+mysqldb://root:liubq@localhost/aps')

jobstores = {
    'default': SQLAlchemyJobStore(url='mysql+mysqlconnector://root:liubq@127.0.0.1/aps'),
}
# 默认添加新任务时的配置（job_defaults）
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}
scheduler = BackgroundScheduler(
    jobstore=jobstores,
    timezone=pytz.timezone('Asia/Shanghai'),
)

scheduler.configure(jobstores=jobstores, job_defaults=job_defaults,)

def execute_task1():
    print("-------------------------------------------------------------------------")
    print(f"Task 1")
    print("-------------------------------------------------------------------------")

def consume_tasks_dep2():
        consumer = None  # 在 try 块外初始化 consumer 为 None
        try:
            consumer = KafkaConsumer(
                'client_tasks',
                bootstrap_servers='localhost:9092',
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            for message in consumer:
                task = message.value
                if task['action'] == 'print_time':
                    create_task(task['uuid'], task['interval'])
                    # scheduler.add_job(create_task, 'interval', seconds=task['interval'], args=[task['uuid'], task['interval']], id=f"task_{task['uuid']}")
                if task['action'] == 'clear_task':
                    clear_tasks(task['uuid'])

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
        finally:
            if consumer is not None:
                consumer.close()  # 确保 consumer 已初始化后再关闭

def create_task(uuid, interval):
    job_id = f"task_{uuid}"
    # Adding or updating a job in the scheduler
    scheduler.add_job(execute_task, 'interval', seconds=interval, args=[uuid], id=job_id, replace_existing=True)
    logging.info(f"Task {job_id} created or updated with interval {interval} seconds")


def execute_task(task):
    print("-------------------------------------------------------------------------")
    print(f"Task {task['uuid']} scheduled with strategy {task['executionStrategy']}")
    print("-------------------------------------------------------------------------")


def consume_tasks():
    consumer = None
    try:
        consumer = KafkaConsumer(
            'client_tasks',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='6543210000'
        )

        for message in consumer:
            task = message.value
            if task['uuid'] == '6543210000':
                unique_time_stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                task_id = f"{task['uuid']}_{unique_time_stamp}"
                print(f"Received task: {task}")  # 打印接收到的任务信息

                if task['executionStrategy'] == 'interval':
                    scheduler.add_job(
                        func=execute_task, trigger='interval', seconds=int(task['expression']),
                        id=task_id,
                        # host_uuid=task['uuid'],
                        # task_name=task['taskName'],
                        # task_description=task['taskDescription'],
                        # calltarget=task['callTarget'],
                        # execution_strategy=task['executionStrategy'],
                        # expression=task['expression'],
                        # start_time=task['startTime'],
                        # end_time=task['endTime'],
                        # execution_time=task['executionTime'],
                        # task_status=task['taskStatus'],
                        args=[task], replace_existing=True
                    )
                elif task['executionStrategy'] == 'cron':
                    scheduler.add_job(
                        execute_task, 'cron', cron_expression=task['expression'],
                        id=task_id,
                        args=[task], replace_existing=True
                    )
                elif task['executionStrategy'] == 'date':
                    try:
                        job = scheduler.add_job(
                            func=execute_task, run_date=task['executionTime'],
                            id=task_id,
                            args=[task], replace_existing=False
                        )
                        print(f"Task {job.id} added successfully.")
                        if scheduler.get_jobs():
                            for job in scheduler.get_jobs():
                                print(
                                    f"Job ID: {job.id}, Next run time: {job.next_run_time}, jobstore:{job._jobstore_alias}")
                        else:
                            print("no jobs here")
                    except Exception as e:
                        print(f"Failed to add task: {e}")
                    scheduler.print_jobs('default')
                    print(f"Task {task['uuid']} scheduled with strategy {task['executionStrategy']}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        if consumer is not None:
            consumer.close()  # 确保 consumer 已初始化后再关闭


def clear_tasks(uuid):
    jobs = scheduler.get_jobs()
    for job in jobs:
        if job.id.startswith(f"task_{uuid}"):
            scheduler.remove_job(job.id)
            logging.info(f"Removed task {job.id}")
    # try:
    #     job_id_prefix = f"task_{uuid}"
    #     jobs = scheduler.get_jobs()
    #     for job in jobs:
    #         if job.id.startswith(job_id_prefix):
    #             scheduler.remove_job(job.id)
    #             logging.info(f"Removed task {job.id}")
    #
    #     return jsonify({'message': f'All tasks for UUID {uuid} cleared'}), 200
    # except Exception as e:
    #     logging.error(f"Failed to clear tasks for UUID {uuid}: {str(e)}")
    #     return jsonify({'error': f'Failed to clear tasks: {str(e)}'}), 500
def job_listener(event):
    try:
        job = scheduler.get_job(event.job_id)
        if job is not None:
            print(f"Job {job.id} encountered an event.")
        else:
            print(f"No job found with ID {event.job_id}")
    except Exception as e:
        print(f"Error handling job event: {e}")






scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
jobs = scheduler.get_jobs()

if jobs:
    for job in jobs:
        print(f"Job ID: {job.id}, Next run time: {job.next_run_time}")
else:
    print("no jobs here")
# if not scheduler.running:
#     scheduler.start()
#     print("Scheduler started.")

with app.app_context():
    if not scheduler.running:
        print("scheduler starting")
        scheduler.start()

@app.teardown_appcontext
def stop_scheduler(response_or_exc):
    if scheduler.running:
        print("scheduler ending")
        scheduler.shutdown(wait=False)
    return response_or_exc


if __name__ == "__main__":
    consume_tasks()
    app.run(debug=True, port=5013)
