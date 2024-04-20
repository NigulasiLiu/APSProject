import hashlib

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from kafka import KafkaProducer
import json
import datetime
import logging

logging.basicConfig(level=logging.DEBUG)  # 设置日志级别为 DEBUG


class Db_Config(object):
    SQLALCHEMY_DATABASE_URI = 'mysql://root:liubq@127.0.0.1:3306/flaskdb'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False


app = Flask(__name__)
app.config.from_object(Db_Config)

CORS(app)  # 启用 CORS
db = SQLAlchemy(app)


class Agent(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    uuid = db.Column(db.String(255), unique=True, nullable=False)
    host_name = db.Column(db.String(255), nullable=False)
    ip_address = db.Column(db.String(255), nullable=False)
    os_version = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(255), nullable=False)
    last_seen = db.Column(db.DateTime, nullable=False, default=datetime.datetime.utcnow)
    disk_total = db.Column(db.String(255))
    mem_total = db.Column(db.String(255))
    mem_use = db.Column(db.String(255))
    cpu_use = db.Column(db.String(255))
    py_version = db.Column(db.String(255))
    processor_name = db.Column(db.String(255))
    processor_architecture = db.Column(db.String(255))


@app.route('/api/send_task', methods=['POST'])
def send_task():
    uuid = request.args.get('uuid')
    logging.debug(f'Received uuid: {uuid}')
    # agent = Agent.query.filter_by(uuid=data['uuid']).first()
    agent = Agent.query.filter_by(uuid=uuid).first()
    if not agent or agent.status != 'Online':
        logging.error('Agent not online or does not exist')
        return jsonify({'error': 'Agent not online or does not exist'}), 404

    # If the agent is online, send a task
    message = {
        'uuid': uuid,
        'action': 'print_time',
        'interval': 2  # Interval set to 2 seconds
    }
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id='test'
        )
        producer.send('client_tasks', message)
        producer.flush()
        logging.debug('Task sent to Kafka')
        return jsonify({'status': 'success', 'message': 'Task sent to client'}), 200
    except Exception as e:
        logging.error(f'Error sending task to Kafka: {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        if 'producer' in locals() and producer is not None:
            producer.close()  # Close the producer after use


@app.route('/api/clear_task', methods=['DELETE'])
def clear_task():
    uuid = request.args.get('uuid')
    logging.debug(f'Received uuid: {uuid}')
    # agent = Agent.query.filter_by(uuid=data['uuid']).first()
    agent = Agent.query.filter_by(uuid=uuid).first()
    if not agent or agent.status != 'Online':
        logging.error('Agent not online or does not exist')
        return jsonify({'error': 'Agent not online or does not exist'}), 404
    # If the agent is online, send a task
    message = {
        'uuid': uuid,
        'action': 'clear_task'
    }
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send('client_tasks', message)
        producer.flush()
        logging.debug('Task sent to Kafka')
        return jsonify({'status': 'success', 'message': 'Task sent to client'}), 200
    except Exception as e:
        logging.error(f'Error sending task to Kafka: {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        if 'producer' in locals() and producer is not None:
            producer.close()  # Close the producer after use


def custom_partitioner(key_bytes, all_partitions, available_partitions):
    # 添加日志或打印来检查key_bytes和计算的分区索引
    print("Key:", key_bytes)
    print("All partitions:", all_partitions)
    print("Available partitions:", available_partitions)
    # 这里是一些示例逻辑
    partition_index = hash(key_bytes) % len(all_partitions)
    print("Chosen partition:", partition_index)
    return partition_index


# 定义路由 '/api/test'，接收POST请求
@app.route('/api/test', methods=['POST'])
def test_task():
    try:
        # 从POST请求的JSON数据中获取各个字段的值
        data = request.get_json()
        uuid = data.get('uuid')
        name = data.get('taskName')
        task_description = data.get('taskDescription', '')
        job_class = data.get('callTarget')
        args = data.get('args'),
        kwargs = data.get('kwargs'),
        exec_strategy = data.get('executionStrategy')
        expression = data.get('expression', '')
        start_date = data.get('startTime')
        end_date = data.get('endTime')
        execution_time = data.get('executionTime')
        task_status = data.get('taskStatus')

        logging.debug(f'Received data: {data}')

        # 如果需要将数据发送到Kafka，则可以在此处添加相应的逻辑
        message = {
            'uuid': uuid,
            'name': name,
            'taskDescription': task_description,
            'job_class': job_class,
            'args': args,
            'kwargs': kwargs,
            'exec_strategy': exec_strategy,
            'expression': expression,
            'start_date': start_date,
            'end_date': end_date,
            'executionTime': execution_time,
            'taskStatus': task_status
        }
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        # 使用 UUID 作为 key 发送消息
        producer.send('client_tasks', value=message)
        producer.flush()
        # logging.debug('Task sent to Kafka')

        return jsonify({'status': 'success', 'message': 'Task sent to client'}), 200

    except Exception as e:
        logging.error(f'Error processing task: {str(e)}')
        return jsonify({'error': str(e)}), 500

    finally:
        # 无论如何，都要关闭producer
        if 'producer' in locals() and producer is not None:
            producer.close()


@app.route('/api/test1', methods=['POST'])
def test_task1():
    try:
        # 从POST请求的JSON数据中获取各个字段的值
        data = request.get_json()
        uuid = data.get('uuid')
        task_name = data.get('taskName')
        task_description = data.get('taskDescription', '')
        call_target = data.get('callTarget')
        execution_strategy = data.get('executionStrategy')
        expression = data.get('expression', '')
        start_time = data.get('startTime')
        end_time = data.get('endTime')
        execution_time = data.get('executionTime')
        task_status = data.get('taskStatus')

        logging.debug(f'Data content: {data}')  # 打印data的内容

        # 在这里添加对字段的进一步处理逻辑，例如数据库操作或其他业务逻辑

        return jsonify({'status': 'success', 'message': 'Task processed successfully'}), 200

    except Exception as e:
        logging.error(f'Error processing task: {str(e)}')
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    with app.app_context():
        db.create_all()  # Create all tables if not exist within app context
    app.run(debug=True, port=5012)
