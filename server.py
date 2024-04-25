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

# 允许所有域名访问
CORS(app, resources={r"/api/*": {"origins": "*"}})
db = SQLAlchemy(app)


#做了一下差Agent表以验证该agent是否存在且在线
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



@app.route('/api/delete_task', methods=['DELETE'])
def delete_task():
    job_id = request.args.get('job_id')
    uuid = job_id.split("_")[0]
    task_name = job_id.split("_")[-1]
    logging.debug(f'Received job_id: {job_id}')
    print(f'Received job_id: {job_id}')
    # agent = Agent.query.filter_by(uuid=data['uuid']).first()
    message = {
        'job_id': job_id,
        'uuid': uuid,
        'action': 'delete_task',
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

# 定义路由 '/api/test'，接收POST请求
@app.route('/api/test', methods=['POST'])
def test_task():
    try:
        # 从POST请求的JSON数据中获取各个字段的值
        data = request.get_json()
        uuid = data.get('uuid')
        job_name = data.get('job_name')
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
            'job_name': job_name,
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


if __name__ == '__main__':
    with app.app_context():
        db.create_all()  # Create all tables if not exist within app context
    app.run(debug=True, port=5012)
