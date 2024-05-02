import ast
import logging
import re
from datetime import datetime

from flask import Flask, jsonify, request
from kafka import KafkaProducer

# from sqlalchemy import func

from config import Db_Config
from model_schema import *
from models import *
from flask_cors import CORS
# json解析
import json

app = Flask(__name__)
app.config.from_object(Db_Config)
db.init_app(app)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Initialize Kafka producer globally if needed across different routes
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@app.route('/api/pause_task', methods=['POST'])
def pause_task():
    job_id = request.args.get('job_id')
    uuid = job_id.split("_")[0]
    task_name = job_id.split("_")[-1]
    logging.debug(f'Received job_id: {job_id}')
    print(f'Received job_id: {job_id}')
    # agent = Agent.query.filter_by(uuid=data['uuid']).first()
    message = {
        'job_id': job_id,
        'action': 'pause_task',
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

@app.route('/api/resume_task', methods=['POST'])
def resume_task():
    job_id = request.args.get('job_id')
    uuid = job_id.split("_")[0]
    task_name = job_id.split("_")[-1]
    logging.debug(f'Received job_id: {job_id}')
    print(f'Received job_id: {job_id}')
    # agent = Agent.query.filter_by(uuid=data['uuid']).first()
    message = {
        'job_id': job_id,
        'action': 'resume_task',
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

@app.route('/api/add_task', methods=['POST'])
def add_task():
    try:
        # 从POST请求的JSON数据中获取各个字段的值
        data = request.get_json()
        uuid = data.get('uuid')
        job_name = data.get('job_name')
        task_description = data.get('taskDescription', 'no')
        job_class = data.get('callTarget')
        args = data.get('args'),
        kwargs = data.get('kwargs'),
        exec_strategy = data.get('executionStrategy')
        excute_times = 0
        expression = data.get('expression', '')
        start_date = data.get('startTime')
        end_date = data.get('endTime')
        execution_time = data.get('executionTime')
        task_status = data.get('taskStatus')
        print(f"expression:{expression}")
        logging.debug(f'Received data: {data}')

        # 如果需要将数据发送到Kafka，则可以在此处添加相应的逻辑
        message = {
            'job_id': f"{uuid}_{job_name}",
            'job_name': job_name,
            'taskDescription': task_description,
            'job_class': job_class,
            'args': args,
            'kwargs': kwargs,
            'exec_strategy': exec_strategy,
            'excute_times': excute_times,
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


# agent信息页面
@app.route('/api/agent/all', methods=['GET'])
def agent_query_all():
    res = {'status': 200, 'message': 'success'}
    try:
        page_size = int(request.args.get("page_size", default=10))
        page_number = int(request.args.get("page_number", default=1))
        agent = Agent.query.offset((page_number - 1) * page_size).limit(page_size).all()
        agent_schema = AgentSchema(many=True)
        agent_res = agent_schema.dumps(agent)
        agent_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), agent_res)
        res.update({'message': ast.literal_eval(agent_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/agent/query_uuid', methods=['GET'])
def agent_query_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        uuid = request.args.get('uuid')
        agent = Agent.query.filter_by(uuid=uuid).first()
        agent_schema = AgentSchema()
        agent_res = agent_schema.dumps(agent)
        res.update({'message': ast.literal_eval(agent_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/agent/query_ip', methods=['GET'])
def agent_query_ip():
    res = {'status': 200, 'message': 'success'}
    try:
        ip = request.args.get('ip_address')
        agent = Agent.query.filter_by(ip_address=ip).first()
        agent_schema = AgentSchema()
        agent_res = agent_schema.dumps(agent)
        res.update({'message': ast.literal_eval(agent_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/agent/delete', methods=['DELETE'])
def agent_delete_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        uuid = request.args.get('uuid')
        agent = Agent.query.filter_by(uuid=uuid).first()
        db.session.delete(agent)
        db.session.commit()
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 风险检测模块
@app.route('/api/monitored/all', methods=['GET'])
def monitored_query_files():
    res = {'status': 200, 'message': 'success'}
    try:
        page_size = int(request.args.get("page_size", default=10))
        page_number = int(request.args.get("page_number", default=10))
        monitorfiles = MonitoredFile.query.offset((page_number - 1) * page_size).limit(page_size).all()
        monitorfiles_schema = MonitorFilesSchema(many=True)
        monitorfiles_res = monitorfiles_schema.dumps(monitorfiles)
        monitorfiles_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), monitorfiles_res)
        res.update({'message': ast.literal_eval(monitorfiles_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/monitored/query_ip', methods=['GET'])
def monitored_query_files_ip():
    res = {'status': 200, 'message': 'success'}
    try:
        page_size = int(request.args.get("page_size", default=10))
        page_number = int(request.args.get("page_number", default=10))
        agentip = request.args.get("agent_ip")
        monitorfiles = MonitoredFile.query.filter_by(agentIP=agentip).offset((page_number - 1) * page_size).limit(
            page_size)
        monitorfiles_schema = MonitorFilesSchema(many=True)
        monitorfiles_res = monitorfiles_schema.dumps(monitorfiles)
        monitorfiles_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), monitorfiles_res)
        res.update({'message': ast.literal_eval(monitorfiles_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/monitored/query_uuid', methods=['GET'])
def monitored_query_files_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        page_size = int(request.args.get("page_size", default=10))
        page_number = int(request.args.get("page_number", default=10))
        uuid = request.args.get("uuid")
        monitorfiles = MonitoredFile.query.filter_by(uuid=uuid).offset((page_number - 1) * page_size).limit(page_size)
        monitorfiles_schema = MonitorFilesSchema(many=True)
        monitorfiles_res = monitorfiles_schema.dumps(monitorfiles)
        monitorfiles_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), monitorfiles_res)
        res.update({'message': ast.literal_eval(monitorfiles_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/monitored/delete', methods=['DELETE'])
def monitored_delete_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        uuid = request.args.get('uuid')
        MonitoredFile.query.filter_by(uuid=uuid).delete()
        db.session.commit()
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 端口服务检测页面
@app.route('/api/hostport/query', methods=['GET'])
def host_port_query_ip():
    res = {'status': 200, 'message': 'success'}
    try:
        hostip = request.args.get("host_ip")
        hostinfo = HostInfo.query.filter_by(ip=hostip).first()
        host_schema = HostSchema()
        porthost_res = host_schema.dumps(hostinfo)
        res.update({'message': ast.literal_eval(porthost_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/hostport/query_uuid', methods=['GET'])
def host_port_query_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        host_uuid = request.args.get("uuid")
        hostinfo = HostInfo.query.filter_by(uuid=host_uuid).first()
        host_schema = HostSchema()
        porthost_res = host_schema.dumps(hostinfo)
        res.update({'message': ast.literal_eval(porthost_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/hostport/delete', methods=['DELETE'])
def host_port_delete_ip():
    res = {'status': 200, 'message': 'success'}
    try:
        hostip = request.args.get("host_ip")
        HostInfo.query.filter_by(ip=hostip).delete()
        db.session.commit()
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/portinfo/all', methods=['GET'])
def all_port_info():
    res = {'status': 200, 'message': 'success'}
    try:
        portinfo = PortInfo.query.all()
        portinfo_schema = PortSchema(many=True)
        portdata = portinfo_schema.dumps(portinfo)
        # res.update({'message': ast.literal_eval(portdata)})
        res.update({'message': json.loads(portdata)})  # 使用 json.loads 替换
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 进程检测页面
@app.route('/api/process/query_ip', methods=['GET'])
def process_info_query_ip():
    res = {'status': 200, 'message': 'success'}
    try:
        page_size = int(request.args.get("page_size", default=10))
        page_number = int(request.args.get("page_number", default=10))
        agentip = request.args.get("host_ip")
        processinfo = ProcessInfo.query.filter_by(agentIP=agentip).offset((page_number - 1) * page_size).limit(
            page_size)
        process_schema = ProcessSchema(many=True)
        processinfo_res = process_schema.dumps(processinfo)
        processinfo_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), processinfo_res)
        res.update({'message': ast.literal_eval(processinfo_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/process/query_uuid', methods=['GET'])
def process_info_query_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        page_size = int(request.args.get("page_size", default=10))
        page_number = int(request.args.get("page_number", default=10))
        uuid = request.args.get("uuid")
        processinfo = ProcessInfo.query.filter_by(uuid=uuid).offset((page_number - 1) * page_size).limit(page_size)
        process_schema = ProcessSchema(many=True)
        processinfo_res = process_schema.dumps(processinfo)
        processinfo_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), processinfo_res)
        res.update({'message': ast.literal_eval(processinfo_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/process/delete', methods=['DELETE'])
def process_delete_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        uuid = request.args.get('uuid')
        ProcessInfo.query.filter_by(uuid=uuid).delete()
        db.session.commit()
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/process/all', methods=['GET'])
def process_info_query_all():
    res = {'status': 200, 'message': 'success'}
    try:
        processinfo = ProcessInfo.query.all()
        process_schema = ProcessSchema(many=True)
        processinfo_res = process_schema.dumps(processinfo)
        processinfo_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), processinfo_res)
        res.update({'message': ast.literal_eval(processinfo_res)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 漏洞扫描页面
@app.route('/api/vulndetetion/query', methods=['GET'])
def vuln_detetion_query_ip():
    res = {'status': 200, 'message': 'success'}
    try:
        ip = request.args.get("host_ip")
        vulndetetion = VulDetectionResult.query.filter_by(ip=ip).first()
        vulndetetion_schema = VulDetectionSchema()
        vulndetetion_res = vulndetetion_schema.dumps(vulndetetion)
        vulndetetion_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), vulndetetion_res)
        res['message'] = ast.literal_eval(vulndetetion_res)
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 漏洞扫描页面
@app.route('/api/vulndetetion/query_uuid', methods=['GET'])
def vuln_detetion_query_uuid():
    res = {'status': 200, 'message': 'success'}
    try:
        uuid = request.args.get("uuid")
        vulndetetion = VulDetectionResult.query.filter_by(uuid=uuid).first()
        vulndetetion_schema = VulDetectionSchema()
        vulndetetion_res = vulndetetion_schema.dumps(vulndetetion)
        vulndetetion_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), vulndetetion_res)
        res['message'] = ast.literal_eval(vulndetetion_res)
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/vulndetetion/all', methods=['GET'])
def vuln_detetion_query_all():
    res = {'status': 200, 'message': 'success'}
    try:
        vulndetetion = VulDetectionResult.query.all()
        vulndetetion_schema = VulDetectionSchema(many=True)
        vulndetetion_res = vulndetetion_schema.dumps(vulndetetion)
        vulndetetion_res = re.sub(date_pattern, lambda x: str(convert_to_timestamp(x.group())), vulndetetion_res)
        res['message'] = ast.literal_eval(vulndetetion_res)
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 资产测绘页面
@app.route('/api/asset_mapping/all', methods=['GET'])
def asset_mapping_query_all():
    """获取所有IP的资产测绘信息"""
    res = {'status': 200, 'message': 'success'}

    try:
        page_size = request.args.get('page_size', default=20)
        page_number = request.args.get('page_number', default=1)

        if page_size and page_number:
            page_number = max(1, int(page_number))
            page_size = max(1, int(page_size))
            assert_mapping = AssetMapping.query.offset((page_number - 1) * page_size).limit(page_size).all()
        else:
            assert_mapping = AssetMapping.query.all()

        assert_mapping_schema = AssetMappingSchema(many=True)
        data = assert_mapping_schema.dumps(assert_mapping)
        res['message'] = ast.literal_eval(data)
    except:
        res = {'status': 500, 'message': 'error'}
    return jsonify(res)


@app.route("/api/asset_mapping/query", methods=['GET'])
def asset_mapping_query_ip():
    """获取指定IP的资产测绘信息"""
    res = {'status': 200, 'message': 'success'}
    try:
        ip = request.args.get('ip')
        assert_mapping = AssetMapping.query.filter_by(ip=ip).all()
        assert_mapping_schema = AssetMappingSchema(many=True)
        data = assert_mapping_schema.dumps(assert_mapping)
        res.update({'message': ast.literal_eval(data)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route("/api/asset_mapping/delete", methods=['DELETE'])
def asset_mapping_delete_ip():
    """删除指定IP的资产测绘信息"""
    res = {'status': 200, 'message': 'success'}
    try:
        ip = request.args.get('ip')
        asset_mapping_to_delete = AssetMapping.query.filter_by(ip=ip).all()

        for entry in asset_mapping_to_delete:
            db.session.delete(entry)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 基线检查页面
@app.route('/api/baseline_check/linux/query_ip', methods=['GET'])
def linux_security_check_query_ip():
    """获取指定IP的Linux主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}

    try:
        ip = request.args.get('ip')
        linux_security_check = LinuxSecurityCheck.query.filter_by(ip=ip).all()
        linux_security_check_schema = LinuxSecurityCheckSchema(many=True)
        data = linux_security_check_schema.dumps(linux_security_check)
        res.update({'message': ast.literal_eval(data)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/baseline_check/linux/query_uuid', methods=['GET'])
def linux_security_check_query_uuid():
    """获取指定IP的Linux主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}

    try:
        uuid = request.args.get('uuid')
        linux_security_check = LinuxSecurityCheck.query.filter_by(uuid=uuid).all()
        linux_security_check_schema = LinuxSecurityCheckSchema(many=True)
        data = linux_security_check_schema.dumps(linux_security_check)
        res.update({'message': ast.literal_eval(data)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route("/api/baseline_check/linux/delete", methods=['DELETE'])
def linux_security_check_delete():
    """删除指定IP的Linux主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}
    try:
        uuid = request.args.get('uuid')
        linux_security_check_to_delete = LinuxSecurityCheck.query.filter_by(uuid=uuid).all()
        for entry in linux_security_check_to_delete:
            db.session.delete(entry)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/baseline_check/windows/query_ip', methods=['GET'])
def windows_security_check_query_ip():
    """获取指定IP的Windows主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}

    try:
        ip = request.args.get('ip')
        windows_security_check = WindowsSecurityCheck.query.filter_by(ip=ip).all()
        windows_security_check_schema = WindowsSecurityCheckSchema(many=True)
        data = windows_security_check_schema.dumps(windows_security_check)
        res.update({'message': ast.literal_eval(data)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/baseline_check/windows/query_uuid', methods=['GET'])
def windows_security_check_query_uuid():
    """获取指定IP的Windows主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}

    try:
        uuid = request.args.get('uuid')
        windows_security_check = WindowsSecurityCheck.query.filter_by(uuid=uuid).all()
        windows_security_check_schema = WindowsSecurityCheckSchema(many=True)
        data = windows_security_check_schema.dumps(windows_security_check)
        res.update({'message': ast.literal_eval(data)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route("/api/baseline_check/windows/delete", methods=['DELETE'])
def windows_security_check_delete():
    """删除指定IP的Windows主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}
    try:
        uuid = request.args.get('uuid')
        windows_security_check_to_delete = WindowsSecurityCheck.query.filter_by(uuid=uuid).all()
        for entry in windows_security_check_to_delete:
            db.session.delete(entry)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/baseline_check/linux/all', methods=['GET'])
def linux_security_check_all():
    """获取指定IP的Linux主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}

    try:
        linux_security_check = LinuxSecurityCheck.query.all()
        linux_security_check_schema = LinuxSecurityCheckSchema(many=True)
        data = linux_security_check_schema.dumps(linux_security_check)
        res.update({'message': ast.literal_eval(data)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/baseline_check/windows/all', methods=['GET'])
def windows_security_check_all():
    """获取指定IP的Windows主机基线检查信息"""
    res = {'status': 200, 'message': 'success'}

    try:
        windows_security_check = WindowsSecurityCheck.query.all()
        windows_security_check_schema = WindowsSecurityCheckSchema(many=True)
        data = windows_security_check_schema.dumps(windows_security_check)
        res.update({'message': ast.literal_eval(data)})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 文件监控页面
@app.route('/api/FileIntegrityInfo/query_uuid', methods=['GET'])
def FileIntegrityInfo_query_uuid():
    res = {'status': 200, 'message': 'success'}
    alerts = []
    result_dict = {}
    try:
        uuid = request.args.get('uuid')
        result = FileInfo.query.filter_by(uuid=uuid)
        for fileinfo in result:
            md5 = fileinfo.filename_md5
            record_data = {
                'filename': fileinfo.filename,
                'file_content_md5': fileinfo.file_content_md5,
                'ctime': fileinfo.ctime,
                'mtime': fileinfo.mtime,
                'atime': fileinfo.atime,
                'host_IP': fileinfo.host_IP,
                'host_name': fileinfo.host_name,
                'is_exists': fileinfo.is_exists,
                'event_time': fileinfo.event_time}

            if md5 in result_dict:
                result_dict[md5].append(record_data)
            else:
                result_dict[md5] = [record_data]
            # 设置告警状态
        for md5, records in result_dict.items():
            for record in records:
                filename = record["filename"].split(",")[-1]
                event_time = record["event_time"].split(",")[-1]
                hostname = record["host_name"].split(",")[-1]
                hostIP = record["host_IP"].split(",")[-1]
                # 上面是前端展示的告警信息
                md5_set = record["file_content_md5"].split(",")
                atimes = record["atime"].split(",")
                ctimes = record["ctime"].split(",")
                mtimes = record["mtime"].split(",")
                is_exists = record["is_exists"].split(",")
                if (len(set(md5_set)) != 1):  # 判断修改状态
                    alert_type = "modified"
                elif (((ctimes)) == ((mtimes)) and len(set(atimes)) == 1):  # 判断创建状态,之前判断状态有误，a=c=m 导致created的也是normal。
                    alert_type = "created"
                elif (int(is_exists[-1]) != 1):
                    alert_type = "deleted"
                else:
                    alert_type = "normal"
                items = dict(filename=filename, hostIP=hostIP, event_time=event_time, alert_type=alert_type,
                             hostname=hostname)
                alerts.append(items)
                res.update({'message': alerts})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/FileIntegrityInfo/query_ip', methods=['GET'])
def FileIntegrityInfo_query_ip():
    res = {'status': 200, 'message': 'success'}
    alerts = []
    result_dict = {}
    try:
        hostip = request.args.get('hostip')
        result = FileInfo.query.filter_by(host_IP=hostip)
        for fileinfo in result:
            md5 = fileinfo.filename_md5
            record_data = {
                'filename': fileinfo.filename,
                'file_content_md5': fileinfo.file_content_md5,
                'ctime': fileinfo.ctime,
                'mtime': fileinfo.mtime,
                'atime': fileinfo.atime,
                'host_IP': fileinfo.host_IP,
                'host_name': fileinfo.host_name,
                'is_exists': fileinfo.is_exists,
                'event_time': fileinfo.event_time}

            if md5 in result_dict:
                result_dict[md5].append(record_data)
            else:
                result_dict[md5] = [record_data]
            # 设置告警状态
        for md5, records in result_dict.items():
            for record in records:
                filename = record["filename"].split(",")[-1]
                event_time = record["event_time"].split(",")[-1]
                hostname = record["host_name"].split(",")[-1]
                hostIP = record["host_IP"].split(",")[-1]
                # 上面是前端展示的告警信息
                md5_set = record["file_content_md5"].split(",")
                atimes = record["atime"].split(",")
                ctimes = record["ctime"].split(",")
                mtimes = record["mtime"].split(",")
                is_exists = record["is_exists"].split(",")
                if (len(set(md5_set)) != 1):  # 判断修改状态
                    alert_type = "modified"
                elif (((ctimes)) == ((mtimes)) and len(set(atimes)) == 1):  # 判断创建状态,之前判断状态有误，a=c=m 导致created的也是normal。
                    alert_type = "created"
                elif (int(is_exists[-1]) != 1):
                    alert_type = "deleted"
                else:
                    alert_type = "normal"
                items = dict(filename=filename, hostIP=hostIP, event_time=event_time, alert_type=alert_type,
                             hostname=hostname)
                alerts.append(items)
                res.update({'message': alerts})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/FileIntegrityInfo/query_time', methods=['GET'])
def FileIntegrityInfo_query_time():
    res = {'status': 200, 'message': 'success'}
    alerts = []
    result_dict = {}
    try:
        start_time = float(request.args.get('start_time'))
        end_time = float(request.args.get('end_time'))
        result = FileInfo.query.filter(FileInfo.event_time.between(start_time, end_time)).all()
        for fileinfo in result:
            md5 = fileinfo.filename_md5
            record_data = {
                'filename': fileinfo.filename,
                'file_content_md5': fileinfo.file_content_md5,
                'ctime': fileinfo.ctime,
                'mtime': fileinfo.mtime,
                'atime': fileinfo.atime,
                'host_IP': fileinfo.host_IP,
                'host_name': fileinfo.host_name,
                'is_exists': fileinfo.is_exists,
                'event_time': fileinfo.event_time}
            if md5 in result_dict:
                result_dict[md5].append(record_data)
            else:
                result_dict[md5] = [record_data]
            # 设置告警状态
        for md5, records in result_dict.items():
            for record in records:
                filename = record["filename"].split(",")[-1]
                event_time = record["event_time"].split(",")[-1]
                hostname = record["host_name"].split(",")[-1]
                hostIP = record["host_IP"].split(",")[-1]
                # 上面是前端展示的告警信息
                md5_set = record["file_content_md5"].split(",")
                atimes = record["atime"].split(",")
                ctimes = record["ctime"].split(",")
                mtimes = record["mtime"].split(",")
                is_exists = record["is_exists"].split(",")
                if (len(set(md5_set)) != 1):  # 判断修改状态
                    alert_type = "modified"
                elif (((ctimes)) == ((mtimes)) and len(set(atimes)) == 1):  # 判断创建状态,之前判断状态有误，a=c=m 导致created的也是normal。
                    alert_type = "created"
                elif (int(is_exists[-1]) != 1):
                    alert_type = "deleted"
                else:
                    alert_type = "normal"
                items = dict(filename=filename, hostIP=hostIP, event_time=event_time, alert_type=alert_type,
                             hostname=hostname)
                alerts.append(items)
                res.update({'message': alerts})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/FileIntegrityInfo/all', methods=['GET'])
def FileIntegrityInfo_all():
    res = {'status': 200, 'message': 'success'}
    alerts = []
    result_dict = {}
    try:
        result = FileInfo.query.all()
        for fileinfo in result:
            md5 = fileinfo.filename_md5
            record_data = {
                'id': fileinfo.id,
                'uuid': fileinfo.uuid,
                'filename': fileinfo.filename,
                'file_content_md5': fileinfo.file_content_md5,
                'ctime': fileinfo.ctime,
                'mtime': fileinfo.mtime,
                'atime': fileinfo.atime,
                'host_IP': fileinfo.host_IP,
                'host_name': fileinfo.host_name,
                'is_exists': fileinfo.is_exists,
                'event_time': fileinfo.event_time
            }
            if md5 in result_dict:
                result_dict[md5].append(record_data)
            else:
                result_dict[md5] = [record_data]
            # 设置告警状态
        for md5, records in result_dict.items():
            for record in records:
                id = record["id"]
                uuid = record["uuid"].split(",")[-1]
                filename = record["filename"].split(",")[-1]
                event_time = record["event_time"].split(",")[-1]
                hostname = record["host_name"].split(",")[-1]
                hostIP = record["host_IP"].split(",")[-1]
                # 上面是前端展示的告警信息
                md5_set = record["file_content_md5"].split(",")
                atimes = record["atime"].split(",")
                ctimes = record["ctime"].split(",")
                mtimes = record["mtime"].split(",")
                is_exists = record["is_exists"].split(",")
                if (len(set(md5_set)) != 1):  # 判断修改状态
                    alert_type = "modified"
                elif (((ctimes)) == ((mtimes)) and len(set(atimes)) == 1):  # 判断创建状态,之前判断状态有误，a=c=m 导致created的也是normal。
                    alert_type = "created"
                elif (int(is_exists[-1]) != 1):
                    alert_type = "deleted"
                else:
                    alert_type = "normal"
                items = dict(id=id, uuid=uuid, filename=filename, hostIP=hostIP, event_time=event_time,
                             alert_type=alert_type,
                             hostname=hostname)
                alerts.append(items)
                res.update({'message': alerts})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


@app.route('/api/taskdetail/all', methods=['GET'])
def taskdetail_query_all():
    res = {'status': 200, 'message': 'success'}
    try:
        page_size = int(request.args.get("page_size", default=10))
        page_number = int(request.args.get("page_number", default=1))
        task = TaskDetail.query.offset((page_number - 1) * page_size).limit(page_size).all()
        task_schema = TaskDetailSchema(many=True)
        task_res = task_schema.dumps(task)
        # 使用json.loads来转换JSON字符串为Python字典
        task_res = json.loads(task_res)
        # 将所有日期转换为UNIX时间戳
        for item in task_res:
            for key in ['create_time', 'start_time', 'end_time', 'start_timestamp', 'update_timestamp']:  # 假设的日期字段
                if item[key]:
                    # 将ISO格式的日期字符串转换为datetime对象，然后转换为时间戳
                    dt = datetime.fromisoformat(item[key].replace('Z', '+00:00'))
                    item[key] = int(dt.timestamp())
        res.update({'message': task_res})
    except Exception as e:
        res = {'status': 500, 'message': str(e)}
    return jsonify(res)


# 时间戳转换
# 正则表达式，用于匹配特定格式的日期
date_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'


# 函数，将日期字符串转换为Unix时间戳
def convert_to_timestamp(date_str):
    # 移除'T'字符，以匹配格式 '%Y-%m-%d %H:%M:%S'
    date_str_formatted = date_str.replace('T', ' ')
    # 转换为datetime对象
    date_obj = datetime.strptime(date_str_formatted, '%Y-%m-%d %H:%M:%S')
    # 转换为Unix时间戳
    return int(date_obj.timestamp())


if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.run(debug=True, port=5000)
