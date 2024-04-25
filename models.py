from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class TaskDetail(db.Model):
    __tablename__ = 'scheduler_task'
    # id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.String(128), primary_key=True)
    job_class = db.Column(db.String(128))
    exec_strategy = db.Column(db.String(128))
    create_time = db.Column(db.TIMESTAMP)
    start_time = db.Column(db.TIMESTAMP)
    end_time = db.Column(db.TIMESTAMP)
    exception = db.Column(db.Text(collation='utf8mb4_unicode_ci'))
    excute_times = db.Column(db.Integer())
    update_timestamp = db.Column(db.TIMESTAMP)
    start_timestamp = db.Column(db.TIMESTAMP)
    process_time = db.Column(db.Float)
    retval = db.Column(db.Text(collation='utf8mb4_unicode_ci'))
    status = db.Column(db.String(128))


class Agent(db.Model):
    __tablename__ = 'agent'

    id = db.Column(db.Integer, primary_key=True)
    host_name = db.Column(db.String(255), nullable=False)
    ip_address = db.Column(db.String(255), nullable=False)
    os_version = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(255), nullable=False)
    last_seen = db.Column(db.DateTime)
    disk_total = db.Column(db.String(255), nullable=False)
    mem_total = db.Column(db.String(255), nullable=False)
    mem_use = db.Column(db.String(255), nullable=False)
    cpu_use = db.Column(db.String(255), nullable=False)
    py_version = db.Column(db.String(255), nullable=False)
    processor_name = db.Column(db.String(255), nullable=False)
    processor_architecture = db.Column(db.String(255), nullable=False)
    uuid = db.Column(db.String(255), nullable=False)

class HostInfo(db.Model):
    __tablename__ = 'host_info'

    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(db.String(256), index=True)
    ip = db.Column(db.String(15), index=False)
    state = db.Column(db.String(256), nullable=False)
    port_info = db.relationship('PortInfo',backref='port_infos',cascade="all,delete")

class MonitoredFile(db.Model):
    __tablename__ = 'monitored_files'

    id = db.Column(db.Integer, primary_key=True)
    agentIP = db.Column(db.Integer, nullable=False)
    file_path = db.Column(db.String(255), nullable=False)
    change_type = db.Column(db.String(10), nullable=False)
    file_type = db.Column(db.String(10))
    timestamp = db.Column(db.DateTime, server_default=db.FetchedValue())
    uuid = db.Column(db.String(255), nullable=False)



class PortInfo(db.Model):
    __tablename__ = 'port_info'

    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(db.ForeignKey('host_info.uuid'), index=True)
    host_ip = db.Column(db.String(256))
    port_number = db.Column(db.Integer, nullable=False)
    port_state = db.Column(db.String(256), nullable=False)
    port_name = db.Column(db.String(256), nullable=False)
    product = db.Column(db.String(256))
    version = db.Column(db.String(256))
    extrainfo = db.Column(db.String(256))
    script_http_title = db.Column(db.Text)
    script_http_server_header = db.Column(db.Text)



class ProcessInfo(db.Model):
    __tablename__ = 'process_info'

    id = db.Column(db.Integer, primary_key=True)
    agentIP = db.Column(db.Integer, nullable=False)
    scanTime = db.Column(db.DateTime, nullable=False)
    pid = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    userName = db.Column(db.String(255), nullable=False)
    exe = db.Column(db.String(255))
    cmdline = db.Column(db.Text, nullable=False)
    cpuPercent = db.Column(db.Float(asdecimal=True), nullable=False)
    memoryPercent = db.Column(db.Float(asdecimal=True), nullable=False)
    createTime = db.Column(db.DateTime, nullable=False)
    highRisk = db.Column(db.String(255), nullable=False)
    uuid = db.Column(db.String(255), nullable=False)



class VulDetectionResult(db.Model):
    __tablename__ = 'vul_detection_result'

    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(db.String(255), nullable=False, index=True)
    scan_time = db.Column(db.DateTime, nullable=False)
    scanType = db.Column(db.String(255), nullable=False)
    ip = db.Column(db.String(15), nullable=False, index=True)
    port = db.Column(db.Integer, nullable=False)
    vul_detection_exp_result = db.relationship('VulDetectionResultBugExp', primaryjoin='VulDetectionResult.ip == VulDetectionResultBugExp.ip', backref='vul_detection_results')
    vul_detection_poc_result = db.relationship('VulDetectionResultBugPoc', primaryjoin='VulDetectionResult.ip == VulDetectionResultBugPoc.ip', backref='vul_detection_results')
    vul_detection_finger_result = db.relationship('VulDetectionResultFinger', primaryjoin='VulDetectionResult.ip == VulDetectionResultFinger.ip', backref='vul_detection_results')


class VulDetectionResultBugExp(db.Model):
    __tablename__ = 'vul_detection_result_bug_exp'

    id = db.Column(db.Integer, primary_key=True)
    scanTime = db.Column(db.DateTime, nullable=False)
    ip = db.Column(db.ForeignKey('vul_detection_result.ip'), nullable=False, index=True)
    bug_exp = db.Column(db.String(255), nullable=False)


class VulDetectionResultBugPoc(db.Model):
    __tablename__ = 'vul_detection_result_bug_poc'

    id = db.Column(db.Integer, primary_key=True)
    scanTime = db.Column(db.DateTime, nullable=False)
    ip = db.Column(db.ForeignKey('vul_detection_result.ip'), nullable=False, index=True)
    url = db.Column(db.Text, nullable=False)
    bug_poc = db.Column(db.String(255), nullable=False)



class VulDetectionResultFinger(db.Model):
    __tablename__ = 'vul_detection_result_finger'

    id = db.Column(db.Integer, primary_key=True)
    scanTime = db.Column(db.DateTime, nullable=False)
    ip = db.Column(db.ForeignKey('vul_detection_result.ip'), nullable=False, index=True)
    url = db.Column(db.Text, nullable=False)
    finger = db.Column(db.String(255), nullable=False)


class LinuxSecurityCheck(db.Model):
    __tablename__ = 'linux_security_checks'

    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String(255, 'utf8_unicode_ci'))
    check_name = db.Column(db.String(255, 'utf8_unicode_ci'))
    details = db.Column(db.String(255, 'utf8_unicode_ci'))
    adjustment_requirement = db.Column(db.String(255, 'utf8_unicode_ci'))
    instruction = db.Column(db.String(255, 'utf8_unicode_ci'))
    status = db.Column(db.String(255, 'utf8_unicode_ci'))
    last_checked = db.Column(db.String(255, 'utf8_unicode_ci'))
    uuid = db.Column(db.String(255), nullable=False)


class WindowsSecurityCheck(db.Model):
    __tablename__ = 'windows_security_checks'

    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String(255, 'utf8_unicode_ci'))
    check_name = db.Column(db.String(255, 'utf8_unicode_ci'))
    details = db.Column(db.String(255, 'utf8_unicode_ci'))
    adjustment_requirement = db.Column(db.String(255, 'utf8_unicode_ci'))
    instruction = db.Column(db.String(255, 'utf8_unicode_ci'))
    status = db.Column(db.String(255, 'utf8_unicode_ci'))
    last_checked = db.Column(db.String(255, 'utf8_unicode_ci'))
    uuid = db.Column(db.String(255), nullable=False)


class AssetMapping(db.Model):
    __tablename__ = 'asset_mapping'

    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(db.String(255, 'utf8_unicode_ci'))
    ip = db.Column(db.String(255, 'utf8_unicode_ci'))
    protocol = db.Column(db.String(255, 'utf8_unicode_ci'))
    port = db.Column(db.String(255, 'utf8_unicode_ci'))
    service = db.Column(db.String(255, 'utf8_unicode_ci'))
    product = db.Column(db.String(255, 'utf8_unicode_ci'))
    version = db.Column(db.String(255, 'utf8_unicode_ci'))
    ostype = db.Column(db.String(255, 'utf8_unicode_ci'))


class FileInfo(db.Model):
    __tablename__ = 'fileIntegrityInfo'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    filename = db.Column(db.String(256))
    file_content_md5 = db.Column(db.String(256))
    filename_md5 = db.Column(db.String(256))
    ctime = db.Column(db.String(256))
    mtime = db.Column(db.String(256))
    atime = db.Column(db.String(256))
    host_IP = db.Column(db.String(256))
    host_name = db.Column(db.String(256))
    is_exists = db.Column(db.String(256))
    event_time = db.Column(db.String(256))
    # alert_type = db.Column(db.String(256))
    uuid = db.Column(db.String(255), nullable=False)