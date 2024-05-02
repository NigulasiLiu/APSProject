from marshmallow import Schema, fields, post_load
from models import *

class AgentSchema(Schema):
    id = fields.Int()
    host_name = fields.String()
    ip_address = fields.String()
    os_version = fields.String()
    status = fields.String()
    last_seen = fields.DateTime()
    disk_total = fields.String()
    mem_total = fields.String()
    mem_use = fields.String()
    cpu_use = fields.String()
    py_version = fields.String()
    processor_name = fields.String()
    processor_architecture = fields.String()
    uuid = fields.String()
    @post_load
    def make_agent(self, data, **kwargs):
        return Agent(**data)

class TaskDetailSchema(Schema):
    # id = fields.Int()
    job_id = fields.String()
    job_class = fields.String()
    exec_strategy = fields.String()
    expression = fields.String()
    create_time = fields.DateTime()
    start_time = fields.DateTime()
    end_time = fields.DateTime()
    taskDescription = fields.String()
    exception = fields.String()
    excute_times = fields.Integer()
    update_timestamp = fields.DateTime()
    start_timestamp = fields.DateTime()
    process_time = fields.Float()
    status = fields.String()
    retval = fields.String()
    @post_load
    def make_taskdetails(self, data, **kwargs):
        return TaskDetail(**data)

class MonitorFilesSchema(Schema):
    id = fields.Int()
    agentIP = fields.String()
    file_path = fields.String()
    change_type = fields.String()
    file_type = fields.String()
    timestamp = fields.DateTime()
    uuid = fields.String()
    @post_load
    def make_monitor_files(self, data, **kwargs):
        return MonitoredFile(**data)

class PortSchema(Schema):
    id = fields.Int()
    uuid = fields.String()
    host_ip = fields.String()
    port_number = fields.String()
    port_state = fields.String()
    port_name = fields.String()
    product = fields.String()
    version = fields.String()
    extrainfo = fields.String()
    script_http_title = fields.String()
    script_http_server_header = fields.String()
    @post_load
    def make_port(self, data, **kwargs):
        return PortInfo(**data)

class HostSchema(Schema):
    id = fields.Int()
    uuid = fields.String()
    ip = fields.String()
    state = fields.String()
    port_info = fields.Nested(PortSchema, many=True)
    @post_load
    def make_host(self, data, **kwargs):
        return HostInfo(**data)


class ProcessSchema(Schema):
    id = fields.Int()
    agentIP = fields.String()
    pid = fields.Int()
    name = fields.String()
    userName = fields.String()
    exe = fields.String()
    cmdline = fields.String()
    cpuPercent = fields.Float()
    memoryPercent = fields.Float()
    highRisk = fields.String()
    createTime = fields.DateTime()
    scanTime = fields.DateTime()
    uuid = fields.String()
    @post_load
    def make_process(self, data, **kwargs):
        return ProcessInfo(**data)


class VulnBugExpSchema(Schema):
    id = fields.Int()
    scanTime = fields.DateTime()
    ip = fields.String()
    bug_exp = fields.String()

class VulnBugPocSchema(Schema):
    id = fields.Int()
    scanTime = fields.DateTime()
    ip = fields.String()
    url = fields.String()
    bug_poc = fields.String()

class VulnFingerSchema(Schema):
    id = fields.Int()
    scanTime = fields.DateTime()
    ip = fields.String()
    url = fields.String()
    finger = fields.String()

class VulDetectionSchema(Schema):
    id = fields.Int()
    uuid = fields.String()
    scan_time = fields.DateTime()
    scanType = fields.String()
    ip = fields.String()
    port = fields.String()
    vul_detection_exp_result = fields.Nested(VulnBugExpSchema,many=True)
    vul_detection_poc_result = fields.Nested(VulnBugPocSchema,many=True)
    vul_detection_finger_result = fields.Nested(VulnFingerSchema,many=True)
    @post_load
    def make_vuldetection(self, data, **kwargs):
        return VulDetectionResult(**data)

class LinuxSecurityCheckSchema(Schema):
    id = fields.Int()
    ip = fields.String()
    check_name = fields.String()
    details = fields.String()
    adjustment_requirement = fields.String()
    instruction = fields.String()
    status = fields.String()
    last_checked = fields.String()
    uuid = fields.String()
    @post_load
    def make_linux_security_check(self, data, **kwargs):
        return LinuxSecurityCheck(**data)


class WindowsSecurityCheckSchema(Schema):
    id = fields.Int()
    ip = fields.String()
    check_name = fields.String()
    details = fields.String()
    adjustment_requirement = fields.String()
    instruction = fields.String()
    status = fields.String()
    last_checked = fields.String()
    uuid = fields.String()
    @post_load
    def make_windows_security_check(self, data, **kwargs):
        return WindowsSecurityCheck(**data)


class AssetMappingSchema(Schema):
    id = fields.Int()
    uuid = fields.String()
    ip = fields.String()
    protocol = fields.String()
    port = fields.String()
    service = fields.String()
    product = fields.String()
    version = fields.String()
    ostype = fields.String()
    @post_load
    def make_asset_mapping(self, data, **kwargs):
        return AssetMapping(**data)


class FileInfoSchema(Schema):
    id = fields.Int()
    uuid = fields.String()
    filename = fields.String()
    file_content_md5 = fields.String()
    filename_md5 = fields.String()
    ctime = fields.String()
    mtime = fields.String()
    atime = fields.String()
    host_IP = fields.String()
    host_name = fields.String()
    is_exists = fields.String()
    event_time = fields.String()
