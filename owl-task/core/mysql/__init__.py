from .mysql_manage import MySQLManage

from application.settings import MYSQL_DB_HOST, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_NAME, MYSQL_DB_PORT

db = MySQLManage(MYSQL_DB_HOST, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_NAME, MYSQL_DB_PORT)


def get_database() -> MySQLManage:
    # 确保数据库连接处于开启状态，如果关闭了则重新连接
    if not db.db.open:
        db.connect_to_database()
    return db
