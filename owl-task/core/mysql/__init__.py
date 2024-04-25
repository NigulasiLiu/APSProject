from .mysql_manage import MySQLManage

from appconfig.config.development import MYSQL_DB_HOST, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_NAME, MYSQL_DB_PORT

db = MySQLManage(MYSQL_DB_HOST, MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_NAME, MYSQL_DB_PORT)


def get_database() -> MySQLManage:
    return db
