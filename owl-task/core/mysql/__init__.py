from .mysql_manage import MySQLManage

db = MySQLManage()


def get_database() -> MySQLManage:
    return db
