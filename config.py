# 定义配置对象
class Db_Config(object):
    SQLALCHEMY_DATABASE_URI = 'mysql://root:liubq@127.0.0.1:3306/flaskdb'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
#
# class Json_Config(object):
#     JSON_AS_ASCII = False