from datetime import datetime
from typing import Any

import pymysql
from bson import ObjectId
from bson.errors import InvalidId
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

Base = declarative_base()

class MySQLManage:
    """
    MySQL 数据库管理器
    """
    #db: pymysql.connections.Connection = None
    def __init__(self, host, user, password, db, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.dbname = db
        self.port = port
        self.db = None
        self.connect_to_database()

        self.engine = self.get_engine(host, user, password, db, port)
        # 使用 scoped_session 确保线程安全
        self.Session = scoped_session(sessionmaker(bind=self.engine))


    # def __init__(self, host, user, password, db, port=3306):
    #     self.connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"

    # def get_engine(self, host, user, password, db, port=3306):
    #     engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    #     Base.metadata.create_all(engine)  # 确保表存在
    #     return engine

    # def connect_to_database(self, host: str, username: str, password: str, database: str, port: int = 3306) -> None:
    #     """
    #     连接 MySQL 数据库
    #     :param host: 主机地址
    #     :param username: 用户名
    #     :param password: 密码
    #     :param database: 数据库名称
    #     :param port: 端口号，默认为 3306
    #     :return:
    #     """
    #     self.db = pymysql.connect(
    #         host=host,
    #         user=username,
    #         password=password,
    #         database=database,
    #         port=port
    #     )
    #     self.test_connect()
    def get_engine(self, host, user, password, db, port):
        """创建带连接池的数据库引擎"""
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}",
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=3600,
            echo=True  # 如果需要看到所有的SQL语句可以开启echo
        )
        Base.metadata.create_all(engine)
        return engine

    def get_session(self):
        """获取一个新的session，保证每次调用都是独立的会话"""
        return self.Session()

    def close_database_connection(self):
        """关闭连接池中的所有连接"""
        self.Session.remove()
        self.engine.dispose()  # 关闭 engine 和其所有连接
    def test_connect(self) -> None:
        """
        测试连接
        :return:
        """
        try:
            # 发送一个查询以测试连接
            with self.db.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result:
                    print("MySQL 连接成功")
                else:
                    print("MySQL 连接失败")
        except pymysql.MySQLError as e:
            # 捕获并处理任何 MySQL 错误
            raise pymysql.MySQLError(f"MySQL 连接失败: {e}")

    # def close_database_connection(self) -> None:
    #     """
    #     关闭 MySQL 连接
    #     :return:
    #     """
    #     if self.db:
    #         self.db.close()

    # 其他方法可以根据需要进行修改，以适应 MySQL 数据库操作
    # def create_data(self, collection: str, data: dict) -> InsertOneResult:
    #     """
    #     创建单个数据
    #     :param collection: 集合
    #     :param data: 数据
    #     """
    #     data['create_time'] = datetime.now()
    #     data['update_time'] = datetime.now()
    #     result = self.db[collection].insert_one(data)
    #     # 判断插入是否成功
    #     if result.acknowledged:
    #         return result
    #     else:
    #         raise ValueError("创建新数据失败")
    # def get_data(
    #         self,
    #         collection: str,
    #         _id: str = None,
    #         v_return_none: bool = False,
    #         v_schema: Any = None,
    #         is_object_id: bool = False,
    #         **kwargs
    # ) -> dict | None:
    #     """
    #     获取单个数据，默认使用 ID 查询，否则使用关键词查询
    #     :param collection: 集合
    #     :param _id: 数据 ID
    #     :param v_return_none: 是否返回空 None，否则抛出异常，默认抛出异常
    #     :param is_object_id: 是否为 ObjectId
    #     :param v_schema: 指定使用的序列化对象
    #     :return:
    #     """
    #     if _id and is_object_id:
    #         kwargs["_id"] = ObjectId(_id)
    #     params = self.filter_condition(**kwargs)
    #     data = self.db[collection].find_one(params)
    #     if not data and v_return_none:
    #         return None
    #     elif not data:
    #         raise ValueError("查询单个数据失败，未找到匹配的数据")
    #     elif data and v_schema:
    #         return v_schema(**data).dict()
    #     return data
    #
    # def put_data(self, collection: str, _id: str, data: dict, is_object_id: bool = False) -> UpdateResult:
    #     """
    #     更新数据
    #     :param collection: 集合
    #     :param _id: 编号
    #     :param data: 更新数据内容
    #     :param is_object_id: _id 是否为 ObjectId 类型
    #     :return:
    #     """
    #     new_data = {'$set': data}
    #     result = self.db[collection].update_one({'_id': ObjectId(_id) if is_object_id else _id}, new_data)
    #
    #     if result.matched_count > 0:
    #         return result
    #     else:
    #         raise ValueError("更新数据失败，未找到匹配的数据")



    # def create_data(self, table: str, data: dict) -> None:
    #     """
    #     创建单个数据
    #     :param table: 表名
    #     :param data: 数据字典
    #     """
    #     columns = ', '.join(data.keys())
    #     placeholders = ', '.join(['%s'] * len(data))
    #     values = tuple(data.values())
    #     query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    #     #print("create_query:"+query)
    #     with self.db.cursor() as cursor:
    #         cursor.execute(query, values)
    #         self.db.commit()
    #     print("数据插入成功")
    #
    # def get_data(self, table: str, **conditions) -> dict:
    #     """
    #     获取单个数据
    #     :param table: 表名
    #     :param conditions: 查询条件
    #     :return: 查询结果
    #     """
    #     condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
    #     values = tuple(conditions.values())
    #     query = f"SELECT * FROM {table} WHERE {condition_str} LIMIT 1"
    #     with self.db.cursor(pymysql.cursors.DictCursor) as cursor:
    #         cursor.execute(query, values)
    #         result = cursor.fetchone()
    #         return result
    #
    # def put_data(self, table: str, conditions: dict, update_data: dict) -> None:
    #     """
    #     更新数据
    #     :param table: 表名
    #     :param conditions: 查询条件
    #     :param update_data: 更新的数据字典
    #     """
    #     condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
    #     update_str = ', '.join([f"{k}=%s" for k in update_data.keys()])
    #     values = tuple(update_data.values()) + tuple(conditions.values())
    #     query = f"UPDATE {table} SET {update_str} WHERE {condition_str}"
    #     #print("put_query:"+query)
    #     with self.db.cursor() as cursor:
    #         cursor.execute(query, values)
    #         self.db.commit()
    #     print("数据更新成功")
    #
    # def delete_data(self, table: str, **conditions):
    #     """
    #     删除数据
    #     :param table: 表名
    #     :param conditions: 查询条件
    #     """
    #     if not conditions:
    #         raise ValueError("删除操作需要至少一个条件")
    #
    #     condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
    #     values = tuple(conditions.values())
    #     query = f"DELETE FROM {table} WHERE {condition_str}"
    #
    #     try:
    #         with self.db.cursor() as cursor:
    #             cursor.execute(query, values)
    #             self.db.commit()
    #             if cursor.rowcount == 0:
    #                 print("没有找到匹配的记录进行删除")
    #             else:
    #                 print(f"成功删除 {cursor.rowcount} 条记录")
    #     except pymysql.MySQLError as e:
    #         raise pymysql.MySQLError(f"删除数据时出错: {e}")

    def connect_to_database(self):
        """尝试连接到数据库，如果连接已关闭，则重新连接。"""
        try:
            if self.db is None or not self.db.open:
                self.db = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.dbname,
                    port=self.port
                )
                self.test_connect()
                #print("数据库连接成功")
        except pymysql.MySQLError as e:
            print(f"数据库连接失败: {e}")

    def create_data(self, table: str, data: dict) -> None:
        """创建单个数据，并在插入前检查数据库连接状态。"""
        self.connect_to_database()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        values = tuple(data.values())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        try:
            with self.db.cursor() as cursor:
                cursor.execute(query, values)
                self.db.commit()
            print("数据插入成功")
        except pymysql.MySQLError as e:
            print(f"数据插入失败: {e}")

    def get_data(self, table: str, **conditions) -> dict:
        """在获取数据前检查数据库连接状态。"""
        self.connect_to_database()
        condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
        values = tuple(conditions.values())
        query = f"SELECT * FROM {table} WHERE {condition_str} LIMIT 1"
        try:
            with self.db.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query, values)
                result = cursor.fetchone()
            return result
        except pymysql.MySQLError as e:
            print(f"数据获取失败: {e}")
            return None

    def put_data(self, table: str, conditions: dict, update_data: dict) -> None:
        """在更新数据前检查数据库连接状态。"""
        self.connect_to_database()
        condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
        update_str = ', '.join([f"{k}=%s" for k in update_data.keys()])
        values = tuple(update_data.values()) + tuple(conditions.values())
        query = f"UPDATE {table} SET {update_str} WHERE {condition_str}"
        try:
            with self.db.cursor() as cursor:
                cursor.execute(query, values)
                self.db.commit()
            #print("数据更新成功")
        except pymysql.MySQLError as e:
            print(f"数据更新失败: {e}")

    def delete_data(self, table: str, **conditions) -> None:
        """在删除数据前检查数据库连接状态。"""
        self.connect_to_database()
        if not conditions:
            raise ValueError("删除操作需要至少一个条件")

        condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
        values = tuple(conditions.values())
        query = f"DELETE FROM {table} WHERE {condition_str}"
        try:
            with self.db.cursor() as cursor:
                cursor.execute(query, values)
                self.db.commit()
                if cursor.rowcount == 0:
                    print("没有找到匹配的记录进行删除")
                else:
                    print(f"成功删除 {cursor.rowcount} 条记录")
        except pymysql.MySQLError as e:
            print(f"删除数据时出错: {e}")


    # @classmethod
    # def filter_condition(cls, **kwargs) -> dict:
    #     """
    #     过滤条件
    #     :param kwargs: 过滤条件
    #     :return:
    #     """
    #     params = {}
    #     for k, v in kwargs.items():
    #         if not v:
    #             continue
    #         elif isinstance(v, tuple):
    #             if v[0] == "like" and v[1]:
    #                 params[k] = {'$regex': v[1]}
    #             elif v[0] == "between" and len(v[1]) == 2:
    #                 params[k] = {'$gte': f"{v[1][0]} 00:00:00", '$lt': f"{v[1][1]} 23:59:59"}
    #             elif v[0] == "ObjectId" and v[1]:
    #                 try:
    #                     params[k] = ObjectId(v[1])
    #                 except InvalidId:
    #                     raise ValueError("任务编号格式不正确！")
    #         else:
    #             params[k] = v
    #     return params



