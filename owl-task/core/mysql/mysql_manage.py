from datetime import datetime
from typing import Any

import pymysql
from bson import ObjectId
from bson.errors import InvalidId
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
        self.db = db
        self.port = port
        self.engine = self.get_engine()
        self.Session = sessionmaker(bind=self.engine)

    def close_all_sessions(self):
        self.engine.dispose()  # 关闭 engine 和其维护的所有连接
    def get_engine(self):
        """
        Create and return the SQLAlchemy engine instance.
        """
        connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}?charset=utf8"
        engine = create_engine(connection_string, echo=True, pool_size=10, max_overflow=20, pool_pre_ping=True)
        Base.metadata.create_all(engine)  # Ensure all tables are created based on ORM definitions
        return engine

    def create_data(self, model_instance):
        """
        使用 session 插入新记录。
        """
        session = self.Session()
        try:
            session.add(model_instance)
            session.commit()
            print("数据插入成功")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"数据插入失败: {e}")
        finally:
            session.close()

    def get_data(self, model_class, **kwargs):
        """
        使用 ORM model 和 filter 条件来获取单条数据。
        """
        session = self.Session()
        try:
            result = session.query(model_class).filter_by(**kwargs).first()
            return result
        finally:
            session.close()

    def update_data(self, model_class, conditions, update_data):
        """
        使用 session 更新记录。
        """
        session = self.Session()
        try:
            obj = session.query(model_class).filter_by(**conditions).first()
            if obj:
                for key, value in update_data.items():
                    setattr(obj, key, value)
                session.commit()
                print("数据更新成功")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"数据更新失败: {e}")
        finally:
            session.close()

    def delete_data(self, model_class, **conditions):
        """
        使用 session 删除记录。
        """
        session = self.Session()
        try:
            objs = session.query(model_class).filter_by(**conditions)
            objs.delete()
            session.commit()
            print("数据删除成功")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"数据删除失败: {e}")
        finally:
            session.close()

    def update_all_data(self, model_class, update_data):
        """
        使用 session 更新所有记录的特定字段。
        """
        session = self.Session()
        try:
            # 更新所有记录
            session.query(model_class).update(update_data)
            session.commit()
            print("所有数据更新成功")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"数据更新失败: {e}")
        finally:
            session.close()

    def delete_all_data(self, model_class):
        """
        使用 session 删除所有记录。
        """
        session = self.Session()
        try:
            # 删除所有记录
            session.query(model_class).delete()
            session.commit()
            print("所有数据删除成功")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"数据删除失败: {e}")
        finally:
            session.close()

    # def test_connect(self) -> None:
    #     """
    #     测试连接
    #     :return:
    #     """
    #     try:
    #         # 发送一个查询以测试连接
    #         with self.db.cursor() as cursor:
    #             cursor.execute("SELECT 1")
    #             result = cursor.fetchone()
    #             if result:
    #                 print("MySQL 连接成功")
    #             else:
    #                 print("MySQL 连接失败")
    #     except pymysql.MySQLError as e:
    #         # 捕获并处理任何 MySQL 错误
    #         raise pymysql.MySQLError(f"MySQL 连接失败: {e}")
    #
    # def close_database_connection(self) -> None:
    #     """
    #     关闭 MySQL 连接
    #     :return:
    #     """
    #     if self.db:
    #         self.db.close()
    #
    #
    # def connect_to_database(self):
    #     """尝试连接到数据库，如果连接已关闭，则重新连接。"""
    #     try:
    #         if self.db is None or not self.db.open:
    #             self.db = pymysql.connect(
    #                 host=self.host,
    #                 user=self.user,
    #                 password=self.password,
    #                 database=self.db,
    #                 port=self.port
    #             )
    #             self.test_connect()
    #             #print("数据库连接成功")
    #     except pymysql.MySQLError as e:
    #         print(f"数据库连接失败: {e}")
    #
    # def create_session(self):
    #     """
    #     创建一个新的Session，用于数据库操作
    #     """
    #     Session = sessionmaker(bind=self.engine)
    #     return Session()
    #
    # # def create_data(self, table: str, data: dict) -> None:
    # #     """创建单个数据，并在插入前检查数据库连接状态。"""
    # #     self.connect_to_database()
    # #     columns = ', '.join(data.keys())
    # #     placeholders = ', '.join(['%s'] * len(data))
    # #     values = tuple(data.values())
    # #     query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    # #     try:
    # #         with self.db.cursor() as cursor:
    # #             cursor.execute(query, values)
    # #             self.db.commit()
    # #         print("数据插入成功")
    # #     except pymysql.MySQLError as e:
    # #         print(f"数据插入失败: {e}")
    # def create_data(self, table, data):
    #     """
    #     使用Session插入数据
    #     """
    #     session = self.create_session()
    #     try:
    #         new_record = table(**data)
    #         session.add(new_record)
    #         session.commit()
    #         print("数据插入成功")
    #     except Exception as e:
    #         session.rollback()
    #         print(f"数据插入失败: {e}")
    #     finally:
    #         session.close()
    # def get_data(self, table: str, **conditions) -> dict:
    #     """在获取数据前检查数据库连接状态。"""
    #     self.connect_to_database()
    #     condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
    #     values = tuple(conditions.values())
    #     query = f"SELECT * FROM {table} WHERE {condition_str} LIMIT 1"
    #     try:
    #         with self.db.cursor(pymysql.cursors.DictCursor) as cursor:
    #             cursor.execute(query, values)
    #             result = cursor.fetchone()
    #         return result
    #     except pymysql.MySQLError as e:
    #         print(f"数据获取失败: {e}")
    #         return None
    #
    # def put_data(self, table: str, conditions: dict, update_data: dict) -> None:
    #     """在更新数据前检查数据库连接状态。"""
    #     self.connect_to_database()
    #     condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
    #     update_str = ', '.join([f"{k}=%s" for k in update_data.keys()])
    #     values = tuple(update_data.values()) + tuple(conditions.values())
    #     query = f"UPDATE {table} SET {update_str} WHERE {condition_str}"
    #     try:
    #         with self.db.cursor() as cursor:
    #             cursor.execute(query, values)
    #             self.db.commit()
    #         #print("数据更新成功")
    #     except pymysql.MySQLError as e:
    #         print(f"数据更新失败: {e}")
    #
    # def delete_data(self, table: str, **conditions) -> None:
    #     """在删除数据前检查数据库连接状态。"""
    #     self.connect_to_database()
    #     if not conditions:
    #         raise ValueError("删除操作需要至少一个条件")
    #
    #     condition_str = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
    #     values = tuple(conditions.values())
    #     query = f"DELETE FROM {table} WHERE {condition_str}"
    #     try:
    #         with self.db.cursor() as cursor:
    #             cursor.execute(query, values)
    #             self.db.commit()
    #             if cursor.rowcount == 0:
    #                 print("没有找到匹配的记录进行删除")
    #             else:
    #                 print(f"成功删除 {cursor.rowcount} 条记录")
    #     except pymysql.MySQLError as e:
    #         print(f"删除数据时出错: {e}")
