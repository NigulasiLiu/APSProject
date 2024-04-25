#!/usr/bin/python
# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2023/6/21 10:08 
# @File           : mian.py
# @IDE            : PyCharm
# @desc           : 简要说明
import datetime
import time


class Test:

    def __init__(self, name: str, age: int, bo: bool, **kwargs):
        self.name = name
        self.age = age
        self.bo = bo

    def main(self, mainpar: str, **kwargs) -> str:
        """
        主入口函数
        :return:
        """
        if self.bo:
            print('Test初始化参数为: {}，{}，{}'.format(self.name, self.age, self.bo))
            print('main参数为: {}'.format(mainpar))
            time.sleep(3)
            return "任务执行完成"
