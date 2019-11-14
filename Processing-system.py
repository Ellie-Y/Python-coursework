# coding:utf-8
import sqlite3
import random
import numpy
import time
import multiprocessing
import re
import os
import math


def generate_id():
    seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+=-"
    seedList = []
    for i in range(6):
        seedList.append(random.choice(seed))
    result = ''.join(seedList)
    return result


def generate_duration():
    duration = numpy.random.exponential(scale=1.0)
    return math.ceil(duration)


def generate_arrival():
    arrival = random.uniform(0, 100)
    return arrival


def generate_clock():
    return 0


def connect_database():
    connect.execute('''DROP TABLE tasks''')
    connect.execute('''CREATE TABLE IF NOT EXISTS tasks 
            (ID          CHAR     NOT NULL,
            ARRIVAL     FLOAT    NOT NULL,
            DURATION    INT    NOT NULL);''')

    task_num = 0
    while task_num <= 100:
        task_id = generate_id()
        task_arrival = generate_arrival()
        task_duration = generate_duration()
        # 插入数据
        connect.execute("INSERT INTO tasks (ID, ARRIVAL, DURATION) \
                         VALUES ('%s', '%f', '%d')" % (task_id, task_arrival, task_duration))
        task_num += 1
    connect.commit()


def retrieve_tasks():
    tasks_data = connect.execute('SELECT ID, ARRIVAL, DURATION FROM tasks ORDER BY ARRIVAL ASC')
    return tasks_data

#类是传入参数然后操作参数实现想实现的功能
#外界使用类创建对象，然后让对象调用方法

'''目前的问题：
1. 当队列满了之后任务如何等待
2. 最后的总时间不对
3. 正则
'''

class SimulateSystem():
    def __init__(self, task_list, clock, queue):
        self.task_list = task_list
        self.queue = queue
        self.clock = clock


    def match_id(self):
        approved_tasks = []
        # findall() ? order of a-zA-Z
        # ?是否有，没有就走到前面再一次匹配
        matched_id = '[a-zA-Z]+[^a-zA-Z0-9]+'
        for row in self.task_list:
            now_time = self.clock + row[1]  # 让时钟更新成arrival时间
            print('** [%s] : Task [%s --- %f] with duration [%d] enters the system'
                  % (now_time, row[0], row[1], row[2]))
            #匹配 ID
            if not re.match(matched_id, row[0]):
                print('** Task [%s] unfeasible and discarded' % row[0])
            else:
                approved_tasks.append(row)
                print('** Task [%s] accepted.' % row[0])
        self.task_list = approved_tasks
        self.clock = now_time
        print('通过的任务！！')
        print(self.task_list)


    def save_data(self):
        #把任务遍历存到队列里面
        for task in self.task_list:
            if not self.queue.full():
                self.queue.put_nowait(task)
            else:
                task = self.queue.get()
                print('Task [%s] on hold.' % task[0])
                queue.put(task, block=True, timeout=None)


    def process_data(self, i):
        final_time = 0
        while True:
            task = self.queue.get()
            print('** [%f] : Task [%s] assigned to processor [%d #].'
                % (self.clock, task[0], i))
            # print('duration --- %d, task id --- %s' % (task[2], task[0]))
            time.sleep(task[2])
            self.clock += task[2]
            print('** [%f] : Task [%s] completed.' % (self.clock, task[0]))
            if self.queue.empty():
                print('队列里面的任务为空')
                final_time = self.clock
                break
        print('** [%f] : SIMULATION COMPLETED. **' % final_time)


    def generate_processors(self):
        #主进程和子进程的关系所以要用 Manager()
        #第一个进程把筛选过的任务存入queue
        process1 = multiprocessing.Process(target=self.save_data, args=())
        process1.start()
        process1.join()

        process_list = []
        for i in range(3):
            p = multiprocessing.Process(target=self.process_data, args=(i, ))
            p.start()
            process_list.append(p)

        for p in process_list:
            p.join()


if __name__ == '__main__':
    connect = sqlite3.connect('dataset-class.db')
    connect_database()
    #把从数据库获取的内容传入
    task_list = retrieve_tasks()
    clock = generate_clock()
    queue = multiprocessing.Manager().Queue()

    print('** SYSTEM INITIALISED **')
    time.sleep(1)
    simulator = SimulateSystem(task_list, clock, queue)
    simulator.match_id()
    simulator.generate_processors()
    #没有调用process_data所以时间没有更新, 调用了也没有更新？
    print('** [%f] : SIMULATION COMPLETED. **' % simulator.clock)
