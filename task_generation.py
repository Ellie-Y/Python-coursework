# coding:utf-8
import os
import random
import numpy
import math
import sqlite3


def generate_id():
    seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+=-"
    seedList = []
    for i in range(6):
        seedList.append(random.choice(seed))
    # Assign the random six strings to the variate of result
    result = ''.join(seedList)
    return result


def generate_duration():
    duration = numpy.random.exponential(scale=1.0)
    # rounded up
    return math.ceil(duration)


def generate_arrival():
    arrival = random.uniform(0, 100)
    return arrival


def connect_database():
    # connect.execute('''DROP TABLE tasks''')
    connect.execute('''CREATE TABLE IF NOT EXISTS tasks 
            (ID          CHAR     NOT NULL,
            ARRIVAL     FLOAT    NOT NULL,
            DURATION    INT    NOT NULL);''')

    task_num = 0
    while task_num <= 100:
        task_id = generate_id()
        task_arrival = generate_arrival()
        task_duration = generate_duration()
        # insert generated data
        connect.execute("INSERT INTO tasks (ID, ARRIVAL, DURATION) \
                         VALUES ('%s', '%f', '%d')" % (task_id, task_arrival, task_duration))
        task_num += 1
    connect.commit()



if __name__ == '__main__':
    connect = sqlite3.connect('task_data.db')
    connect_database()
    print('Database created successfully')
    connect.close()
