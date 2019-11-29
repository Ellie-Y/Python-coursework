# coding:utf-8
import os
import re
import time
import sqlite3
import threading
import multiprocessing


def generate_clock():
    return 0


def retrieve_tasks():
    # retieve tasks by ASC order
    tasks_data = connect.execute(
        'SELECT ID, ARRIVAL, DURATION FROM tasks ORDER BY ARRIVAL ASC')
    return tasks_data


class SimulateSystem():
    def __init__(self, task_list, clock, queue):
        self.task_list = task_list
        self.queue = queue
        self.clock = clock
        self.final_time = 0

    def match_id(self):
        approved_tasks = []
        for row in self.task_list:
            now_time = self.clock + row[1]  # update clock to arrival time
            print('** [%s] : Task [%s] with duration [%d] enters the system'
                  % (now_time, row[0], row[2]))
            # at least 1 uppercase letter, lowercase letter, symbol or
            # at least 1 uppercase letter, number, symbol or
            # at least 1 lowercase letter, number, symbol or
            # at least 1 uppercase letter, lowercase letter, number
            if not re.match(r'(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[\W_])|(?=.*?[A-Z])(?=.*?[0-9])(?=.*?[\W_])|(?=.*?[a-z])(?=.*?[0-9])(?=.*?[\W_])|(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])', row[0]):
                print('** Task [%s] unfeasible and discarded' % row[0])
            else:
                approved_tasks.append(row)
                print('** Task [%s] accepted.' % row[0])
        self.task_list = approved_tasks
        self.clock = now_time

    def save_data(self):
        # iterate the task into a queue
        for task in self.task_list:
            self.queue.put_nowait(task)

    def process_data(self, i, d, ):
        loop = True
        while not self.queue.empty():
            try:
                if loop:
                    # processors are available in the beignning
                    # no on hold tasks at first time
                    task = self.queue.get_nowait()
                    loop = False
                else:
                    # non-blocking way to prevent infinite loop
                    task = self.queue.get_nowait()
                    print('** Task [%s] on hold.' % task[0])

                print('** [%f]: Task [%s] assigned to processor [%d #].' %
                      (self.clock, task[0], i))
                self.clock += task[2]
                print('** [%f] : Task [%s] completed.' % (self.clock, task[0]))
            except:
                # when queue is empty, break the loop
                break

        self.final_time = self.clock
        # pass the finish time to clock, the clock of d['clock'] is a variate
        d['clock'] = self.final_time

    def generate_processors(self):
        # process1 stores the approved tasks to a queue
        process1 = multiprocessing.Process(target=self.save_data, args=())
        process1.start()
        process1.join()

        process_list = []
        # using manager to share memory objects
        manager = multiprocessing.Manager()
        d = manager.dict()

        # generate three identical processors
        for i in range(3):
            p = threading.Thread(target=self.process_data, args=(i, d,))
            p.start()
            process_list.append(p)

        for p in process_list:
            p.join()

        # retrieve the latest clock time from d then pass to the clock
        self.clock = list(d.values())[0]
        connect.close()  # close SQLite


if __name__ == '__main__':
    connect = sqlite3.connect('task_data.db')
    # retrieve the task data from database
    task_list = retrieve_tasks()
    clock = generate_clock()
    queue = multiprocessing.Manager().Queue()

    print('** SYSTEM INITIALISED **')
    simulator = SimulateSystem(task_list, clock, queue)
    simulator.match_id()
    print()  # approved tasks
    simulator.generate_processors()
    print('** [%f] : SIMULATION COMPLETED. **' % simulator.clock)
