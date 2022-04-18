#!usr/bin/env python3
import time
from threading import Thread
from time import perf_counter, sleep

class MyThread(Thread):
    # inherting everything from thread
    def __init__(self, thread_id, name, counter):
        Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.counter = counter
        # self.exitFlag = False

    def run(self):
        print(f"Starting {self.name}")
        self.long_Task(self.name, self.counter, 1)
        print(f"Starting {self.name}")

    def long_Task(self, thread_name, thread_counter, delay):
        start_time = perf_counter()

        while thread_counter:
            sleep(delay)
            print(f"Threadname: {thread_name} | Time: "
                  f"{perf_counter() - start_time}")
            thread_counter -= 1


thread1 = MyThread(1,"Threaddy", 5)
thread2 = MyThread(2, "Threadder", 2)

thread1.start()
thread2.start()

thread1.join()
thread2.join()

print("Finished")
