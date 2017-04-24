from heapq import heappush, heappop
from threading import Condition, Thread, current_thread
from datetime import datetime
from time import sleep


class Task:
    def __init__(self, task, time):
        self.task = task
        self.time = time

    def run(self):
        exec(self.task)

    def ready(self):
        return self.time <= datetime.now()

    def __str__(self):
        return "time: {}\ntask:\n\t{}".format(self.time, str(self.task).replace("\n", "\n\t"))


class Scheduler:
    def __init__(self, cv=Condition()):
        self.tasks = []
        self.cv = cv

        self.terminate = False

        self.run_scheduler()

    def schedule(self, task, time):
        with open('task_scheduling.txt', 'w') as ts:
            ts.write("Scheduling task from thread {}\n".format(current_thread()))

        with self.cv:
            heappush(self.tasks, Task(task, time))
            self.cv.notify()

    def run_scheduler(self):
        Thread(target=self.run_scheduler_helper).start()
        print("Started scheduler from thread {}".format(current_thread()))

    def run_scheduler_helper(self):
        with open('scheduler_out.txt', 'w') as f:
            f.write("Running scheduler on thread {}\n".format(current_thread()))

        with self.cv:
            while not self.terminate:

                # if no tasks are scheduled, wait until a task is available
                while len(self.tasks) == 0 and not self.terminate:
                    with open('scheduler_out.txt', 'a') as f:
                        f.write("No task found, sleeping\n")
                    self.cv.wait()

                while len(self.tasks) > 0 and not self.terminate:
                    if self.tasks[0].ready():
                        with open('scheduler_out.txt', 'a') as f:
                            f.write("Executing task {}\n".format(self.tasks[0]))
                        Thread(target=heappop(self.tasks).run).start()

        with open('scheduler_out.txt', 'a') as f:
            f.write("Terminating scheduler on thread {}\n".format(current_thread()))
        exit(0)


if __name__ == "__main__":
    cv = Condition()
    s = Scheduler(cv)

    # empty out.txt file
    open('out.txt', 'w').close()

    # note: the import current thread is not strictly needed since the generated threads can use the same imports as
    # the scheduler
    t = "from threading import current_thread\n" \
        "with open('out.txt', 'a') as f:\n" \
        "   f.write('hello world from thread {}\\n'.format(current_thread()))"

    try:
        while True:
            s.schedule(t, datetime.now())
            sleep(5)
    except KeyboardInterrupt:
        # notify the scheduler thread if it's waiting
        with cv:
            s.terminate = True
            cv.notify()

        print("Sent terminate message to scheduler")
        exit(0)


