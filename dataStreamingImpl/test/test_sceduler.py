import sched
import time


def do_something(sc):
    print("Doing stuff...")
    # do your stuff
    s.enter(60, 1, do_something, (sc,))


# Start the scheduler
s = sched.scheduler(time.time, time.sleep)
s.enter(60, 1, do_something, (s,))
s.run()

