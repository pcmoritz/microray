import redis
import threading
import json
import dill
import logging

dill.settings['recurse'] = True

FUNCTIONS = dict()

class RegisterFunction(threading.Thread):
  "Thread that registers functions that are defined interactively by the user."
  def __init__(self, r):
    threading.Thread.__init__(self)
    self.redis = r
    self.pubsub = self.redis.pubsub()
    self.pubsub.subscribe(["register_function"])

  def work(self, item):
    global FUNCTIONS
    function_id = item['data']
    key = "def:" + str(function_id)
    name = self.redis.hget(key, "name")
    code = self.redis.hget(key, "bytecode")
    if code is not None:
      print "registering", name, "as", function_id
      FUNCTIONS[function_id] = dill.loads(code)

  def run(self):
    for item in self.pubsub.listen():
      if item['data'] == "KILL":
        self.pubsub.unsubscribe()
        print self, "unsubscribed and finished"
        break
      else:
        self.work(item)

r = redis.Redis("localhost")
register_function = RegisterFunction(r)
register_function.start()

worker_id = r.incr("num_workers")
print "registered worker with worker_id ", worker_id

pubsub = r.pubsub()
pubsub.subscribe("execute:" + str(worker_id))

# The worker main loop
for item in pubsub.listen():
  task_id = item['data']
  if task_id != 1:
    task_data = r.hgetall("graph:" + task_id)
    args, function_id, num_results = json.loads(task_data["args"]), task_data["function_id"], task_data["num_results"]
    result_ids = []
    for i in range(int(num_results)):
      result_ids.append(task_data["result_id:" + str(i)])
    logging.info("called function with args {}" % args)
    results = FUNCTIONS[function_id](*args)
    pipe = r.pipeline()
    for result_id, result in zip(result_ids, results):
      r.set("store:" + result_id, json.dumps(result))
    r.hset("queue:" + task_id, "status", "done")
    pipe.execute()

client.join()
