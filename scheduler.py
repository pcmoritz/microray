import redis
import random

r = redis.Redis("localhost")

pubsub = r.pubsub()
pubsub.subscribe("submit")

num_workers = int(r.get("num_workers"))
print "number of workers registered so far:", num_workers

for item in pubsub.listen():
  task_id = item['data']
  if task_id != 1:
    pipe = r.pipeline()
    r.publish("execute:" + str(random.randint(1, num_workers)), task_id)
    r.hset("queue:" + task_id, "status", "scheduled")
    pipe.execute()
