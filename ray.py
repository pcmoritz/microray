import redis
import IPython
import dill
import hashlib
import inspect
import json

dill.settings['recurse'] = True

server = redis.Redis("localhost")

class Handle(object):
  """A Ray object handle."""

  def __init__(self, key):
    self.key = key

  def __repr__(self):
    return self.key

  def to_JSON(self):
    return self.key
    
class HandleEncoder(json.JSONEncoder):
  """Serializing object handles to JSON."""

  def default(self, o):
    return o.__dict__

def register(function):
  """Register a function in the redis function table and tell the workers that there is a new function available."""
  name = function.__module__ + "." + function.__name__
  function_id = hashlib.sha1(name).hexdigest()
  key = "def:" + function_id
  server.hset(key, "name", name)
  server.hset(key, "bytecode", dill.dumps(function))
  server.hset(key, "code", inspect.getsource(function))
  server.publish("register_function", function_id)
  return function_id

def remote(num_results=1):
  """The Ray remote decorator."""
  def decorator(function):
    function_id = register(function)
    def call(*args):
      task_id = hashlib.sha1()
      task_id.update(function_id)
      serialized_args = json.dumps(args, cls=HandleEncoder)

      result_ids = []
      for i in range(num_results):
        task_id.update(str(i))
        result_ids.append(task_id.hexdigest())

      task_id.update(serialized_args)
      key = "graph:" + task_id.hexdigest()
      pipe = server.pipeline()
      pipe.hset(key, "function_id", function_id)
      pipe.hset(key, "args", serialized_args)
      pipe.hset(key, "num_results", num_results)
      for i in range(num_results):
        pipe.hset(key, "result_id:" + str(i), result_ids[i])
        pipe.hset("object:" + result_ids[i], "task_id", task_id.hexdigest())
        pipe.hset("object:" + result_ids[i], "index", i)
      pipe.hset("queue:" + task_id.hexdigest(), "status", "waiting")
      pipe.publish("submit", task_id.hexdigest())
      pipe.execute()
      return [Handle(result_id) for result_id in result_ids]
    return call
  return decorator
