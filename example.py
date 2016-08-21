import ray

@ray.remote(num_results=2)
def f(x):
  return 1, 2

@ray.remote(num_results=2)
def g(x, y):
  return "hello", "world"

for i in range(1000):
  x, y = f(0)
  g(x, y)
  z = f(x)
