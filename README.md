# MicroRay

To try it out, you need to compile redis and start it using

```
./redis/src/redis-server
```

Go into the `microray` directory and start 5 workers using
```
python worker.py &
python worker.py &
python worker.py &
python worker.py &
python worker.py &
```

Start a scheduler using

```
python scheduler.py &
```

And then start a few drivers using

```
python example.py & python example.py & python example.py & python example.py & python example.py &
```

After each run, flush the redis database using

```
./redis/src/redis-cli
```

and type `flushall` in the command line interface. You can inspect the database by typing

```
keys *
```

in the CLI and get each of these keys with a command like

```
hget graph:78dedc22cd758c9be5ae30e2e0cff440f4868793 *
```