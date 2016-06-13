bind = "0.0.0.0:8080"

backlog = 64
workers = 20
timeout = 24*60*60

accesslog = "/logs/gunicorn_access.log"
errorlog = "/logs/gunicorn_error.log"
logfile = "/logs/gunicorn_debug.log"
loglevel = "debug"
