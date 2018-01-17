bind = "0.0.0.0:8080"

backlog = 64
workers = 20
timeout = 24*60*60

capture_output = True
accesslog = "/data1/logs/gunicorn_access.log"
errorlog = "/data1/logs/gunicorn_error.log"
logfile = "/data1/logs/gunicorn_debug.log"

access_log_format = '{' \
                    '"remote_addr": "%(h)s",' \
                    '"remote_user": "%(u)s",' \
                    '"local_time": "%(t)s",' \
                    '"request": "%(r)s",' \
                    '"status": "%(s)s",' \
                    '"bytes_sent": %(b)s,' \
                    '"referer": "%(f)s",' \
                    '"user_agent": "%(a)s",' \
                    '"upstream_request_time": %(D)s,' \
                    '"process_id": "%(p)s"' \
                    '}'
