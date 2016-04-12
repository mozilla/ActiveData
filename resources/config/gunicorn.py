bind = "0.0.0.0:8080"

backlog = 64
workers = 5
timeout = 24*60*60

accesslog = "/logs/gunicorn_access.log"
errorlog = "/logs/gunicorn_error.log"

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
