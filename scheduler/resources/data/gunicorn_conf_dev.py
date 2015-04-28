debug = 'true'
daemon = 'false'

bind = 'unix:/tmp/scheduler.gunicorn.sock'

timeout = 120

# So we don't see the logging associated with the unhandled screen-resize 
# signals.
errorlog = '-'
loglevel = 'warning'

worker_class = 'sync'
