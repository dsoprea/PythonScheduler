import os

SMTP_HOSTNAME = os.environ.get(
                    'SCHED_EMAIL_SMTP_HOSTNAME', 
                    'localhost')

SMTP_DEFAULT_FROM = os.environ.get(
                        'SCHED_SMTP_DEFAULT_FROM', 
                        'scheduler@localhost')

SINK_EMAILS_TO_FILE = bool(int(os.environ.get(
                        'SCHED_DO_EMAIL_SINK', 
                        '0')))

EMAIL_SINK_FILEPATH = os.environ.get(
                        'SCHED_EMAIL_SINK_FILEPATH', 
                        '/tmp/scheduler_email_sink.txt')
