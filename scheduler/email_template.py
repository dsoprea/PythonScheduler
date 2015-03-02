import logging
import datetime
import email.mime.text
import email.mime.multipart

import markdown
import jinja2

import scheduler.config.general
import scheduler.config.email

_LOGGER = logging.getLogger(__name__)

_SMTP_HOSTNAME = 'localhost'


class EmailTemplate(object):
    def __init__(self, to_list, subject_template, markdown_body_template=None, 
                 html_body_template=None, text_body_template=None):

        if markdown_body_template is not None and html_body_template is not None:
            raise ValueError("A markdown template can not be provided if an HTML "
                             "template was.")

        self.__to_list = to_list
        self.__subject_template = jinja2.Template(subject_template)
        self.__markdown_body_template = jinja2.Template(markdown_body_template) \
                                            if markdown_body_template is not None \
                                            else None

        self.__html_body_template = jinja2.Template(html_body_template) \
                                        if html_body_template is not None \
                                        else None

        self.__text_body_template = jinja2.Template(text_body_template) \
                                        if text_body_template is not None \
                                        else None

    def send(self, replacements={}):
        subject = self.__subject_template.render(**replacements)

        if self.__html_body_template is not None:
            html_body = self.__html_body_template.render(**replacements)
        else:
            html_body = None
        
        if self.__markdown_body_template is not None:
            markdown_body = self.__markdown_body_template.render(
                                **replacements)
            
            html_body = markdown.markdown(markdown_body)

        if self.__text_body_template is not None:
            text_body = self.__text_body_template.render(**replacements)
        else:
            text_body = None

        msg = email.mime.multipart.MIMEMultipart()
        msg['Subject'] = subject
        msg['To'] = ', '.join(self.__to_list)
        msg['From'] = scheduler.config.general.EMAIL_FROM_NAME
        msg.preamble = 'You will not see this in a MIME-aware mail reader.\n'

        if html_body is not None:
            html_part = email.mime.text.MIMEText(html_body, 'html')
            msg.attach(html_part)

        if text_body is not None:
            text_part = email.mime.text.MIMEText(text_body, 'plain')
            msg.attach(text_part)

        if scheduler.config.email.SINK_EMAILS_TO_FILE is True:
            now_dt = datetime.datetime.now()

            with open(scheduler.config.email.EMAIL_SINK_FILEPATH, 'a+') as f:
                f.write(str(now_dt))
                f.write(":\n\n")
                f.write(msg.as_string())
                f.write("\n")
                f.write("\n")
        else:
            s = smtplib.SMTP(scheduler.config.email.SMTP_HOSTNAME)
            s.sendmail(
                scheduler.config.email.SMTP_DEFAULT_FROM, 
                self.__to_list, 
                msg.as_string())

            s.quit()
