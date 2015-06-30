__author__ = 'dor'
import requests
import socket
import sys
import yaml
import MySQLdb
import datetime
from dateutil.parser import parse
import logging
from logging.handlers import RotatingFileHandler
import time
from Config import Config
cnf = Config.Config().conf
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
log_formatter.converter = time.gmtime

logFile = cnf['strmLog']
    #'/home/ise/Logs/streamer.log'
#logFile = '/home/eran/Documents/Logs/streamer.log'

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=1*1024*1024, backupCount=50, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

app_log = logging.getLogger('root')
app_log.setLevel(logging.INFO)
app_log.addHandler(my_handler)


def tmprint(txt):
    local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print "{0} - {1}".format(local_time, txt)

def sql(user_id, city_name, country_name, project, subjects, created_at):
    local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    # connect
    conn = MySQLdb.connect(host=cnf['host'], user=cnf['user'], passwd=cnf['password'], db=cnf['db'])

    cursor = conn.cursor()
    try:
        datet=parse(created_at)
        time=datetime.datetime(datet.year,datet.month,datet.day,datet.hour,datet.minute,datet.second)
        cursor.execute("""INSERT INTO stream (user_id,project,subjects,created_at,country_name,city_name,local_time) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                       (user_id,project,subjects,time,country_name,city_name,local_time))
        conn.commit()
    except MySQLdb.Error as e:
        app_log.info(e)
        conn.rollback()
    conn.close()


def stream():
    while True:
        try:
            headers = {'Accept': 'application/vnd.zooevents.stream.v1+json'}
            url = 'http://event.zooniverse.org/classifications'
            r = requests.get(url, headers=headers, stream=True, timeout=3600)
            # TODO: set the chunk_size to be large enough so as not to overwhelm the CPU
            for line in r.iter_lines(chunk_size=1024*2):
                if len(line)>10:
                    if (line!='Stream Start') and (line!='Heartbeat'):
                        x=yaml.load(line)
                        if (x['project']=="galaxy_zoo"):
                            sql(x['user_id'],x['city_name'],x['country_name'],x['project'],x['subjects'],x['created_at'])
                            local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                            app_log.info("User:{0} Record added.\n".format( x['user_id']))

        except (requests.Timeout, socket.error) as e:
            app_log.info("timeout\n")
            app_log.info(e)
            continue



def main():
    while True:
        try:
            app_log.info("starting stream\n")
            stream()
        except:
            app_log.info("Stream Crashed\n")
            app_log.info(sys.exc_info()[0])
            print sys.exc_info()
            continue

if __name__ == "__main__":
    main()
