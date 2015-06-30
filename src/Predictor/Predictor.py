__author__ = 'eran'
import MySQLdb
import sys
import requests
import json
from dis_predictor import dis_predictor
import datetime
import logging
from logging.handlers import RotatingFileHandler
from Config import Config as mconf
cnf = mconf.Config().conf

log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
#logFile = '/home/ise/Logs/predictor.log'
logFile = cnf['predLog']

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=1*1024*1024, backupCount=50, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

app_log = logging.getLogger('root')
app_log.setLevel(logging.INFO)
app_log.addHandler(my_handler)

Alg = None

def tmprint(txt):
    local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print "{0} - {1}".format(local_time, txt)

def main():
    try:
        global Alg
        Alg = dis_predictor()
        app_log.info("Algorithm initialization successful.\n")
    except:
        app_log.info("Error, unable to start algorithm.\n")
        app_log.info(sys.exc_info()[0])
        return
    while True:
        try:
            prediction_loop()
        except:
            app_log.info("Prediction Loop failed.\n")
            app_log.info(sys.exc_info()[0])
            continue


def prediction_loop():
    try:
        conn = MySQLdb.connect(host=cnf['host'], user=cnf['user'], passwd=cnf['password'], db=cnf['db'])
        conn.autocommit(True)
        cursor = conn.cursor()
    except:
        app_log.info("Unable to connect to DB.\n")
        app_log.info(sys.exc_info()[0])
        return
    try:
        local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        app_log.info("predicting for %s"%local_time)
        while True:
            try:
                cursor.execute("update stream set intervention_id=%s where user_id=%s", ("-1", "Not Logged In"))
                cursor.execute("SELECT id,user_id,created_at FROM stream WHERE intervention_id IS NULL and local_time>='%s'"%local_time)
                rows = cursor.fetchall()
                if len(rows) == 0:
                    continue
                for row in rows:
                    try:
                        id = row[0]
                        user_id = row[1]
                        created_at = row[2]
                        if user_id == "Not Logged In":
                            continue

                        created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
                        incentive = Alg.intervene(user_id, created_at)
                        if incentive[0] > 0:
                            intervention_id = send_intervention_for_user(user_id,incentive[0],incentive[1])
                            app_log.info("User:" + user_id + " intervention_id: " + intervention_id + " preconfigured_id:%s cohort_id: %s"%(incentive[0], incentive[1]))
                            cursor.execute("update stream set preconfigured_id=%s,cohort_id=%s,algo_info=%s,intervention_id=%s where id=%s", (incentive[0],incentive[1],incentive[2],intervention_id, id))
                            app_log.info("done execute\n")
                        else:
                            intervention_id = "None"
                            app_log.info("User:" + user_id + " intervention_id: " + intervention_id + " preconfigured_id:%s cohort_id: %s"%(incentive[0], incentive[1]))
                            cursor.execute("update stream set preconfigured_id=%s,cohort_id=%s,algo_info=%s,intervention_id=%s where id=%s", (incentive[0],incentive[1],incentive[2],intervention_id, id))
                            app_log.info("done execute\n")
                    except:
                        app_log.info(sys.exc_info()[0])
                        cursor.execute("update stream set intervention_id=%s where id=%s", ("Failed", row[0]))
                        continue
            except:
                #sleep(5)
                app_log.info(sys.exc_info()[0])
                local_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                continue

    except MySQLdb.Error as e:
        app_log.info("Connection to DB lost.\n")
        app_log.info(e)
        conn.rollback()
        conn.close()
        return


def send_intervention_for_user(user_id,preconfigured_id,cohort_id):
    with requests.Session() as c:
        url = "http://experiments.zooniverse.org/users/" + user_id + "/interventions"
        payload = {
            "project": "galaxy_zoo",
            "intervention_type": "prompt user about talk",
            "text_message": "please return",
            "cohort_id": cohort_id,
            "time_duration": 120,
            "presentation_duration": 30,
            "intervention_channel": "web message",
            "take_action": "after_next_classification",
            "preconfigured_id": preconfigured_id,
            "experiment_name": "Zooniverse-MSR-BGU GalaxyZoo Experiment 1"

        }

        request = c.post(url, data=payload)
        # app_log.info('send_intervention_for_user(' + userId + '):' + request.content
        try:
            if request.status_code == 500:
                content = json.loads(request.content)
                app_log.info('ERROR: Invalid Parameters Sent, send_intervention_for_user(' + user_id + '):'+content)
                return "-1"

            content = json.loads(request.content)
            intervention_id = content['id']
            app_log.info('SUCCESS send_intervention_for_user(' + user_id + '):' + intervention_id)
            return intervention_id
        except:
            app_log.info('ERROR: send_intervention_for_user(' + user_id + '):')
            return "-1"





if __name__ == "__main__":
    main()
