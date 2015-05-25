__author__ = 'eran'
import MySQLdb
import sys
import requests
import json
from dis_predictor import dis_predictor

#dis_predictor
#import numpy as np
#import numpy
#from sklearn.externals import joblib
#import logging
#import datetime




Alg = None


def main():
    try:
        global Alg
        Alg = dis_predictor()
        print "Algorithm initialization successful.\n"
    except:
        print "Error, unable to start algorithm.\n"
        print sys.exc_info()[0]
        return
    prediction_loop()


def prediction_loop():
    try:
        conn = MySQLdb.connect(host="localhost", user="root", passwd="9670", db="streamer")
        conn.autocommit(True)
        cursor = conn.cursor()
    except:
        print "Unable to connect to DB.\n"
        print sys.exc_info()[0]
        return
    try:
        while True:
            cursor.execute("update stream set intervention_id=%s where id=%s", ("-1", "Not Logged In"))
            cursor.execute("SELECT id,user_id,created_at FROM stream WHERE intervention_id IS NULL")
            rows = cursor.fetchall()
            if len(rows) == 0:
                continue
            for row in rows:
                id = row[0]
                user_id = row[1]
                created_at = row[2]
                created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
                incentive = Alg.predicting(user_id, created_at)
                if incentive[0] == 1:
                    intervention_id = send_intervention_for_user(user_id)
                    print "User:" + user_id + " intervention_id: " + intervention_id
                    cursor.execute("update stream set intervention_id=%s where id=%s", (intervention_id, id))
                    print "done execute\n"
                else:
                    intervention_id = "-1"
                    print "User:" + user_id + " intervention_id: " + intervention_id
                    cursor.execute("update stream set intervention_id=%s where id=%s", (intervention_id, id))
                    print "done execute\n"

    except MySQLdb.Error as e:
        print "Connection to DB lost.\n"
        print e
        conn.rollback()
        conn.close()
        return


def send_intervention_for_user(user_id):
    with requests.Session() as c:
        url = "http://experiments.zooniverse.org/users/" + user_id + "/interventions"
        payload = {
            "project": "galaxy_zoo",
            "intervention_type": "prompt user about talk",
            "text_message": "please return",
            "cohort_id": 1,
            "time_duration": 60,
            "presentation_duration": 20,
            "intervention_channel": "web model",
            "take_action": "after_next_classification"
        }

        request = c.post(url, data=payload)
        # print 'send_intervention_for_user(' + userId + '):' + request.content
        try:
            content = json.loads(request.content)
            intervention_id = content['id']
            print 'SUCCESS send_intervention_for_user(' + user_id + '):' + intervention_id
            return intervention_id
        except:
            print 'ERROR: send_intervention_for_user(' + user_id + '):'
            return "-1"





if __name__ == "__main__":
    main()
