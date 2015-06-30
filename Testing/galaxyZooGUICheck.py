__author__ = 'eran'
import requests
import json

'''
import logging
from logging.handlers import RotatingFileHandler
from Config import Config as mconf
cnf = mconf.Config().conf

log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
logFile = cnf['predLog']

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=1*1024*1024, backupCount=50, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

app_log = logging.getLogger('root')
app_log.setLevel(logging.INFO)
app_log.addHandler(my_handler)
'''




def main():
    with open("DB.txt") as f:
        for line in f:
            lineArr = line.split(',')
            user_id = lineArr[0]
            preconfigured_id = lineArr[1]
            cohort_id = lineArr[2]
            print line + ": " + send_intervention_for_user(user_id,preconfigured_id,cohort_id) + "\n"


def send_intervention_for_user(user_id,preconfigured_id,cohort_id):
    with requests.Session() as c:
        url = "http://experiments.zooniverse.org/users/" + user_id + "/interventions"

        #"http://demo.zooniverse.org/gz"
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

        try:
            if request.status_code == 500:
                content = json.loads(request.content)
                return "Invalid Parameters Sent"

            content = json.loads(request.content)
            intervention_id = content['id']
            return intervention_id
        except:
            return "Unable to read intervention."





if __name__ == "__main__":
    main()
