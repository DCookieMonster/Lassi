__author__ = 'dor'
import requests
import yaml
import MySQLdb
import datetime
from dateutil.parser import parse
def sql(user_id, city_name, country_name, project, subjects, created_at):
    # connect
    conn = MySQLdb.connect(host="localhost", user="root", passwd="9670", db="streamer")

    cursor = conn.cursor()
    try:
        datet=parse(created_at)
        time=datetime.datetime(datet.year,datet.month,datet.day,datet.hour,datet.minute,datet.second)
        cursor.execute("""INSERT INTO stream (user_id,project,subjects,created_at,country_name,city_name) VALUES (%s,%s,%s,%s,%s,%s)""",
                       (user_id,project,subjects,time,country_name,city_name))
        conn.commit()
    except MySQLdb.Error as e:
        conn.rollback()
    conn.close()


def stream():

    headers = {'Accept': 'application/vnd.zooevents.stream.v1+json'}
    url = 'http://event.zooniverse.org/classifications'
    r = requests.get(url, headers=headers,stream=True)
    # TODO: set the chunk_size to be large enough so as not to overwhelm the CPU
    for line in r.iter_lines(chunk_size=1024*2):
        if len(line)>10:
            if line!='Stream Start':
                x=yaml.load(line)
                #TODO:check which field to get to the database
                #TODO: what to do with NOT LOGIN users?
                #TODO: Create DB for this streaming
                if (x['project']=="galaxy_zoo"):
                    sql(x['user_id'],x['city_name'],x['country_name'],x['project'],x['subjects'],x['created_at'])






def Pusher(userId,projectID,cohort_id,preconfigured_id,text_message,intervention_channel):
    with requests.Session() as c:
        url="http://localhost:8080/"+userId+"/interventions"
        payload={
            'project_id': projectID,
            'intervention_type': 'interrupt',
            'cohort_id': cohort_id,
            'preconfigured_id':preconfigured_id,
            'text_message': text_message,
            'intervention_duration': '0',
            'intervention_channel': intervention_channel,
            'take_action':'immediately',
        }

        request=c.post(url,data=payload)
        print (request.content)

if __name__ == "__main__":
    stream()
