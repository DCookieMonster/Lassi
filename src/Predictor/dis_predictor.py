__author__ = 'avisegal'

import numpy as np
import numpy
import logging
import sys
import datetime
from sklearn.externals import joblib
import MySQLdb

from contextlib import closing
import random
from logging.handlers import RotatingFileHandler
import time


# Perform before experiment starts: truncate user_cohorts ; truncate user_interventions

class dis_predictor:
    def median(self,lst):
        return numpy.median(numpy.array(lst))

    def avg(self,lst):
        return numpy.average(numpy.array(lst))


    def __init__(self):
        log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
        logFile = '/home/ise/Logs/dis_predictor.log'
        #logFile = '/home/eran/Documents/Logs/dis_predictor.log'

        my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=1*1024*1024, backupCount=50, encoding=None, delay=0)
        my_handler.setFormatter(log_formatter)
        my_handler.setLevel(logging.INFO)

        app_log = logging.getLogger('root')
        app_log.setLevel(logging.INFO)
        app_log.addHandler(my_handler)
        app_log.info("Loading and Initializing  Model")

        self.RANDINT_THRESHOLD = 0.4
        self.cohort_number=0
        self.y_leaving=0.
        self.y_staying=0.
        self.running_uid=0
        self.user_dict = {}
        self.user_past_session_time=[]        # session0_time, session1_time, session2_time ...
        self.user_past_session_dwell_time=[]  # session0_dwell_time, session1_dwell_time, session2_dwell_time ...
        self.user_past_tasks=[]               # session0_tasks, session1_tasks, session2_tasks, ....
        self.user_current_session_stats=[]    # timestamp_of_previous_task_in_this_session, this_sessions_tasks_count, this session_dwell_time, this_session_min_dwell_time, this_session_time
        self.user_past_session_stats=[]       # past_sessions_count, total_tasks_count, total_sessions_time, total_dwell_time

        #self.clf = joblib.load('/Users/avisegal/models/dtew/dismodel.pkl')
        #self.clf = joblib.load('/home/eran/Documents/Lassi/src/Algorithem/Model/dismodel.pkl')

        self.clf = joblib.load('/home/ise/Model/dismodel.pkl')

        app_log.info("Finished Loading  Model")

        app_log.info("Connecting to DB")
        self.db1 = MySQLdb.connect(host="localhost", user="root", passwd="9670", db="streamer")

        self.db2 = MySQLdb.connect(host="localhost", user="root", passwd="9670", db="streamer")

        self.get_user_cohorts = ("select * from user_cohorts order by user_id asc")

        self.insert_user_cohort = ("INSERT INTO user_cohorts VALUES (%s, %s, %s)")

        self.get_user_last_intervention_session = ("select * from user_interventions")

        self.replace_user_intervention_session = ("REPLACE INTO user_interventions VALUES (%s, %s)")

        self.get_user_cohorts = ("select * from user_cohorts order by user_id asc")

        self.get_user_fe_stats = ("select * from user_fe_stats")

        self.replace_user_fe_stats = ("REPLACE INTO user_fe_stats VALUES (%s, %s, %s, %s, %s)")

        #self.get_user_session_fe_stats = ("select session_time,session_avg_dwell_time, session_tasks from user_session_fe_stats where user_id = '%s' order by session_id asc")
        self.get_user_session_fe_stats = ("select * from user_session_fe_stats where user_id=%(uid)s order by session_id asc")

        self.insert_user_session_fe_stats = ("INSERT INTO user_session_fe_stats VALUES (%s, %s, %s, %s, %s)")

        app_log.info("Finished Connecting to DB")

        app_log.info("Loading User_Cohorts and User_Intervention_Session from DB")
        # table created with CREATE TABLE ngz.user_cohorts (user_id int(11) NOT NULL, user_original_id varchar(255) NOT NULL, cohort_id int(11) NOT NULL,PRIMARY KEY (user_id))
        self.user_cohorts_dict = {}

        #read user_id, user_original_id and cohort_id and create structs for these users
        with closing(self.db1.cursor()) as cur1:
            status = cur1.execute(self.get_user_cohorts)
            data = cur1.fetchall()
            for row in data:
                uid=int(row[0])
                original_uid = row[1]
                cohort=row[2]
                self.user_cohorts_dict[uid]=cohort
                self.user_dict[original_uid]=uid

                self.create_structs_for_user(uid)

                if (int(uid)>self.running_uid):
                    self.running_uid = int(uid)
            if (self.running_uid>0):
                self.running_uid+=1

            # table created with CREATE TABLE ngz.user_interventions (user_id int(11) NOT NULL, session_id int(11) NOT NULL,PRIMARY KEY (user_id))
            self.user_intervention_session_dict = {}
            status = cur1.execute(self.get_user_last_intervention_session)
            data = cur1.fetchall()
            for row in data:
                uid=row[0]
                session_id=row[1]
                self.user_intervention_session_dict[uid]=session_id
        app_log.info("Finished Loading User Cohorts from DB")

        app_log.info("Loading Features from DB")

        pscountidx=0; ptasksidx=1; pstimeidx=2; pdwellidx=3
        clastseenidx=0; ctaskidx=1;cdwellidx=2;cmdwellidx=3;csstartidx=4

         # table created with CREATE TABLE ngz.user_fe_stats (user_id int(11) NOT NULL, total_sessions int(11) NOT NULL, total_tasks int(11) NOT NULL, total_time int(11) NOT NULL, total_dwell int(11) NOT NULL, PRIMARY KEY (user_id))
         # table created with CREATE TABLE ngz.user_session_fe_stats (user_id int(11) NOT NULL, session_id int(11) NOT NULL, session_time int(11) NOT NULL, session_avg_dwell_time FLOAT NOT NULL, session_tasks int(11) NOT NULL, PRIMARY KEY(user_id, session_id))
        with closing(self.db1.cursor()) as cur1:
            status = cur1.execute(self.get_user_fe_stats)
            data = cur1.fetchall()
            for row in data:
                uid=int(row[0])
                total_sessions=int(row[1])
                total_tasks=int(row[2])
                total_time=int(row[3])
                total_dwell=int(row[4])
                self.user_past_session_stats[uid][pscountidx]= total_sessions
                self.user_past_session_stats[uid][ptasksidx]=total_tasks
                self.user_past_session_stats[uid][pdwellidx]=total_dwell
                self.user_past_session_stats[uid][pstimeidx]=total_time
                with closing(self.db2.cursor()) as cur2:
                    status = cur2.execute(self.get_user_session_fe_stats, {'uid':uid})
                    data2 = cur2.fetchall()
                    for row2 in data2:
                        session_time=int(row2[0])
                        session_avg_dwell_time=float(row2[1])
                        session_tasks=int(row2[2])
                        self.user_past_session_time[uid].append(session_time)
                        self.user_past_session_dwell_time[uid].append(session_avg_dwell_time) #average!
                        self.user_past_tasks[uid].append(session_tasks)

        app_log.info("Finished Loading Features from DB")

    # NOTE: Highly dependant on local user_ids starting at Zero and incrementing by 1 !!!
    def create_structs_for_user(self,user_id):
        # create data structures
        self.user_past_session_time.append([])        # session0_time, session1_time, session2_time ...
        self.user_past_session_dwell_time.append([])  # session0_dwell_time, session1_dwell_time, session2_dwell_time ...
        self.user_past_tasks.append([])              # session0_tasks, session1_tasks, session2_tasks, ....
        self.user_past_session_stats.append([])       # (0)past_sessions_count, (1)total_tasks_count,
                                                   # (2)total_sessions_time, (3)total_dwell_time
        self.user_current_session_stats.append([])    # (0)timestamp_of_previous_task_in_this_session,
                                                       # (1)this_sessions_tasks_count,#  (2)this session_dwell_time,
                                                       # (3)this_session_min_dwell_time, (4)this_session_start_time

        self.user_past_session_stats[user_id].append(0)
        self.user_past_session_stats[user_id].append(0)
        self.user_past_session_stats[user_id].append(0)
        self.user_past_session_stats[user_id].append(0)

        self.user_current_session_stats[user_id].append(-1) #identify first task in session
        self.user_current_session_stats[user_id].append(0)
        self.user_current_session_stats[user_id].append(0)
        self.user_current_session_stats[user_id].append(-1) # identify no min dwell time
        self.user_current_session_stats[user_id].append(0)


    def fe(self,user_id_str,created_at_str):
        user_sid=user_id_str
        created_at=datetime.datetime.strptime(created_at_str,'%Y-%m-%d %H:%M:%S')
        pscountidx=0; ptasksidx=1; pstimeidx=2; pdwellidx=3
        clastseenidx=0; ctaskidx=1;cdwellidx=2;cmdwellidx=3;csstartidx=4
        new_session = False

         # get integer user_id from dictionary or create new if non existent
        if user_sid not in self.user_dict:  # first time encountering user
            self.user_dict[user_sid]=self.running_uid
            user_id=self.running_uid
            self.running_uid+=1
            # create data structures
            self.create_structs_for_user(user_id)

        else: # user is known
            user_id = self.user_dict[user_sid]

        # update all auxilary structures

        last_seen=self.user_current_session_stats[user_id][clastseenidx]

        if (last_seen == -1): # first task for this user
            self.user_current_session_stats[user_id][clastseenidx]=created_at # this will be previous task time stamp for next task
            self.user_current_session_stats[user_id][ctaskidx]=1 # first task
            self.user_current_session_stats[user_id][csstartidx]=created_at # initialize session start time
            self.user_current_session_stats[user_id][cdwellidx]=0 # zero dwell for first task in session
            self.user_current_session_stats[user_id][cmdwellidx]=-1 # no min dwell for first task in session
            new_session = True

        else:
            delta = created_at-last_seen
            if ((delta.days == 0) and (delta.seconds <=1800)): # still in this session
                self.user_current_session_stats[user_id][ctaskidx]+=1 # increase this sessions tasks by one
                self.user_current_session_stats[user_id][cdwellidx]+=(created_at-last_seen).total_seconds() #add to session dwell time
                min_dwell = self.user_current_session_stats[user_id][cmdwellidx]
                if (min_dwell == -1) or ((created_at-last_seen).total_seconds() < min_dwell):  # update min dwell
                    self.user_current_session_stats[user_id][cmdwellidx] = (created_at-last_seen).total_seconds()
                self.user_current_session_stats[user_id][clastseenidx]=created_at # this will be previous task ts for next task

            else: # new session
                new_session = True
                self.user_past_session_stats[user_id][pscountidx]+=1 # one more session just ended
                self.user_past_session_stats[user_id][ptasksidx]+=self.user_current_session_stats[user_id][ctaskidx] # add task counts
                self.user_past_session_stats[user_id][pdwellidx]+=self.user_current_session_stats[user_id][cdwellidx] # add dwell time
                past_session_time=0
                if (self.user_current_session_stats[user_id][clastseenidx] != -1): # one task in this session
                    past_session_time=(self.user_current_session_stats[user_id][clastseenidx]-self.user_current_session_stats[user_id][csstartidx]).total_seconds()
                self.user_past_session_stats[user_id][pstimeidx]+=past_session_time # add session duration

                # write to db for persistence
                total_sessions= self.user_past_session_stats[user_id][pscountidx]
                total_tasks=self.user_past_session_stats[user_id][ptasksidx]
                total_time=self.user_past_session_stats[user_id][pstimeidx]
                total_dwell=self.user_past_session_stats[user_id][pdwellidx]
                with closing(self.db1.cursor()) as cur1:
                    status = cur1.execute(self.replace_user_fe_stats,(user_id,total_sessions, total_tasks, total_time, total_dwell))
                self.db1.commit()

                # update running history
                self.user_past_session_time[user_id].append(past_session_time)
                self.user_past_session_dwell_time[user_id].append(self.user_current_session_stats[user_id][cdwellidx]/self.user_current_session_stats[user_id][ctaskidx]) #average!
                self.user_past_tasks[user_id].append(self.user_current_session_stats[user_id][ctaskidx])

                # write to db for persistence
                session_avg_dwell_time = self.user_current_session_stats[user_id][cdwellidx]/self.user_current_session_stats[user_id][ctaskidx]
                session_tasks = self.user_current_session_stats[user_id][ctaskidx]
                with closing(self.db1.cursor()) as cur1:
                    status = cur1.execute(self.insert_user_session_fe_stats,(user_id,total_sessions,past_session_time,session_avg_dwell_time,session_tasks))
                self.db1.commit()

                # update current session data
                self.user_current_session_stats[user_id][clastseenidx]=created_at # this will be previous task time stamp for next task
                self.user_current_session_stats[user_id][ctaskidx] =1 # first task of this session
                self.user_current_session_stats[user_id][csstartidx]=created_at # initialize session start time
                self.user_current_session_stats[user_id][cdwellidx]=0 # zero dwell for first task in session
                self.user_current_session_stats[user_id][cmdwellidx]=-1 # no min dwell for first task in session

        # compute the features

        u_sessionCount = self.user_past_session_stats[user_id][pscountidx]
        s_minDwell= self.user_current_session_stats[user_id][cmdwellidx]
        s_avgDwell= self.user_current_session_stats[user_id][cdwellidx]/self.user_current_session_stats[user_id][ctaskidx]
        s_sessionTasks=self.user_current_session_stats[user_id][ctaskidx]
        s_sessionTime=(created_at-self.user_current_session_stats[user_id][csstartidx]).total_seconds()

        if (self.user_past_session_stats[user_id][pscountidx] == 0): #no past sessions
            u_bHavePastSession=0
            u_avgSessionTasks=0
            u_medianSessionTasks=0
            u_recentAvgSessionTasks=0
            u_sessionTasksvsUserMedian=0
            u_sessionTasksvsRecentMedian=0
            u_avgSessionTime=0
            u_sessionTimevsRecentAvg=0
            u_sessionTimevsUserMedian=0
            u_sessionAvgDwellvsUserAvg=0
            u_sessionAvgDwellvsRecentAvg=0
        else:
            u_bHavePastSession=1
            u_avgSessionTasks = self.user_past_session_stats[user_id][ptasksidx]/self.user_past_session_stats[user_id][pscountidx]
            u_medianSessionTasks=self.median(self.user_past_tasks[user_id])
            len1 =  np.clip(len(self.user_past_tasks[user_id]), 1, 10)
            u_recentAvgSessionTasks =self.avg(self.user_past_tasks[user_id][-len1:])
            u_sessionTasksvsUserMedian=self.user_current_session_stats[user_id][ctaskidx]-u_medianSessionTasks
            u_sessionTasksvsRecentMedian=self.user_current_session_stats[user_id][ctaskidx] - self.median(self.user_past_tasks[user_id][-len1:])
            u_avgSessionTime=self.avg(self.user_past_session_time[user_id])
            len2 =  np.clip(len(self.user_past_session_time[user_id]), 1, 10)
            this_session_time=created_at-self.user_current_session_stats[user_id][csstartidx]
            u_sessionTimevsRecentAvg=this_session_time.total_seconds() - self.avg(self.user_past_session_time[user_id][-len2:])
            u_sessionTimevsUserMedian=this_session_time.total_seconds() - self.median(self.user_past_session_time[user_id])
            this_session_dwell_time= self.user_current_session_stats[user_id][cdwellidx]/self.user_current_session_stats[user_id][ctaskidx]
            u_sessionAvgDwellvsUserAvg=this_session_dwell_time-self.avg(self.user_past_session_dwell_time[user_id])
            len3 =  np.clip(len(self.user_past_session_dwell_time[user_id]), 1, 10)
            u_sessionAvgDwellvsRecentAvg=this_session_dwell_time-self.avg(self.user_past_session_dwell_time[user_id][-len3:])

        X_t=np.array([u_bHavePastSession,
                        u_sessionCount,
                        u_avgSessionTasks,
                        u_medianSessionTasks,
                        u_recentAvgSessionTasks,
                        u_sessionTasksvsUserMedian,
                        u_sessionTasksvsRecentMedian,
                        u_avgSessionTime,
                        u_sessionTimevsRecentAvg,
                        u_sessionTimevsUserMedian,
                        u_sessionAvgDwellvsUserAvg,
                        u_sessionAvgDwellvsRecentAvg,
                        s_minDwell,
                        s_avgDwell,
                        s_sessionTasks,
                        s_sessionTime])
        return X_t, user_id, new_session


# Predicting
    def predicting(self,user_id, created_at, X_test):

        y_predicted= self.clf.predict(X_test)
        if y_predicted == 1:
            self.y_leaving+=1
         #   app_log.info("Prediction for User " + user_id + ": U S E R   L E A V I N G!!!!!")
        else:
            self.y_staying+=1
          #  app_log.info("Prediction for User " + user_id + ": U S E R   STAYING ")
        return y_predicted

    def predict_prob(self,user_id, created_at):

        X_test, new_session=self.fe(user_id,created_at)
        y_predicted_proba= self.clf.predict_proba(X_test)[0,1]
        return y_predicted_proba

    def disratio(self):
        return self.y_leaving/(self.y_leaving+self.y_staying),self.y_staying/(self.y_leaving+self.y_staying)

    # Allocate cohort to new user - currently round robin policy, for 7 cohorts
    def get_next_cohort(self):
        self.cohort_number+=1
        if (self.cohort_number > 7):
            self.cohort_number = 1
        return self.cohort_number

    # update latest intervention
    def update_int_session(self,user_id,session):
        self.user_intervention_session_dict[user_id] = session
        with closing(self.db1.cursor()) as cur1:
            status = cur1.execute(self.replace_user_intervention_session, (user_id,session))
        self.db1.commit()


    # Persistant Prediction - Intervene
    # return values: intervention action, cohort id
    # intervention action: 0 - no intervention; 1 - intervention message 1; 2 - intervention message 2; 3 - intervention message 3
    def intervene(self,user_sid, created_at):
        """
        cohorts:
        1 - no intervention
        2 - random intervention  at beginning of session, message 1
        3 - random intervention  at beginning of session, message 2
        4 - random intervention  at beginning of session, message 3
        5 - intelligent intervention  once in session, message 1
        6 - intelligent intervention  once in session, message 2
        7 - intelligent intervention  once in session, message 3
        """

        X_test, user_id, new_session = self.fe(user_sid,created_at)

        # if user not allocated to cohort, do it now and update db

        if user_id not in self.user_cohorts_dict:
            user_cohort = self.get_next_cohort()
            self.user_cohorts_dict[user_id] = user_cohort
            with closing(self.db1.cursor()) as cur1:
                status = cur1.execute(self.insert_user_cohort,(user_id,user_sid, user_cohort))
            self.db1.commit()

        user_cohort = self.user_cohorts_dict[user_id]

        # check if intervention already given in this session
        last_intervention_session = -1;
        if user_id in self.user_intervention_session_dict:
            last_intervention_session = self.user_intervention_session_dict[user_id]
        current_session = self.user_past_session_stats[user_id][0]

        y_p = self.predicting(user_sid, created_at, X_test)

        if ((last_intervention_session != current_session) and (y_p == 1)):
            # user leaving and no intervention yet in this session
            new_intervention = True
        else:
            new_intervention = False

        r = random.random()

        if (user_cohort == 1):
            return 0,user_cohort,y_p

        elif (user_cohort == 2):
            if new_session and (r<self.RANDINT_THRESHOLD):
                self.update_int_session(user_id, current_session)
                return 1, user_cohort,y_p
            else:
                return 0, user_cohort,y_p

        elif (user_cohort == 3):
            if new_session and (r<self.RANDINT_THRESHOLD):
                self.update_int_session(user_id, current_session)
                return 2, user_cohort,y_p
            else:
                return 0, user_cohort,y_p

        elif (user_cohort == 4):
            if new_session and (r<self.RANDINT_THRESHOLD):
                self.update_int_session(user_id, current_session)
                return 3, user_cohort,y_p
            else:
                return 0, user_cohort,y_p

        elif (user_cohort == 5):
            if new_intervention:
                self.update_int_session(user_id, current_session)
                return 1, user_cohort,y_p
            else:
                return 0, user_cohort,y_p

        elif (user_cohort == 6):
            if new_intervention:
                self.update_int_session(user_id, current_session)
                return 2, user_cohort,y_p
            else:
                return 0, user_cohort,y_p

        elif (user_cohort == 7):
            if new_intervention:
                self.update_int_session(user_id, current_session)
                return 3, user_cohort,y_p
            else:
                return 0, user_cohort,y_p

def main():
        pred=dis_predictor()
        file_raw = open("/Users/avisegal/temp/ngzLast1M.csv")
        first_line = file_raw.readline()
        counter=0;
        for line in file_raw:
            fields = line.strip().split(',')
            user_sid=fields[0]
            created_at=fields[1]
            message, cohort, prediction = pred.intervene(user_sid, created_at)
         #   app_log.info("Intervention for User " + user_sid + ":   Message: " + str(message) + "   Cohort: " + str(cohort))

#            y_pred=pred.predicting(user_sid,created_at)
#            if y_pred == 1:
#                app_log.info("Prediction for User " + user_sid + ": U S E R   L E A V I N G!!!!!")
#            else:
#                app_log.info("Prediction for User " + user_sid + ": U S E R   STAYING ")

            counter+=1
            if (counter>=1000):
                l,s=pred.disratio()
                #logging.info ("LEAVING/STAYING R A T I O: " + str(l)+'/'+str(s))
                counter=0;
        l,s=pred.disratio()
        #logging.info ("LEAVING/STAYING R A T I O: " + str(l)+'/'+str(s))

if __name__=="__main__":
       main()

#truncate ngz.user_cohorts; truncate ngz.user_interventions; truncate ngz.user_fe_stats; truncate ngz.user_session_fe_stats
