import pandas as pd
import numpy as np
import gzip
import re
import dateutil.parser

class DataAnalysis(object):
    def __init__(self,gzip_path,session_period):
        '''
        purpose : read data from gzip and transform data into dataframe
        param gzip_path      : path of data
        param session_period : define total time for a session (unit: second)
        '''
        # a list of all column name
        self.column_list=[
        'timestamp',
        'elb',
        'client:port',
        'backend:port',
        'request_processing_time',
        'backend_processing_time',
        'response_processing_time',
        'elb_status_code',
        'backend_status_code',
        'received_bytes',
        'sent_bytes',
        'request',
        'user_agent',
        'ssl_cipher',
        'ssl_protocol']
        self.session_period=session_period
        self.content_list=[]
        with gzip.open(gzip_path, 'rb') as f:
            for row in f:

                variable_row=re.split((" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"),row.strip().decode())

                #string split succefully
                if len(variable_row)==len(self.column_list):
                    pass
                # double quotation in request dealing       
                else:
                    request=re.search("GET.*HTTP/[0-9]+.[0-9]+",row.strip().decode())[0]
                    row_update=re.sub('"GET.*HTTP/[0-9]+.[0-9]+" ','',row.strip().decode())
                    variable_row=re.split((" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"),row_update)
                    variable_row.insert(self.column_list.index('request'),request)
                self.content_list.append(variable_row)        
        self.log_dataframe=pd.DataFrame(self.content_list,columns=self.column_list)
        # format 'timestamp' from str into timestamp
        self.log_dataframe['timestamp']=self.log_dataframe['timestamp'].apply(lambda x:dateutil.parser.parse(x))
    def SessionAnalysis(self):
        '''
        Q1      : session analysis
        purpose : aggregrate all page hits by visitor/IP during a defined session
        '''
        self.result_dict={}
        # select and sort by client:port,timestamp and tansform dataframe into list
        for index,item in enumerate(self.log_dataframe[['client:port','timestamp']].sort_values(by=['client:port','timestamp']).values.tolist()):
            if item[0] not in self.result_dict.keys():
                self.result_dict[item[0]]=[[item[1]]]
            else:
                if (item[1]-self.result_dict[item[0]][-1][0]).total_seconds()<self.session_period:            
                    self.result_dict[item[0]][-1].append(item[1])
                else:
                    self.result_dict[item[0]].append([item[1]])
        
        #total clicks for all sessions
        sum_all_session=sum([len(session) for item in self.result_dict.values() for session in item])
        # total session numbers
        session_num=len([len(session) for item in self.result_dict.values() for session in item])
        
        # average click times per session
        return sum_all_session/session_num

    def SessionAverageTime(self):
        '''
        Q2      : average session time
        purpose : aggregrate average session time during a session period
        '''
        self.average_time_dict={}
        for key in self.result_dict.keys():
            
            # record time period between the first and the last request
            for session in self.result_dict[key]:
            
                #ignore only  one click during a session
                if len(session) ==1:
                    pass
                
                elif key not in self.average_time_dict.keys():
                    self.average_time_dict[key]=[(session[-1]-session[0]).total_seconds()]
                
                else:
                    self.average_time_dict[key].append((session[-1]-session[0]).total_seconds())
        
        
        
        # total session time
        total_session_time=sum([time for session in self.average_time_dict.values() for time in session])       
        # total session number
        total_session_numbers=len([time for session in self.average_time_dict.values() for time in session])
        
        # return average time (seconds)
        return  total_session_time / total_session_numbers

    def DistinctSessionAnalysis(self):
        '''
        Q3      : session analysis with distinct visited URL
        purpose : aggregrate all dustinct URLs hits by visitor/IP during a defined session
        '''
        self.result_dict={}
        # select and sort by client:port,timestamp and tansform dataframe into list
        for index,item in enumerate(self.log_dataframe[['client:port','timestamp','request']].sort_values(by=['client:port','timestamp']).values.tolist()):

            if item[0] not in self.result_dict.keys():
                distinct=[]
                self.result_dict[item[0]]=[[item[1]]]
                distinct.append(item[2])
            else:
                if (item[1]-self.result_dict[item[0]][-1][0]).total_seconds()<self.session_period:            
                    # only record distinct request
                    if item[2] not in distinct:
                        self.result_dict[item[0]][-1].append(item[1])
                    else:
                        pass
                else:
                    distinct=[]
                    self.result_dict[item[0]].append([item[1]])
                    distinct.append(item[2])
        
        #total clicks for all sessions
        distinct_sum_all_session=sum([len(session) for item in self.result_dict.values() for session in item])
        # total session numbers
        distinct_session_num=len([len(session) for item in self.result_dict.values() for session in item])
        
        # average click times per session
        return distinct_sum_all_session / distinct_session_num
    
    def MostEngagedUsers(self):
        '''
        Q4      : most engaged users
        purpose : find out the ip with the longest within a session
        '''       
        # use session time record (SessionAverageTime) in Q2 to aggregate result
        self.engaged_time={}
        for IP in self.average_time_dict.keys():
            
            if sum(self.average_time_dict[IP]) not in self.engaged_time.keys():
                self.engaged_time[sum(self.average_time_dict[IP])]=[IP]
            else:
                self.engaged_time[sum(self.average_time_dict[IP])].append(IP)

        longest_session_time=max(self.engaged_time.keys()) 
        most_engaged_users_list=self.engaged_time[longest_session_time],   
        # return the user with the longest duration of all sessions
        return most_engaged_users_list,longest_session_time  


if __name__=='__main__':

    gzip_path_str='data/2015_07_22_mktplace_shop_web_log_sample.log.gz'
    # define 1800 seconds as a session
    session_period_int=1800
    data=DataAnalysis(gzip_path=gzip_path_str,session_period=session_period_int)

    # Q1 average clicks per session (defined session as 1800 seconds)
    resultQ1=data.SessionAnalysis()
    print('average clicks per session(%s seconds): '%session_period_int,resultQ1)

    # Q2 average clicks per session (defined session as 1800 seconds)
    resultQ2=data.SessionAverageTime()
    print('average session time(seconds) per session(%s seconds): '%session_period_int,resultQ2)

    # Q3 average distinct urls visited per session (defined session as 1800 seconds)
    resultQ3=data.DistinctSessionAnalysis()
    print('unique URL visits per session(%s seconds): '%session_period_int,resultQ3)
    # Q4 most engaged users per session (defined session as 1800 seconds)
    resultQ4,overall_duration_time=data.MostEngagedUsers()
    if len(resultQ4)==1:
        print('With the longest overall session time at %s seconds, the most engaged user is: '%overall_duration_time,resultQ4[0])
    else:
        print('With the longest overall session time at %s seconds, the most engaged users are: '%overall_duration_time,resultQ4)
