
#!/usr/bin/env python
# coding: utf-8

##########################################################################################################
#Library list
import os
import pyodbc
import time
import pathlib
from pathlib import Path
import importlib
import importlib.util
import pandas as pd
import numpy as np

#################################################################################################################################
# importing log to database class from the determined location
drive = Path(__file__).drive
logger_module = ''

if os.name == 'nt':
    ConnectorSpecs = importlib.util.spec_from_file_location("Connector.py", os.path.expandvars("{}\\Users\\$USERNAME\\Desktop\\Python-Scripts\\Vivint\\SQL\\Connector.py".format(drive)))
else:
    ConnectorSpecs = importlib.util.spec_from_file_location("Connector.py", os.path.expandvars("{}$HOME/Desktop/Python-Scripts/Vivint/SQL/Connector.py".format(drive)))

Connector = importlib.util.module_from_spec(ConnectorSpecs)
ConnectorSpecs.loader.exec_module(Connector)

sqlcon = Connector.connection.sqlConnection('vivint')

select = """
    select  distinct webstation_id, aid from [vivint].[dbo].[vivint_roster_schedule]
"""
cursor1 = sqlcon.cursor()
cursor1.execute(select)
usersTable = cursor1.fetchall()
cursor1.close()
usersDict = {user[0]:user[1] for user in usersTable}

def get_sec(time_str):
    splitted = str(time_str).split(':')
    if len(splitted) == 3:
        h, m, s = str(time_str).split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

    elif len(splitted) == 2:
        h, m = str(time_str).split(':')
        return int(h) * 3600 + int(m) * 60

def date_checker(date):
    if "Date:" in str(date):
        return date.replace("Date: ","")
    elif "Total for" in str(date):
        return date
   
def agent_checker(agent):
    if "Agent:" in str(agent):
        return agent.replace("Agent: ","")
class WebstationUploader():
    def __init__(self, logger):
        self.logger_module = logger
        global logger_module
        logger_module = logger
        global usersDict
        self.usersDict = usersDict
        global sqlcon
        self.sqlcon = sqlcon

    def insert_Auxes(self,vals):
        logger_module.pyLogger('running',msg='Running insert Time Utilization function')

        query = "BEGIN TRAN \
            UPDATE vivint_agent_activity WITH (serializable) set [agent_id]=?,[agent_name]=?,[mu]=?,[date]=?,[aux]=?, \
                [duration]=?,[percent]=?,[aid]=? \
            WHERE uid=? \
            IF @@rowcount = 0 \
            BEGIN \
                INSERT INTO vivint_agent_activity ([agent_id], [agent_name], [mu], [date], [aux], [duration], [percent], [aid] \
                )  \
                VALUES(?,?,?,?,?,?,?,?)\
            END\
            COMMIT TRAN"
        
        try:
            conn = self.sqlcon
            cursor = conn.cursor()
            logger_module.pyLogger('running',msg='Uploading data')
            # cursor.fast_executemany = True
            cursor.executemany(query, vals)

            conn.commit()
            print("Rows Inserted in Vivint Agent Activity: ",len(vals))
            logger_module.pyLogger('running',msg="Rows Inserted in Vivint Agent Activity: {}".format(len(vals)))
        except pyodbc.Error as error:
            print(error)
            logger_module.pyLogger('error',msg='Error uploading data: '+str(error))
            pass
        finally:
            cursor.close()
    
    def delete_AuxesSchedules(self,vals):
        logger_module.pyLogger('running',msg='Running delete Time Utilization Schedules function')

        query = "DELETE \
            FROM [vivint].[dbo].[vivint_agent_activity_schedules] \
            where [date] = ? "

        try:
            conn = self.sqlcon
            cursor = conn.cursor()
            logger_module.pyLogger('running',msg='Uploading data')
            # cursor.fast_executemany = True
            cursor.executemany(query, vals)

            conn.commit()
            print("Rows Deleted in Vivint Agent Activity Schedules: ",cursor.rowcount)
            logger_module.pyLogger('running',msg="Rows Deleted in Vivint Agent Activity Schedules: {}".format(cursor.rowcount))
        except pyodbc.Error as error:
            print(error)
            logger_module.pyLogger('error',msg='Error uploading data: '+str(error))
            pass
        finally:
            cursor.close()


    def insert_AuxesSchedules(self,vals):
        logger_module.pyLogger('running',msg='Running insert Time Utilization Schedules function')

        query = "BEGIN TRAN \
            UPDATE vivint_agent_activity_schedules WITH (serializable) set [agent_id]=?,[agent_name]=?,[mu]=?,[date]=?,[aux]=?, \
                [duration]=?,[percent]=?,[aid]=? \
            WHERE uid=? \
            IF @@rowcount = 0 \
            BEGIN \
                INSERT INTO vivint_agent_activity_schedules ([agent_id], [agent_name], [mu], [date], [aux], [duration], [percent], [aid] \
                )  \
                VALUES(?,?,?,?,?,?,?,?)\
            END\
            COMMIT TRAN"
        

        try:
            conn = self.sqlcon
            cursor = conn.cursor()
            logger_module.pyLogger('running',msg='Uploading data')
            # cursor.fast_executemany = True
            cursor.executemany(query, vals)

            conn.commit()
            print("Rows Inserted in Vivint Agent Activity Schedules: ",len(vals))
            logger_module.pyLogger('running',msg="Rows Inserted in Vivint Agent Activity Schedules: {}".format(len(vals)))
        except pyodbc.Error as error:
            print(error)
            logger_module.pyLogger('error',msg='Error uploading data: '+str(error))
            pass
        finally:
            cursor.close()

    def insert_schedules(self,vals):
        logger_module.pyLogger('running',msg='Running insert schedules function')

        query = "BEGIN TRAN \
            UPDATE vivint_schedules WITH (serializable) set [agent_id]=?,[agent_name]=?,[mu]=?,[date]=?, \
                [shift_start]=?,[shift_end]=?,[scheduled_activity]=?,[activity_start]=?,[activity_end]=?,[aid]=?\
            WHERE uid=? \
            IF @@rowcount = 0 \
            BEGIN \
                INSERT INTO vivint_schedules ([agent_id],[agent_name],[mu],[date],[shift_start],\
                    [shift_end],[scheduled_activity],[activity_start],[activity_end],[aid] \
                )  \
                VALUES(?,?,?,?,?,?,?,?,?,?)\
            END\
            COMMIT TRAN"

        try:
            conn = self.sqlcon
            cursor = conn.cursor()
            logger_module.pyLogger('running',msg='Uploading data')
            # cursor.fast_executemany = True
            cursor.executemany(query, vals)

            conn.commit()
            print("Rows Inserted in Vivint Schedules: ",len(vals))
            logger_module.pyLogger('running',msg="Rows Inserted in Vivint Schedules: {}".format(len(vals)))
        except pyodbc.Error as error:
            print(error)
            logger_module.pyLogger('error',msg='Error uploading data: '+str(error))
            pass
        finally:
            cursor.close()

    def insert_queues(self,vals):
        logger_module.pyLogger('running',msg='Running insert queues function')

        query = "BEGIN TRAN \
            UPDATE vivint_queue_utilization WITH (serializable) set [mu]=?,[date]=?,[queue_name]=?,[inbound_contacts]=?,[talk]=?,[work]=?, \
                [total]=?,[avg_talk_time]=?,[avg_work_time]=?,[avg_handle_time]=?,[outbound_time]=?,[system_time]=? \
            WHERE uid=? \
            IF @@rowcount = 0 \
            BEGIN \
                INSERT INTO vivint_queue_utilization ([mu],[date],[queue_name],[inbound_contacts],[talk],[work],[total],[avg_talk_time], \
                    [avg_work_time],[avg_handle_time],[outbound_time],[system_time]) \
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)\
            END\
            COMMIT TRAN"

        try:
            conn = self.sqlcon
            cursor = conn.cursor()
            logger_module.pyLogger('running',msg='Uploading data')
            # cursor.fast_executemany = True
            cursor.executemany(query, vals)

            conn.commit()
            print("Rows Inserted in Vivint Queue Utilization: ",len(vals))
            logger_module.pyLogger('running',msg="Rows Inserted in Vivint Queue Utilization: {}".format(len(vals)))
        except pyodbc.Error as error:
            print(error)
            logger_module.pyLogger('error',msg='Error uploading data: '+str(error))
            pass
        finally:
            cursor.close()

    def insert_adherence(self,vals):
        logger_module.pyLogger('running',msg='Running insert adherence function')

        query = "BEGIN TRAN \
            UPDATE vivint_adherence WITH (serializable) set [agent_id]=?,[agent_name]=?,[mu]=?,[scheduled_activities]=?,[date]=?,[scheduled_time]=?,\
                [actual_time]=?,[min_in_adherence]=?,[min_out_adherence]=?,[percent_in_adherence]=?,[min_in_conformance]=?,[percent_in_conformance]=?,\
                [percent_of_total_schedule]=?,[percent_of_total_actual]=?,[aid]=? \
            WHERE uid=? \
            IF @@rowcount = 0 \
            BEGIN \
                INSERT INTO vivint_adherence ([agent_id], [agent_name], [mu], [scheduled_activities], [date], [scheduled_time], [actual_time], \
                    [min_in_adherence], [min_out_adherence], [percent_in_adherence], [min_in_conformance], [percent_in_conformance],\
                    [percent_of_total_schedule], [percent_of_total_actual], [aid] \
                )  \
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\
            END\
            COMMIT TRAN"

        try:
            conn = self.sqlcon
            cursor = conn.cursor()
            logger_module.pyLogger('running',msg='Uploading data')
            # cursor.fast_executemany = True
            cursor.executemany(query, vals)

            conn.commit()
            print("Rows Inserted in Vivint Adherence: ",len(vals))
            logger_module.pyLogger('running',msg="Rows Inserted in Vivint Adherence: {}".format(len(vals)))
        except pyodbc.Error as error:
            print(error)
            logger_module.pyLogger('error',msg='Error uploading data: '+str(error))
            pass
        finally:
            cursor.close()

    def auxes_ParserUp(self,data):
        try:
            vals = None
            logger_module.pyLogger('running',msg='Parsing Data for Time Utilization Activity with Auxes function')
            mu = data['Unnamed: 2'].str.find("MU: ")
            muindex = mu.isin([0.0,0]).idxmax()
            # * Pandas rearranging columns and renaming
            logger_module.pyLogger('running',msg='Pandas rearranging columns and renaming')
            data['Unnamed: 4'] = data.apply(lambda row: row['Unnamed: 3'] if pd.isnull(row['Unnamed: 4'])  else row['Unnamed: 4'],axis=1)
            data['Unnamed: 0'] = str(data.iloc[muindex,2]).replace("MU: ","")
            data = data.drop(['Unnamed: 1','Unnamed: 3','Unnamed: 5','Unnamed: 6','Unnamed: 8','Unnamed: 9','Unnamed: 10','Unnamed: 12','Unnamed: 14'],axis=1)
            
            data = data.rename(columns={'Unnamed: 0':'MU','Unnamed: 2': 'Agent','Unnamed: 4':'Date','Unnamed: 7':'Aux','Unnamed: 11':'Duration','Unnamed: 13':'Percent'})
            data = data.iloc[6:-134]

            # * Pandas Filling agent and date columns
            logger_module.pyLogger('running',msg='Pandas Filling agent and date columns')
            data["Agent"] = data["Agent"].ffill(axis = 0)
            data["Agent"] = data["Agent"].bfill(axis = 0)
            data["Agent"] = data["Agent"].str.strip()
            data["Date"] = data["Date"].ffill(axis = 0)
            
            data = data[data["Agent"].str.contains("Agent:")]
            data["Agent"] = data["Agent"].str.replace('Agent: ','')
            data["Aux"] = data["Aux"].str.replace('1:1','1-1')

            data = data.dropna(subset=['Date'])
            data = data.dropna(subset=['Aux'])
            data = data[data["Aux"]!='Total']

            data = data[~data["Date"].str.contains("Total")]
            data["Date"] = pd.to_datetime(data["Date"],format="%A, %B %d, %Y")
            
            # * Pandas Cleaning and fixing column formats
            logger_module.pyLogger('running',msg='Pandas Cleaning and fixing column formats')
            data["Duration"] = data.apply(lambda row: get_sec(row["Duration"]),axis=1)
            data['Percent'] = data['Percent'].str.rstrip('%').astype(float) / 100.0

            datasplit = pd.DataFrame(data["Agent"].str.split(' ',1).tolist(), columns = ['agent_id','agent_name'])
            data = data.drop(columns=['Agent'])
            data = pd.concat([datasplit.reset_index(drop=True),data.reset_index(drop=True)],axis=1)

            # * Creating the AID List of the Agent IDs provided in the data.
            logger_module.pyLogger('running',msg='Creating the AID List of the Agent IDs provided in the data.')
            agentid = data["agent_id"].tolist()
            aidList = [usersDict[str(webid).replace(".0","")] if str(webid).replace(".0","") in usersDict else "Not in Roster" for webid in agentid]
            
            # * Creating the Unique ID List based on the specified columns of the data.
            logger_module.pyLogger('running',msg='Creating the Unique ID List based on the specified columns of the data.')
            dates = data["Date"].tolist()
            datesList = [date.strftime("%Y-%m-%d") for date in dates]
            auxList = data["Aux"].tolist()
            sep = " - "
            uidList = [str(agentid).replace(".0","") + sep + str(dates).replace(".0","") + sep + str(aux).replace(".0","")  for agentid,dates,aux in zip(agentid,datesList,auxList)]

            # * Creating dataframe on the AID List and UID List, then appending the columns to the data
            logger_module.pyLogger('running',msg='Creating dataframe on the AID List and UID List, then appending the columns to the data')
            aidDf = pd.DataFrame(aidList,columns=['aid'])
            uidDf = pd.DataFrame(uidList,columns=['uid'])
            data = pd.concat([data.reset_index(drop=True),aidDf.reset_index(drop=True)],axis=1)
            data = pd.concat([data.reset_index(drop=True),uidDf.reset_index(drop=True),data.reset_index(drop=True)],axis=1)

            vals = list(data.itertuples(index=False, name=None))
            return vals

        except Exception as e:
            print("Error Parsing Auxes Data: ",e)
            self.logger_module.pyLogger('error',msg='Error Parsing Auxes Data. Error is {}'.format(e))
            pass

        finally:
            if vals == None:
                return vals

    def auxesSchedules_ParserUp(self,data):
        try:
            vals = None
            mu = data['Unnamed: 2'].str.find("MU: ")
            muindex = mu.isin([0.0,0]).idxmax()
            # * Pandas rearranging columns and renaming
            logger_module.pyLogger('running',msg='Pandas rearranging columns and renaming')
            data['Unnamed: 4'] = data.apply(lambda row: row['Unnamed: 3'] if pd.isnull(row['Unnamed: 4'])  else row['Unnamed: 4'],axis=1)
            data['Unnamed: 0'] = str(data.iloc[muindex,2]).replace("MU: ","")
            data = data.drop(['Unnamed: 1','Unnamed: 3','Unnamed: 5','Unnamed: 6','Unnamed: 8','Unnamed: 9','Unnamed: 10','Unnamed: 12','Unnamed: 14'],axis=1)
            
            data = data.rename(columns={'Unnamed: 0':'MU','Unnamed: 2': 'Agent','Unnamed: 4':'Date','Unnamed: 7':'Aux','Unnamed: 11':'Duration','Unnamed: 13':'Percent'})
            data = data.iloc[6:-134]

            # * Pandas Filling agent and date columns
            logger_module.pyLogger('running',msg='Pandas Filling agent and date columns')
            # data.to_csv('test.csv')
            data["Agent"] = data["Agent"].ffill(axis = 0)
            data["Agent"] = data["Agent"].bfill(axis = 0)
            data["Agent"] = data["Agent"].str.strip()
            data["Date"] = data["Date"].ffill(axis = 0)
            
            data = data[data["Agent"].str.contains("Agent:")]
            data["Agent"] = data["Agent"].str.replace('Agent: ','')
            data["Aux"] = data["Aux"].str.replace('1:1','1-1')

            data = data.dropna(subset=['Date'])
            data = data.dropna(subset=['Aux'])
            data = data[data["Aux"]!='Total']

            data = data[~data["Date"].str.contains("Total")]
            data["Date"] = pd.to_datetime(data["Date"],format="%A, %B %d, %Y")
            
            uniqueDate = data["Date"].unique()

            # * Pandas Cleaning and fixing column formats
            logger_module.pyLogger('running',msg='Pandas Cleaning and fixing column formats')
            data["Duration"] = data.apply(lambda row: get_sec(row["Duration"]),axis=1)
            data['Percent'] = data['Percent'].str.rstrip('%').astype(float) / 100.0

            datasplit = pd.DataFrame(data["Agent"].str.split(' ',1).tolist(), columns = ['agent_id','agent_name'])
            data = data.drop(columns=['Agent'])
            data = pd.concat([datasplit.reset_index(drop=True),data.reset_index(drop=True)],axis=1)

            # * Creating the AID List of the Agent IDs provided in the data.
            logger_module.pyLogger('running',msg='Creating the AID List of the Agent IDs provided in the data.')
            agentid = data["agent_id"].tolist()
            aidList = [usersDict[str(webid).replace(".0","")] if str(webid).replace(".0","") in usersDict else "Not in Roster" for webid in agentid]
            
            # * Creating the Unique ID List based on the specified columns of the data.
            logger_module.pyLogger('running',msg='Creating the Unique ID List based on the specified columns of the data.')
            dates = data["Date"].tolist()
            datesList = [date.strftime("%Y-%m-%d") for date in dates]
            auxList = data["Aux"].tolist()
            sep = " - "
            uidList = [str(agentid).replace(".0","") + sep + str(dates).replace(".0","") + sep + str(aux).replace(".0","")  for agentid,dates,aux in zip(agentid,datesList,auxList)]

            # * Creating dataframe on the AID List and UID List, then appending the columns to the data
            logger_module.pyLogger('running',msg='Creating dataframe on the AID List and UID List, then appending the columns to the data')
            aidDf = pd.DataFrame(aidList,columns=['aid'])
            uidDf = pd.DataFrame(uidList,columns=['uid'])
            data = pd.concat([data.reset_index(drop=True),aidDf.reset_index(drop=True)],axis=1)
            data = pd.concat([data.reset_index(drop=True),uidDf.reset_index(drop=True),data.reset_index(drop=True)],axis=1)
            # data.to_csv('test2.csv')
            vals = list(data.itertuples(index=False, name=None))
            # uniqueDate = [pd.to_datetime(date).strftime("%Y-%m-%d") for date in uniqueDate]
            # uniqueDate = list(tuple([row]) for row in uniqueDate)
            # self.delete_AuxesSchedules(uniqueDate)

            return vals

        except Exception as e:
            print("Error Parsing Auxes Schedules Data: ",e)
            self.logger_module.pyLogger('error',msg='Error on dataframe. Error is {}'.format(e))
            pass

        finally:
            if vals == None:
                return vals

    def schedules_ParserUp(self,f,data):
        try:
            vals = None
            # * Reading dataframe in excelfile
            # * Pandas rearranging columns and renaming
            logger_module.pyLogger('running',msg='Pandas rearranging columns and renaming')
            f = str(f).replace("Agent Schedules ","")
            data['Unnamed: 0'] = str(f).strip().replace(".xls","")
            data = data.iloc[6:-13]
            # data['Unnamed: 3'] = data.apply(lambda row: date_checker(row['Unnamed: 2']) if pd.isnull(row['Unnamed: 3'])  else row['Unnamed: 3'],axis=1)
            # data['Unnamed: 1'] = data.apply(lambda row: agent_checker(row['Unnamed: 2']) if pd.isnull(row['Unnamed: 1'])  else row['Unnamed: 1'],axis=1)
            
            data = data.drop(['Unnamed: 1','Unnamed: 6','Unnamed: 7','Unnamed: 10','Unnamed: 11'],axis=1)
            
            data = data.rename(columns={'Unnamed: 0':'MU','Unnamed: 2': 'Agent','Unnamed: 3':'Date','Unnamed: 4':'Shift Start','Unnamed: 5':'Shift End','Unnamed: 8':'Scheduled Activity','Unnamed: 9':'Activity Start','Unnamed: 12':'Activity End'})
            
            # * Pandas Filling agent and date columns
            logger_module.pyLogger('running',msg='Pandas Filling agent and date columns')
            # data.to_csv("webstest.csv")
            data = data[data["Date"]!="Date"]
            # data.to_csv("webstest2.csv")
            data["Agent"] = data["Agent"].ffill(axis = 0)
            data["Date"] = data["Date"].ffill(axis = 0)
            data["Shift Start"] = data["Shift Start"].ffill(axis = 0)
            data["Shift End"] = data["Shift End"].ffill(axis = 0)

            # * Pandas Cleaning and fixing column formats
            logger_module.pyLogger('running',msg='Pandas Cleaning and fixing column formats')

            data = data.dropna(subset=['Activity Start'])

            data["Agent"] = data["Agent"].str.replace('Agent: ','')

            data["Date"] = pd.to_datetime(data["Date"],format="%m/%d/%y")

            data["Shift Start"] = pd.to_datetime(data["Shift Start"],format="%I:%M %p").dt.strftime('%H:%M:%S')
            data["Shift End"] = pd.to_datetime(data["Shift End"],format="%I:%M %p").dt.strftime('%H:%M:%S')
            data["Activity Start"] = pd.to_datetime(data["Activity Start"],format="%I:%M %p").dt.strftime('%H:%M:%S')
            data["Activity End"] = pd.to_datetime(data["Activity End"],format="%I:%M %p").dt.strftime('%H:%M:%S')
            datasplit = pd.DataFrame(data["Agent"].str.split(' ',1).tolist(), columns = ['agent_id','agent_name'])
            data = data.drop(columns=['Agent'])
            
            data = pd.concat([datasplit.reset_index(drop=True),data.reset_index(drop=True)],axis=1)

            # * Creating the AID List of the Agent IDs provided in the data.
            logger_module.pyLogger('running',msg='Creating the AID List of the Agent IDs provided in the data.')
            agentid = data["agent_id"].tolist()
            aidList = [usersDict[str(webid).replace(".0","")] if str(webid).replace(".0","") in usersDict else "Not in Roster" for webid in agentid]
            
            # * Creating the Unique ID List based on the specified columns of the data.
            logger_module.pyLogger('running',msg='Creating the Unique ID List based on the specified columns of the data.')
            dates = data["Date"].tolist()
            datesList = [date.strftime("%Y-%m-%d") for date in dates]
            auxList = data["Scheduled Activity"].tolist()
            startList = data["Activity Start"].tolist()
            endList = data["Activity End"].tolist()
            sep = " - "
            uidList = [str(agentid).replace(".0","") + sep + str(dates).replace(".0","") + sep + str(aux).replace(".0","") + sep + str(start).replace(".0","") + sep + str(end).replace(".0","")  for agentid,dates,aux,start,end in zip(agentid,datesList,auxList,startList,endList)]

            # * Creating dataframe on the AID List and UID List, then appending the columns to the data
            logger_module.pyLogger('running',msg='Creating dataframe on the AID List and UID List, then appending the columns to the data')
            aidDf = pd.DataFrame(aidList,columns=['aid'])
            uidDf = pd.DataFrame(uidList,columns=['uid'])
            data = pd.concat([data.reset_index(drop=True),aidDf.reset_index(drop=True)],axis=1)
            data = pd.concat([data.reset_index(drop=True),uidDf.reset_index(drop=True),data.reset_index(drop=True)],axis=1)
            
            vals = list(data.itertuples(index=False, name=None))
            return vals

        except Exception as e:
            print("Error Parsing schedules Data: ",e)
            self.logger_module.pyLogger('error',msg='Error Parsing schedules Data. Error is {}'.format(e))
            pass

        finally:
            if vals == None:
                return vals

    def queues_ParserUp(self,data):
        try:
            vals = None
            # * Pandas rearranging columns and renaming
            logger_module.pyLogger('running',msg='Pandas rearranging columns and renaming')
            mu = data['Unnamed: 2'].str.find("MU: ")
            muindex = mu.isin([0.0,0]).idxmax()
            mu = str(data.iloc[muindex,2]).replace("MU: ","")
            data['Unnamed: 0'] = mu
            data['Unnamed: 4'] = data.apply(lambda row: row['Unnamed: 3'] if pd.isnull(row['Unnamed: 4'])  else row['Unnamed: 4'],axis=1)
            data['Unnamed: 3'] = data.apply(lambda row: row['Unnamed: 2'] if pd.isnull(row['Unnamed: 3'])  else row['Unnamed: 3'],axis=1)
            
            data = data.drop(['Unnamed: 1','Unnamed: 2','Unnamed: 10','Unnamed: 12'],axis=1)
            
            data = data.rename(columns={'Unnamed: 0':'MU','Unnamed: 3': 'Date','Unnamed: 4':'Queue_Name','Unnamed: 5':'Inbound contacts','Unnamed: 6':'Talk','Unnamed: 7':'Work','Unnamed: 8':'Total','Unnamed: 9':'Avg talk time','Unnamed: 11':'Avg work time','Unnamed: 13':'Avg handle time','Unnamed: 14':'Outbound time','Unnamed: 15':'System time'})
            data = data.iloc[10:-12]
            
            # * Pandas Filling agent and date columns
            logger_module.pyLogger('running',msg='Pandas Filling agent and date columns')

            # data = data.dropna(subset=['Date'])
            data = data.dropna(subset=['Queue_Name'])
            data["Date"] = data["Date"].ffill(axis = 0)
            data = data[~((data["Date"].str.contains("MU:"))|(data["Date"]=='Totals'))]
            data = data[(data["Queue_Name"].str.contains("Q"))|(data["Queue_Name"]=='Total')]
            
            # * Pandas Cleaning and fixing column formats
            logger_module.pyLogger('running',msg='Pandas Cleaning and fixing column formats')
            data["Date"] = pd.to_datetime(data["Date"],format="%m/%d/%y")
            data["Talk"] = data.apply(lambda row: get_sec(row["Talk"]),axis=1)
            data["Work"] = data.apply(lambda row: get_sec(row["Work"]),axis=1)
            data["Total"] = data.apply(lambda row: get_sec(row["Total"]),axis=1)
            data["Outbound time"] = data.apply(lambda row: get_sec(row["Outbound time"]),axis=1)
            data["System time"] = data.apply(lambda row: get_sec(row["System time"]),axis=1)

            data["Avg talk time"] = data["Avg talk time"].str.replace(',','')
            data["Avg work time"] = data["Avg work time"].str.replace(',','')
            data["Avg handle time"] = data["Avg handle time"].str.replace(',','')

            # * Creating the Unique ID List based on the specified columns of the data.
            logger_module.pyLogger('running',msg='Creating the Unique ID List based on the specified columns of the data.')
            dates = data["Date"].tolist()
            datesList = [date.strftime("%Y-%m-%d") for date in dates]
            queuesList = data["Queue_Name"].tolist()
            sep = " - "
            uidList = [ mu + sep + str(dates).replace(".0","") + sep + str(queues).replace(".0","")  for dates,queues in zip(datesList,queuesList)]

            # * Creating dataframe on the AID List and UID List, then appending the columns to the data
            logger_module.pyLogger('running',msg='Creating dataframe on the  UID List, then appending the columns to the data')

            uidDf = pd.DataFrame(uidList,columns=['uid'])
            data = pd.concat([data.reset_index(drop=True),uidDf.reset_index(drop=True),data.reset_index(drop=True)],axis=1)
            data = data.replace({np.nan: None})
            
            vals = list(data.itertuples(index=False, name=None))
            return vals

        except Exception as e:
            print("Error Parsing Queues Utilization Data: ",e)
            self.logger_module.pyLogger('error',msg='Error Parsing Queues Utilization Data. Error is {}'.format(e))

        finally:
            if vals == None:
                return vals
   
    def adherence_ParserUp(self,f,data):
        try:
            vals = None
            # * Pandas rearranging columns and renaming
            logger_module.pyLogger('running',msg='Pandas rearranging columns and renaming')
            f = str(f).replace("Adherence ","")
            data['Unnamed: 0'] = str(f).strip().replace(".xls","")
            data['Unnamed: 1'] = data['Unnamed: 1'].replace([' Adherence'],None)
            data['Unnamed: 3'] = data.apply(lambda row: date_checker(row['Unnamed: 2']) if pd.isnull(row['Unnamed: 3'])  else row['Unnamed: 3'],axis=1)
            data['Unnamed: 1'] = data.apply(lambda row: agent_checker(row['Unnamed: 2']) if pd.isnull(row['Unnamed: 1'])  else row['Unnamed: 1'],axis=1)
            
            data = data.drop(['Unnamed: 4','Unnamed: 6','Unnamed: 8','Unnamed: 10','Unnamed: 12','Unnamed: 15','Unnamed: 16','Unnamed: 18','Unnamed: 19'],axis=1)
            data = data.rename(columns={'Unnamed: 0':'MU','Unnamed: 1': 'Agent','Unnamed: 2':'Scheduled Activities','Unnamed: 3':'Date','Unnamed: 5':'Scheduled Time','Unnamed: 7':'Actual Time','Unnamed: 9':'Min in Adherence','Unnamed: 11':'Min out Adherence','Unnamed: 13':'Percent in Adherence','Unnamed: 14':'Min in Conformance','Unnamed: 17':'Percent in Conformance','Unnamed: 20':'Percent of Total Schedule','Unnamed: 21':'Percent of Total Actual'})

            # * Pandas Filling agent and date columns
            logger_module.pyLogger('running',msg='Pandas Filling agent and date columns')
            data["Agent"] = data["Agent"].ffill(axis = 0)
            data["Date"] = data["Date"].ffill(axis = 0)
            
            data = data.iloc[10:-5]
            data = data[~data["Date"].str.contains("Total for")]
            data = data.dropna(subset=['Date'])
            data = data.dropna(subset=['Scheduled Activities'])
            data = data[data["Scheduled Activities"]!='Total']
            data = data[data["Scheduled Activities"]!='Scheduled Activities']

            data = data[~data["Scheduled Activities"].str.contains("Date:")]
            data["Date"] = pd.to_datetime(data["Date"],format="%m/%d/%y")
            
            # * Pandas Cleaning and fixing column formats
            logger_module.pyLogger('running',msg='Pandas Cleaning and fixing column formats')
            
            data["Scheduled Time"] = data.apply(lambda row: get_sec(row["Scheduled Time"]),axis=1)
            data["Actual Time"] = data.apply(lambda row: get_sec(row["Actual Time"]),axis=1)
            
            data['Percent in Adherence'] = data['Percent in Adherence'].str.replace(",","")
            data['Percent in Conformance'] = data['Percent in Conformance'].str.replace(",","")
            data['Percent of Total Schedule'] = data['Percent of Total Schedule'].str.replace(",","")
            data['Percent of Total Actual'] = data['Percent of Total Actual'].str.replace(",","")

            data['Percent in Adherence'] = data['Percent in Adherence'].str.rstrip('%').astype(float) / 100.0
            data['Percent in Conformance'] = data['Percent in Conformance'].str.rstrip('%').astype(float) / 100.0
            data['Percent of Total Schedule'] = data['Percent of Total Schedule'].str.rstrip('%').astype(float) / 100.0
            data['Percent of Total Actual'] = data['Percent of Total Actual'].str.rstrip('%').astype(float) / 100.0

            datasplit = pd.DataFrame(data["Agent"].str.split(' ',1).tolist(), columns = ['agent_id','agent_name'])
            data = data.drop(columns=['Agent'])
            data = pd.concat([datasplit.reset_index(drop=True),data.reset_index(drop=True)],axis=1)

            # * Creating the AID List of the Agent IDs provided in the data.
            logger_module.pyLogger('running',msg='Creating the AID List of the Agent IDs provided in the data.')
            agentid = data["agent_id"].tolist()
            aidList = [usersDict[str(webid).replace(".0","")] if str(webid).replace(".0","") in usersDict else "Not in Roster" for webid in agentid]
            
            # * Creating the Unique ID List based on the specified columns of the data.
            logger_module.pyLogger('running',msg='Creating the Unique ID List based on the specified columns of the data.')
            dates = data["Date"].tolist()
            datesList = [date.strftime("%Y-%m-%d") for date in dates]
            auxList = data["Scheduled Activities"].tolist()
            startList = data["Scheduled Time"].tolist()
            endList = data["Actual Time"].tolist()
            sep = " - "
            uidList = [str(agentid).replace(".0","") + sep + str(dates).replace(".0","") + sep + str(aux).replace(".0","") + sep + str(start).replace(".0","") + sep + str(end).replace(".0","")  for agentid,dates,aux,start,end in zip(agentid,datesList,auxList,startList,endList)]

            # * Creating dataframe on the AID List and UID List, then appending the columns to the data
            logger_module.pyLogger('running',msg='Creating dataframe on the AID List and UID List, then appending the columns to the data')
            aidDf = pd.DataFrame(aidList,columns=['aid'])
            uidDf = pd.DataFrame(uidList,columns=['uid'])
            data = pd.concat([data.reset_index(drop=True),aidDf.reset_index(drop=True)],axis=1)
            data = pd.concat([data.reset_index(drop=True),uidDf.reset_index(drop=True),data.reset_index(drop=True)],axis=1)

            vals = list(data.itertuples(index=False, name=None))
            return vals

        except Exception as e:
            print("Error Parsing Adherence Data: ",e)
            self.logger_module.pyLogger('error',msg='Error Parsing Adherence Data. Error is {}'.format(e))
            pass

        finally:
            if vals == None:
                return vals
   