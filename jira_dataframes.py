# -*- coding: utf-8 -*-
"""
jira_dataframes

Created on Mon Sep 30 17:45:52 2019

@author: David Allyn

This script works with the Jira python library to retrieve issue information from
Jira for a specific project, convert it to related, normalized Pandas dataframes, 
and save as .csv files to be consumed with other reporting tools.

You can find documentation (including installation) at:
https://jira.readthedocs.io/en/master/index.html

This the jira_project_dataframes class transferres jira data to dataframes
and saves them as a relational database so that reporting tools can consume
them.

Instantiate the class by providing the server, the user, the authentication
as either a password or API token, the project key, and the full path to the
folder where the relational data will be stored.

All files will relate using the "issue_key" field.

instantiation keyword arguments:
    server: the Jira server to connect to
    user: the email address or user name to be authorized
    auth: the password or API token (Atlassian Cloud) to authorize
    project_key: the key of the project to get data from
    data_directory_path: the path to the directory to store the issue data files
"""
from jira import JIRA
import pyIS.IS_ToolBox as ist
import dateutil.parser as dtup
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import os

class jira_project_dataframes:
    
    def __init__(self,server,user,auth,project_key,data_directory_path):
        
        self.project_key = project_key
        
        # settings dataframe should probably be abstracted to a setting file
        # since issue data parts rarely change, we're building for "in-code" visibility

        self.data_part_settings = pd.DataFrame({
                'data_part':['issues','issue_components','issue_labels','issue_stakeholders','issue_worklogs','issue_comments'],
                'key_name':['issue_key','issue_component_key','issue_label_key','issue_stakeholder_key','issue_worklog_id','issue_comments_id'],
                'file_name':['ISSUES.csv','ISSUES_COMPONENTS.csv','ISSUES_LABELS.csv','ISSUES_STAKEHOLDERS.csv','ISSUES_WORKLOGS.csv','ISSUES_COMMENTS.csv']
                })
        
        # placeholder for storing dataframes
        # NOTE: DATAFRAMES CAN NOT REALLY BE STORED WITHIN OTHER DATAFRAMES
        #       THIS IS WHY DATAFRAMES ARE NOT COMBINED WITH data_part_settings
        #       TO ACCESS DATAFRAMES, USE THE data_part_name like this:
        #       comments = jdf.data_part_dataframes['issue_comments']
        self.data_part_dataframes = {}
        
        self.jira_server = server
        self.jira_user = user
        self.jira_auth = auth
        
        self.jira = JIRA(options={'server':self.jira_server},basic_auth=(self.jira_user,self.jira_auth)) 
        self.issues = []
        
        self.data_directory_path = os.path.abspath(data_directory_path)
        self.make_project_data_directory()
        
    def string_to_datetime(self,x):
        """
        Returns a datetime for a given string.
        
        keyword arguments:
            x: date string
        """
        return_value = ""
        
        if x:
            return_value = dtup.parse(x)
            
        return return_value

    def make_delimit_ready(self,x):
        """
        Jira can contain a bunch of characters that will affect the delimiting
        of dataframes that are saved to .csv files such as carraige returns
        in the issue comments, description, or worklog
        
        """
        return_value = ""
        
        if x:
            x = str(x)
            x = x.replace(u"\u201c",'"') #Windows left smart double quote with simple double quote
            x = x.replace(u"\u201d",'"') #Windows right smart double quote with simple double quote
            x = x.replace(u"\u2018","'") #Windows left smart single quote with simple single quote
            x = x.replace(u"\u2019","'") #Windows right smart single quote with simple single quote
            x = x.replace(u"\u000C", "_") #Tab with underscore
            x = x.replace(u"\u000D","-") #Carraige Return with dash
            x = x.replace(u"\u000A","-") #Line Feed with dash
            
            return_value = str(x).replace(u"\u201c",'"').replace(u"\u201d",'"').replace(u"\u2018","'").replace(u"\u2019","'").replace(r"\r\n","-").replace(r"\r","---")
        
        return return_value
    
    def ifnull(self,value,null_value=None):
        """
        Returns value or null_value if an error is raise or it is "false"
        
        Note: empty strings are not NULL
        
        keyword arguments:
            value: a value to interrogate for null
            null_value: a value to substitute if the given value is null
        """
        return_value = null_value
        
        try:
            if type(value) is None:
                return_value = value
            else:
                return_value = null_value
                    
        except:
            return_value = null_value
        
        return return_value
                
    def make_project_data_directory(self):
        """
        Creates and/or Returns the data directory for the project that is 
        set in the class self.project_key
        """
        
        try:
            os.mkdir(self.data_directory_path)
        except:
            "Do Nothing"
            
        return True
                
        
    def get_data_part_element(self,data_part_name,element_name):
        """
        Returns the value of the element of a data part
        
        keyword arguments:
            data_part_name: the name of a data_part
            element_name: the name of the element to return
        """
        
        data_part = self.data_part_settings.loc[self.data_part_settings['data_part']==data_part_name,element_name]
        
        return data_part.iloc[0]
        
        
    def file_to_dataframe(self,file_path):
        """
        Retrieves a file as a dataframe. If no file is found with the given
        file_path, then returns None
        
        keyword arguments:
            file_path: the full path to the csv file to open
        """
        
        return_df = None
        
        try:
            return_df = pd.read_csv(file_path)
            
        except FileNotFoundError:
            ist.msg("file_to_dataframe(): data file was not found: '" + file_path + "'",True)
            return_df = None

        return return_df
        
    def get_file_path_for_data_part(self,data_part_name):
        """
        Retrieves the file name from the data_file_name dictionary for a
        given data part name
        
        keyword arguments:
            data_part_name: the name of an issue data part defined in the data_part_settings dataframe
        """
        
        return_value = ""
        
        try:
            file_name = self.project_key + "_" + self.get_data_part_element(data_part_name,'file_name')
            file_path = os.path.abspath(self.data_directory_path + "/" + file_name)
            return_value = file_path
        except:
            return_value = ""
        
        #ist.msg("get_file_path_for_data_part(): file_path is '" + return_value + "'",True)
        return return_value
        
    def get_issues_file_mod_date(self):
        """
        Returns the modified date from the ISSUES.csv file using 
        
        If file is not found, returns an empty string
        """
    
        file_path = self.get_file_path_for_data_part('issues')
        
        date_string = ''
        
        if os.path.isfile(file_path):
            
            timestamp_modified_on = os.path.getmtime(file_path)
            date_modified_on = dt.fromtimestamp(timestamp_modified_on)
            date_modified_on = date_modified_on + timedelta(days=-1)
            date_string = dt.strftime(date_modified_on,"%Y-%m-%d")
            
        return date_string


    def retrieve_issues(self,jql,max_results=0):
        """
        Retrives all issues for a jira query language query string up to a 
        specified number of results
        
        keyword arguments:
            jql: a jira query language script to pass to Jira
            max_results (default 0): the maximum results to return; 0 is unlimited
        """
        issues = self.jira.search_issues(jql_str=jql,maxResults=max_results)
        self.issues = issues
        
        ist.msg(str(len(issues)) + " issues retrieved",True)
        return issues


    def retrieve_latest_issues(self,min_updated_date):
        """
        Retrieves all issues that have been updated since the given minimum updated date
        
        keyword arguments:
            min_updated_date: the beginning date to consider issues to retrieve
        """
         
        min_updated_date = ist.convert_date_string(min_updated_date,'%F')
            
        jql = "(project = " + self.project_key + ") and (updatedDate >= " + min_updated_date + " or worklogDate >= " + min_updated_date + ")"
        issues = self.retrieve_issues(jql)
        return issues

    def data_part_to_dataframe(self,issues,data_part_name):
        """
        Converts a set of JIRA.issues to a dataframe based on the data_part_name
        provided.
        
        **NOTE, THESE ARE A HARD CODED SET OF COLUMNS**
        **THERE MAY BE MORE COLUMNS AVAILABLE**
        
        keyword arguments:
            issues: a list of JIRA.issue objects
            data_part_name: the data part to return as a dataframe
        """
        
        return_dataframe = pd.DataFrame()
            
        if data_part_name == 'issues':
            for issue in issues:
                i = len(return_dataframe) + 1
                return_dataframe.loc[i,'issue_key'] = issue.key
                return_dataframe.loc[i,'summary'] = issue.fields.summary
                return_dataframe.loc[i,'description'] = self.make_delimit_ready(issue.fields.description)
                return_dataframe.loc[i,'priority'] = str(issue.fields.priority)
                return_dataframe.loc[i,'issue_type'] = str(issue.fields.issuetype)
                return_dataframe.loc[i,'status'] = str(issue.fields.status.name)
                return_dataframe.loc[i,'stakeholders'] = ",".join(self.ifnull(issue.fields.customfield_10800,[""]))
            
                return_dataframe.loc[i,'create_date'] = self.string_to_datetime(issue.fields.created)
                return_dataframe.loc[i,'due_date'] = self.string_to_datetime(issue.fields.duedate)
                return_dataframe.loc[i,'last_viewed'] = self.string_to_datetime(issue.fields.lastViewed)
                return_dataframe.loc[i,'resolution_date'] = self.string_to_datetime(issue.fields.resolutiondate)
                
                
                return_dataframe.loc[i,'resolution'] = str(issue.fields.resolution)
                
                return_dataframe.loc[i,'total_seconds_spent'] = issue.fields.aggregatetimespent
                return_dataframe.loc[i,'assignee'] = str(issue.fields.assignee)
                return_dataframe.loc[i,'reporter'] = str(issue.fields.reporter)
                return_dataframe.loc[i,'components'] = ",".join([str(component) for component in issue.fields.components])
                return_dataframe.loc[i,'labels'] = ",".join([str(label) for label in issue.fields.labels])
                
        elif data_part_name == 'issue_components':
            for issue in issues:
                for component in [str(component) for component in issue.fields.components]:
                    i = len(return_dataframe) + 1
                    
                    return_dataframe.loc[i,'issue_component_key'] = i
                    return_dataframe.loc[i,'issue_key'] = issue.key
                    return_dataframe.loc[i,'component'] = self.make_delimit_ready(component)
        
        elif data_part_name == 'issue_stakeholders':
            for issue in issues:
                if issue.fields.customfield_10800:
                    for stakeholder in [str(stakeholder) for stakeholder in issue.fields.customfield_10800]:
                        
                        i = len(return_dataframe) + 1
                        
                        return_dataframe.loc[i,'issue_stakeholder_key'] = i
                        return_dataframe.loc[i,'issue_key'] = issue.key
                        return_dataframe.loc[i,'stakeholder'] = self.make_delimit_ready(stakeholder)
        
        
        elif data_part_name == 'issue_labels':
            for issue in issues:
                for label in [str(label) for label in issue.fields.labels]:
                    i = len(return_dataframe) + 1
                    
                    return_dataframe.loc[i,'issue_label_key'] = i
                    return_dataframe.loc[i,'issue_key'] = issue.key
                    return_dataframe.loc[i,'label'] = self.make_delimit_ready(label)
                
        elif data_part_name == 'issue_worklogs':
            for issue in issues:
                for worklog in self.jira.worklogs(issue):
                    i = len(return_dataframe) + 1
                    
                    return_dataframe.loc[i,'issue_worklog_id'] = i
                    return_dataframe.loc[i,'issue_key'] = issue.key
                    return_dataframe.loc[i,'author'] = worklog.author.displayName
                    return_dataframe.loc[i,'created'] = self.string_to_datetime(worklog.created)
                    return_dataframe.loc[i,'started'] = self.string_to_datetime(worklog.started)
                    return_dataframe.loc[i,'seconds_spent'] = worklog.timeSpentSeconds
                    
                    
        elif data_part_name == 'issue_comments':
            for issue in issues:
                for comment in self.jira.comments(issue):
                    i = len(return_dataframe) + 1
                    
                    ## it turns out that a comment sent to the Jira from an email
                    ## that has an error will result in no author being included
                    ## and will show as "Anonymous" in the Jira web application
                    
                    ## this causes a missing author attribute in the result of
                    ## jira.comments(issue)
                    
                    try:
                        return_dataframe.loc[i,'issue_comments_id'] = i
                        return_dataframe.loc[i,'issue_key'] = issue.key
                        return_dataframe.loc[i,'author'] = comment.author.displayName
                        return_dataframe.loc[i,'created'] = self.string_to_datetime(comment.created)
                        return_dataframe.loc[i,'body_text'] = self.make_delimit_ready(comment.body)
                    except AttributeError:
                        return_dataframe.loc[i,'issue_comments_id'] = i
                        return_dataframe.loc[i,'issue_key'] = issue.key
                        return_dataframe.loc[i,'author'] = 'Unknown'
                        return_dataframe.loc[i,'created'] = self.string_to_datetime(comment.created)
                        return_dataframe.loc[i,'body_text'] = self.make_delimit_ready(comment.body)
                    
               
        else:
            raise ValueError("The data_part_name given: '" + data_part_name + "' does not have a dataframe definition.")
        
        ist.msg(str(len(return_dataframe)) + " records converted for " + data_part_name,True)    
        return return_dataframe
        

    def update_dataframe(self,existing_df,updated_df,key_column):
        """
        Returns an given dataframe updated with values of a second given dataframe
        based on a given key. If no updated_df is given or there are 0 records
        in updated_df, then the existing_df is returned.
        
        keyword arguments:
            existing_df: a dataframe that contains the existing information
            updated_df: a dataframe that contains the updated information
            key_column: the string name of the column that is the key for both
        """
        ### technique used is to concatenate (union) rows of both dataframes,
        ### then deduplicate based on the given key
        
        
        return_df = None
        
           
        existing = ist.dataframe_has_rows(existing_df)
        updated = ist.dataframe_has_rows(updated_df)
        
        # if we have an existing and updated then update the existing with updated
        if updated & existing:
            
            # get all issue keys from the updated_df
            issue_keys = list(set(updated_df['issue_key'].tolist()))
            
            # Remove any records where the issue was modified.
            existing_df = existing_df[~existing_df['issue_key'].isin(issue_keys)]
            
            #ist.msg("updated_dataframe(): updating existing dataframe with updated records",True)
            return_df = pd.concat([updated_df,existing_df],sort=False)
            
            return_df = return_df.drop_duplicates(key_column)
            return_df = return_df.sort_values(key_column)
            return_df = return_df.set_index(key_column)
            return_df = return_df.reset_index()
            
        # if we only have an updated_df, then return the updated_df
        elif (updated) & (not existing):
            ist.msg("updated_dataframe(): missing existing dataframe; returning updated dataframe",True)
            return_df = updated_df
            
        # if we only have an existing_df then return the existing_df
        elif (not updated) & (existing):
            ist.msg("updated_dataframe(): missing updated dataframe; returning existing dataframe",True)
            return_df = existing_df
            
        # we don't have an updated or existing - something's wrong
        else:
            ist.msg("update_dataframe(): there is neither an existing nor updated dataframe passed",True)
            
        # if an updated_df is None (e.g. there was no file found)
        return return_df

    def refresh_data_part(self,data_part_name, issues):
        """
        Refreshes a data part's information based on the updated_date given
        
        keyword arguments:
            data_part_given: the data part to refresh
            issues: the issues to refresh the data part with
        """
        
        # get info about the data part
        file_path = self.get_file_path_for_data_part(data_part_name)
        key_name = self.get_data_part_element(data_part_name,'key_name')
    
        #convert to dataframes and save
        updated_df = self.data_part_to_dataframe(issues,data_part_name)
        existing_df = self.file_to_dataframe(file_path)
        final_df = self.update_dataframe(existing_df,updated_df,key_name)
        
        # save final dataframe
        self.data_part_dataframes[data_part_name] = final_df
        final_df.to_csv(file_path,index=False)
        
        return final_df
        
        
    def get_project_data(self,from_date=None):
        """
        Refreshes the project data for any issues that have a modified date on 
        or after the given from_date.
               
        If no updated_date is given, the from_date will be one day before the
        modification date of the save ISSUES.csv file.
        
        If no file exists, all issues for the project will be retrieved and
        saved.
        
        keyword arguments:
            from_date (default None): the minimum issue modified date to retrieve updated project data for
        """
            
        if not self.project_key:
            raise ValueError("A project_key is required to refresh the project information. Update the project_key attribute and rerun.")
        
        
        start = dt.now()
        ist.msg("###################################################")
        ist.msg("Begin updating issues...",True)
    
        #retrieve issues from Jira based on the updatedDate given
        #if no updated_date is given, get update date from ISSUES.csv modification date

        if from_date is None:
            ist.msg("No update_date given, using file modified date instead",True)
            from_date = self.get_issues_file_mod_date()
        
        #if no file exists (i.e. updated_date = ""), then get all issues for the project
        if not from_date:
                
            #pull full set of issues for the project
            ist.msg("No issues file found.  Retrieving all issues for the project.",True)
            issues = self.retrieve_issues(jql="project=" + self.project_key,max_results=0)

        else:
            #pull just the latest issues
            ist.msg("Retrieving issues modified on or after: " + from_date)
            issues = self.retrieve_latest_issues(from_date)

            
        # iterate through all data_parts and update issues   
        for data_part in self.data_part_settings['data_part'].to_list():
            ist.msg("Updating data_part " + data_part,True)
            self.refresh_data_part(data_part,issues)
            
            
    
        ist.msg("Issue Information update completed...",True,start)
        ist.msg("###################################################")
        return True

    def issue_to_dataframe_all_fields(self,issue):
        """
        Returns a dataframe with all available fields in a Jira Issue object
        
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        !! this method is a way to inspect all of the fields in a jira issues object
        !! for quick reference.
        !!
        !! Please note, the value may need to be parsed as a nested Jira object
        !! For full reference, see the documentation at:
        !! https://jira.readthedocs.io/en/master/
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        
        PLEASE NOTE: INSPECTION OF THE RETURNED DATAFRAME FROM A VARIABLE
        EXPLORER MAY RESULT IN A RECURSSION ERRROR DUE TO SOME FIELDS CONTAINING
        MULTIPLE JIRA OBJECTS.
        """
        
        df = pd.DataFrame({'issue_key':[0]})
        
        df['issue_key'] = issue.key
        
        df['aggregateprogress'] = self.ifnull(issue.fields.aggregateprogress)
        df['aggregatetimeestimate'] = self.ifnull(issue.fields.aggregatetimeestimate)
        df['aggregatetimeoriginalestimate'] = self.ifnull(issue.fields.aggregatetimeoriginalestimate)
        df['aggregatetimespent'] = self.ifnull(issue.fields.aggregatetimespent)
        df['assignee'] = self.ifnull(issue.fields.assignee)
        df['components'] = self.ifnull(issue.fields.components)
        df['created'] = self.ifnull(issue.fields.created)
        df['creator'] = self.ifnull(issue.fields.creator)
        df['customfield_10000'] = self.ifnull(issue.fields.customfield_10000)
        df['customfield_10001'] = self.ifnull(issue.fields.customfield_10001)
        df['customfield_10002'] = self.ifnull(issue.fields.customfield_10002)
        df['customfield_10003'] = self.ifnull(issue.fields.customfield_10003)
        df['customfield_10006'] = self.ifnull(issue.fields.customfield_10006)
        df['customfield_10007'] = self.ifnull(issue.fields.customfield_10007)
        df['customfield_10011'] = self.ifnull(issue.fields.customfield_10011)
        df['customfield_10025'] = self.ifnull(issue.fields.customfield_10025)
        df['customfield_10100'] = self.ifnull(issue.fields.customfield_10100)
        df['customfield_10101'] = self.ifnull(issue.fields.customfield_10101)
        df['customfield_10102'] = self.ifnull(issue.fields.customfield_10102)
        df['customfield_10104'] = self.ifnull(issue.fields.customfield_10104)
        df['customfield_10105'] = self.ifnull(issue.fields.customfield_10105)
        df['customfield_10300'] = self.ifnull(issue.fields.customfield_10300)
        df['customfield_10400'] = self.ifnull(issue.fields.customfield_10400)
        df['customfield_10500'] = self.ifnull(issue.fields.customfield_10500)
        df['customfield_10600'] = self.ifnull(issue.fields.customfield_10600)
        df['customfield_10700'] = self.ifnull(issue.fields.customfield_10700)
        df['customfield_10800'] = self.ifnull(issue.fields.customfield_10800)
        df['customfield_10801'] = self.ifnull(issue.fields.customfield_10801)
        df['customfield_10804'] = self.ifnull(issue.fields.customfield_10804)
        df['customfield_10805'] = self.ifnull(issue.fields.customfield_10805)
        df['customfield_10807'] = self.ifnull(issue.fields.customfield_10807)
        df['customfield_10808'] = self.ifnull(issue.fields.customfield_10808)
        df['customfield_10809'] = self.ifnull(issue.fields.customfield_10809)
        df['customfield_10812'] = self.ifnull(issue.fields.customfield_10812)
        df['customfield_10813'] = self.ifnull(issue.fields.customfield_10813)
        df['customfield_10814'] = self.ifnull(issue.fields.customfield_10814)
        df['customfield_10815'] = self.ifnull(issue.fields.customfield_10815)
        df['customfield_10817'] = self.ifnull(issue.fields.customfield_10817)
        df['customfield_10818'] = self.ifnull(issue.fields.customfield_10818)
        df['customfield_10819'] = self.ifnull(issue.fields.customfield_10819)
        df['customfield_10820'] = self.ifnull(issue.fields.customfield_10820)
        df['customfield_10821'] = self.ifnull(issue.fields.customfield_10821)
        df['customfield_10822'] = self.ifnull(issue.fields.customfield_10822)
        df['customfield_10823'] = self.ifnull(issue.fields.customfield_10823)
        df['customfield_10824'] = self.ifnull(issue.fields.customfield_10824)
        df['customfield_10825'] = self.ifnull(issue.fields.customfield_10825)
        df['customfield_10826'] = self.ifnull(issue.fields.customfield_10826)
        df['customfield_10827'] = self.ifnull(issue.fields.customfield_10827)
        df['customfield_10828'] = self.ifnull(issue.fields.customfield_10828)
        df['description'] = self.ifnull(issue.fields.description)
        df['duedate'] = self.ifnull(issue.fields.duedate)
        df['environment'] = self.ifnull(issue.fields.environment)
        df['fixVersions'] = self.ifnull(issue.fields.fixVersions)
        df['issuelinks'] = self.ifnull(issue.fields.issuelinks)
        df['issuetype'] = self.ifnull(issue.fields.issuetype)
        df['labels'] = self.ifnull(issue.fields.labels)
        df['lastViewed'] = self.ifnull(issue.fields.lastViewed)
        df['priority'] = self.ifnull(issue.fields.priority)
        df['progress'] = self.ifnull(issue.fields.progress)
        df['project'] = self.ifnull(issue.fields.project)
        df['reporter'] = self.ifnull(issue.fields.reporter)
        df['resolution'] = self.ifnull(issue.fields.resolution)
        df['resolutiondate'] = self.ifnull(issue.fields.resolutiondate)
        df['security'] = self.ifnull(issue.fields.security)
        df['status'] = self.ifnull(issue.fields.status)
        df['statuscategorychangedate'] = self.ifnull(issue.fields.statuscategorychangedate)
        df['subtasks'] = self.ifnull(issue.fields.subtasks)
        df['summary'] = self.ifnull(issue.fields.summary)
        df['timeestimate'] = self.ifnull(issue.fields.timeestimate)
        df['timeoriginalestimate'] = self.ifnull(issue.fields.timeoriginalestimate)
        df['timespent'] = self.ifnull(issue.fields.timespent)
        df['updated'] = self.ifnull(issue.fields.updated)
        df['versions'] = self.ifnull(issue.fields.versions)
        df['votes'] = self.ifnull(issue.fields.votes)
        df['watches'] = self.ifnull(issue.fields.watches)
        df['workratio'] = self.ifnull(issue.fields.workratio)


        return df