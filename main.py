import numpy as np
import pandas
import tableauserverclient as TSC
import psycopg2
import pandas as pd
import logging
import smtplib
from email.message import EmailMessage
import config
import Variables
import time

LOG_FILE_GEN_TIME = time.strftime("%Y%m%d-%H%M%S")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s, %(levelname)-8s [%(filename)s:%(module)s:%(funcName)s:%(name)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    filename='logs/Unlicensed_{0}.log'.format(LOG_FILE_GEN_TIME),
    filemode='a'
)


def sendEmail(SUBJECT, BODY):
    try:
        msg = EmailMessage()
        msg['Subject'] = SUBJECT
        msg['From'] = Variables.FROM
        msg['To'] = Variables.TO
        if "SUCCESS" in SUBJECT:
            msg.set_content(BODY)
            msg.add_alternative(
                "<!DOCTYPE html> <html> <body> <p><b>Hi Team,</p></p>Following users have been marked as "
                "unlicensed.<br> "
                + BODY
                + "<br><p><b>Regards, <br><b>Tableau Admin Team</p> </body> </html>",
                subtype='html')
            logging.debug('Sending Email To Users For Successful Operation.')
        elif "FAILURE" in SUBJECT:
            msg.set_content(BODY)
            msg.add_alternative(
                "<!DOCTYPE html> <html> <body> <p><b>Hi Team,</p></p><b>This activity has failed due to below errors:  "
                "</p><br>" + BODY + "<p><b>Regards, <br><b>Tableau Admin Team</p> </body> </html>",
                subtype='html')
            logging.debug('Sending Email To Users For Failed Operation.')
        else:
            msg.set_content(BODY)
            msg.add_alternative(
                "<!DOCTYPE html> <html> <body> <p><b>Hi Team,</p><br>" + BODY
                + "<p><b>Regards, <br><b>Tableau Admin Team</p> </body> </html>",
                subtype='html')
            logging.debug('Sending Email To Users For Not Required Operation.')
        with smtplib.SMTP(Variables.smtp_host, Variables.smtp_port) as smtp:
            smtp.send_message(msg)
    except Exception as e:
        logging.critical("Exception Occurred In : sendEmail", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'sendEmail',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def removeSubscribedUsers(df):
    try:
        query = open('sql_queries/removeSubscribedUsers.sql', 'r')
        query_string = query.read()
        for index, row in df.iterrows():
            query_final = query_string + "'" + str(row['USERS_NAME']) + "'"
            df_subscriptions_temp = pd.read_sql_query(query_final, Variables.pgsql_connection)
            if df_subscriptions_temp.empty:
                Variables.df_subscriptions = Variables.df_subscriptions.append(row)
        logging.info('Followings are the users from removeSubscribedUsers()')
        logging.info(Variables.df_subscriptions.to_string(), exc_info=True)
    except Exception as e:
        logging.critical("Exception Occurred In: removeSubscribedUsers", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'removeSubscribedUsers',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def removeFlowOwners(df):
    try:
        query = open('sql_queries/removeFlowsOwners.sql', 'r')
        query_string = query.read()
        for index, row in df.iterrows():
            query_final = query_string + "'" + str(row['USERS_ID']) + "'"
            df_flows_temp = pd.read_sql_query(query_final, Variables.pgsql_connection)
            if df_flows_temp.empty:
                Variables.df_flows = Variables.df_flows.append(row)
        logging.info('Followings are the users from removeFlowOwners()')
        logging.info(Variables.df_flows.to_string(), exc_info=True)
    except Exception as e:
        logging.critical("Exception Occurred In: removeFlowOwners", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'removeFlowOwners',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def removeDataSourceOwners(df):
    try:
        query = open('sql_queries/removeDataSourceOwners.sql', 'r')
        query_string = query.read()
        for index, row in df.iterrows():
            query_final = query_string + "'" + str(row['USERS_NAME']) + "'"
            df_datasource_temp = pd.read_sql_query(query_final, Variables.pgsql_connection)
            if df_datasource_temp.empty:
                Variables.df_datasources = Variables.df_datasources.append(row)
        logging.info('Followings are the users from removeDataSourceOwners()')
        logging.info(Variables.df_datasources.to_string(), exc_info=True)
    except Exception as e:
        logging.critical("Exception Occurred In: removeDataSourceOwners", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'removeDataSourceOwners',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def removeWorkbookOwners(df):
    try:
        query = open('sql_queries/removeWorkbookOwners.sql', 'r')
        query_string = query.read()
        for index, row in df.iterrows():
            query_final = query_string + "'" + str(row['USERS_NAME']) + "'"
            df_workbooks_temp = pd.read_sql_query(query_final, Variables.pgsql_connection)
            if df_workbooks_temp.empty:
                Variables.df_workbooks = Variables.df_workbooks.append(row)
        logging.info('Followings are the users from removeWorkbookOwners()')
        logging.info(Variables.df_workbooks.to_string(), exc_info=True)
    except Exception as e:
        logging.critical("Exception Occurred In: removeWorkbookOwners", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'removeWorkbookOwners',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def markUnlicensed(df):
    try:
        for index, row in df.iterrows():
            Variables.tableau_auth = TSC.TableauAuth(Variables.user_name, Variables.password,
                                                     site_id=row['SITE_URL'])
            Variables.server = TSC.Server(Variables.server_url)
            Variables.server.add_http_options({'verify': False})
            Variables.server.use_server_version()
            req_option = TSC.RequestOptions()
            req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                             TSC.RequestOptions.Operator.Equals,
                                             row['USERS_NAME']))
            with Variables.server.auth.sign_in(Variables.tableau_auth):
                user_item, pagination = Variables.server.users.get(req_option)
                previous_role = user_item[0].site_role
                user_item[0].site_role = 'Unlicensed'
                user = user_item[0]
                user = Variables.server.users.update(user_item[0])
                Variables.df_restored_access = Variables.df_restored_access.append(
                    {'site_name': row['SITE_NAME'],
                     'user_name': user.name,
                     'created_day_count': row['CREATED_DAY_COUNT'],
                     'last_login_day_count': row['LAST_LOGIN_DAY_COUNT'],
                     'previous_role': previous_role,
                     'new_role': user.site_role},
                    ignore_index=True)
                logging.info(user.name + ' User\'s: ' + ' previous role: ' + previous_role + ' changed to: '
                             + user.site_role + ' in site: ' + row['SITE_NAME'], exc_info=True)
                logging.debug('Closing Connection to Tableau Server.')
                Variables.server.auth.sign_out()
        logging.info('Followings are the users from markUnlicensed()')
        logging.info(Variables.df_restored_access.to_string(), exc_info=True)
    except Exception as e:
        logging.critical("Exception Occurred In: markUnlicensed", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'markUnlicensed',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def closeAllConnections():
    try:
        logging.debug('Closing Connection to PG SQL Database.')
        Variables.pgsql_connection.close()
    except Exception as e:
        logging.critical("Exception Occurred In: closeAllConnections", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'closeAllConnections',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def removeUsersBasedOnLogin(df):
    try:
        df['LAST_LOGIN_DAY_COUNT'] = pd.to_numeric(df['LAST_LOGIN_DAY_COUNT'])
        Variables.df_basedonlastlogin = df.query('LAST_LOGIN_DAY_COUNT > 100 | LAST_LOGIN_DAY_COUNT.isnull()',
                                                 engine='python')
        logging.info('Followings are the users from removeUsersBasedOnLogin()')
        logging.info(Variables.df_basedonlastlogin.to_string(), exc_info=True)
    except Exception as e:
        logging.critical("Exception Occurred In: removeUsersBasedOnLogin", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'removeUsersBasedOnLogin',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def removeLastRoleUpdatedUsers(df):
    try:
        temp = pd.DataFrame()
        temp2 = pd.DataFrame()
        query = open('sql_queries/removeLastRoleUpdatedUsers.sql', 'r')
        query_string = query.read()
        for index, row in df.iterrows():
            query_final = query_string + "'" + str(row['USERS_ID']) + "' GROUP BY HIST_USERS.USER_ID, " \
                                                                      "HIST_USERS.NAME, HIST_USERS.EMAIL, " \
                                                                      "HIST_USERS.SYSTEM_USER_ID"
            df_temp = pd.read_sql_query(query_final, Variables.pgsql_connection)
            if not df_temp.empty and df_temp['DAY_COUNT_USER_SITE_ROLE_CHANGED'].ge(60).all():
                temp = temp.append(df_temp)  # remove this.
                Variables.df_lastRoleUpdatedUsers = Variables.df_lastRoleUpdatedUsers.append(row)
            else:
                temp2 = temp2.append(df_temp)
        temp.to_excel('files\\temp.xlsx')  # remove this.
        temp2.to_excel('files\\temp2.xlsx')
        logging.info('Followings are the users from removeLastRoleUpdatedUsers()')
        logging.info(Variables.df_lastRoleUpdatedUsers.to_string(), exc_info=True)
    except Exception as e:
        logging.critical("Exception Occurred In: removeLastRoleUpdatedUsers", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'removeLastRoleUpdatedUsers',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


def generateExcels(df):
    df.to_excel('files\\_all-users-1.xlsx')
    Variables.df_basedonlastlogin.to_excel('files\\df_basedonlastlogin-2.xlsx')
    Variables.df_subscriptions.to_excel('files\\_df_subscriptions-3.xlsx')
    Variables.df_workbooks.to_excel('files\\_df_workbooks-4.xlsx')
    Variables.df_datasources.to_excel('files\\_df_datasources-5.xlsx')
    Variables.df_flows.to_excel('files\\_df_flows-6.xlsx')
    Variables.df_lastRoleUpdatedUsers.to_excel('files\\df_lastRoleUpdatedUsers-7.xlsx')
    Variables.df_lastRoleUpdatedUsers.to_excel('files\\_final_data-8.xlsx')
    Variables.df_restored_access.to_excel('files\\_df_restored_access-9.xlsx')


def PGsignIn():
    try:
        logging.debug('Connecting to PG SQL Database.')
        # Create the connection object and connect to PG SQL DB.
        paramspg = config.configpg()
        Variables.pgsql_connection = psycopg2.connect(**paramspg)
        cursor = Variables.pgsql_connection.cursor()
        logging.debug('Reading PG SQL Query.')
        # Condition: 1 Checking for all users #
        query = open('sql_queries/first-query.sql', 'r')
        logging.debug('Executing PG SQL Query.')
        data = pd.read_sql_query(query.read(), Variables.pgsql_connection)
        logging.debug('Creating DataFrame.')
        df = pd.DataFrame(data)
        if df.empty:
            logging.debug('There are no users to process')
            sendEmail("NOT REQUIRED - DAILY - TABLEAU " + Variables.env + " - USERS CLEANUP ACTIVITY",
                      'There are no users to process.')
            closeAllConnections()
            logging.debug(('#' * 15) + ' CleanUp Operation Completed ' + ('#' * 15))
        else:
            df.to_excel('files\\df1.xlsx')
            df.sort_values(by=['LAST_LOGIN'], ascending=False, inplace=True)
            df.drop_duplicates(subset="USERS_NAME", keep='first', inplace=True)
            df.to_excel('files\\df2.xlsx')
            removeUsersBasedOnLogin(df)  # LAST_LOGIN_DAY_COUNT > 100 | LAST_LOGIN_DAY_COUNT.isnull()
            removeSubscribedUsers(Variables.df_basedonlastlogin)
            removeWorkbookOwners(Variables.df_subscriptions)
            removeDataSourceOwners(Variables.df_workbooks)
            removeFlowOwners(Variables.df_datasources)
            removeLastRoleUpdatedUsers(Variables.df_flows)
            markUnlicensed(Variables.df_lastRoleUpdatedUsers)
            generateExcels(df)
            closeAllConnections()
    except Exception as e:
        logging.critical("Exception Occurred In: PGsignIn", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': 'PGsignIn',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)


if __name__ == '__main__':
    try:
        logging.debug(('#' * 15) + ' Operation Started ' + ('#' * 15))
        PGsignIn()
        if not Variables.df_restored_access.empty and Variables.list_of_errors.empty:
            logging.debug(('#' * 15) + ' CleanUp Operation Completed ' + ('#' * 15))
            sendEmail("SUCCESS - WEEKLY - TABLEAU " + Variables.env + " - USERS CLEANUP ACTIVITY",
                      Variables.df_restored_access.to_html(na_rep="", index=False, render_links=True,
                                                           escape=False).replace('<th>', '<th style="color:white; '
                                                                                         'background-color:#180e62">'))
        elif Variables.df_restored_access.empty and not Variables.list_of_errors.empty:
            sendEmail("FAILURE - WEEKLY - TABLEAU " + Variables.env + " - USERS CLEANUP ACTIVITY",
                      Variables.list_of_errors.to_html(na_rep="", index=False, render_links=True,
                                                       escape=False).replace('<th>', '<th style="color:white; '
                                                                                     'background-color:#180e62">'))
            logging.debug(('#' * 15) + ' CleanUp Operation Completed ' + ('#' * 15))
        elif Variables.df_restored_access.empty and Variables.list_of_errors.empty:
            logging.debug('There are no users to process')
            sendEmail("NOT REQUIRED - WEEKLY - TABLEAU " + Variables.env + " - USERS CLEANUP ACTIVITY",
                      'There are no users to process.')
            logging.debug(('#' * 15) + ' CleanUp Operation Completed ' + ('#' * 15))
        else:
            sendEmail("FAILURE - WEEKLY - TABLEAU " + Variables.env + " - USERS CLEANUP ACTIVITY",
                      Variables.list_of_errors.to_html(na_rep="", index=False, render_links=True,
                                                       escape=False).replace('<th>', '<th style="color:white; '
                                                                                     'background-color:#180e62">'))
            logging.debug(('#' * 15) + ' CleanUp Operation Completed ' + ('#' * 15))
    except Exception as e:
        logging.critical("Exception Occurred In: __main__", exc_info=True)
        Variables.list_of_errors = Variables.list_of_errors.append(
            {'TIME': time.strftime("%d:%m:%Y %H:%M:%S"), 'FUNCTION_NAME': '__main__',
             'ERROR_MESSAGE': repr(e)}, ignore_index=True)
        sendEmail("FAILURE - WEEKLY - TABLEAU " + Variables.env + " - USERS CLEANUP ACTIVITY",
                  Variables.list_of_errors.to_html(na_rep="", index=False, render_links=True,
                                                   escape=False).replace('<th>', '<th style="color:white; '
                                                                                 'background-color:#180e62">'))
        logging.debug(('#' * 15) + ' CleanUp Operation Failed ' + ('#' * 15))
