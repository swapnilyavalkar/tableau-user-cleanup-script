import datetime
import pandas as pd

env = ''
server_url = "https://abc.com/"
user_name = ''
password = ''
site_id = ""

df_lastRoleUpdatedUsers = pd.DataFrame()
df_basedonlastlogin = pd.DataFrame()
df_subscriptions = pd.DataFrame()
df_workbooks = pd.DataFrame()
df_datasources = pd.DataFrame()
df_flows = pd.DataFrame()
df_final_data = pd.DataFrame()
df_restored_access = pd.DataFrame(columns=['site_name', 'user_name', 'created_day_count', 'last_login_day_count',
                                           'previous_role', 'new_role'])

# Variables used for Execution #
FROM = 'automation@abc.com'
TO = 'admin@abc.com' # update this with all required users.
CC = ''  # update DL here.
ADMIN_DL = 'admin@abc.com' # used for failure attempts.

# Variables used for Execution #
TODAY = datetime.datetime.now()

smtp_host = 'mail.abc.com'
smtp_port = 25

tableau_auth = ''
server = ''
pgsql_connection = ''
list_of_errors = pd.DataFrame(columns=['TIME', 'FUNCTION_NAME', 'ERROR_MESSAGE'])
