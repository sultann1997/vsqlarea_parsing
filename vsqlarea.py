import cx_Oracle
import os
import pandas as pd
import requests
import pyarrow as pa
from pyarrow import fs
import ahocorasick 
from datetime import datetime

#classpath.txt = subprocess.getoutput('hdfs classpath --glob')
with open('classpath.txt', 'r') as file:
    os.environ['CLASSPATH'] = file.read()
    
os.environ['ARROW_LIBHDFS_DIR'] = '/hadoop-3.1.1/lib/native'
hdfs = fs.HadoopFileSystem('hdfs://hadoop_server:port?user=user')

oh = "/oracle/11.2/client64"
os.environ["ORACLE_HOME"] = oh
os.environ["PATH"] = oh + os.pathsep + os.environ["PATH"]
os.environ['TNS_ADMIN'] = '/oracle/11.2/client64/lib/network/admin'
dsn_var = '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=port))(ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=port2)))(CONNECT_DATA=(SERVICE_NAME=dbase service)))'

token = os.environ['my_token']
dependence = 'my_dep'

r = requests.get(f"api with passwords/?token={token}&dependence={dependence}")\
.json()
username, password = r['username'], r['password']


class OracleTable():
    def __init__(self, dsn=dsn_var):
        self.user,self.password = username, password
        self.dsn = dsn
        self.connect = cx_Oracle.Connection(user=self.user, password=self.password, dsn=self.dsn)

    def returnDataframe(self, sql_statement):
        dataframe = None
        try:
            dataframe = pd.read_sql(sql_statement, con=self.connect)
        except cx_Oracle.DatabaseError as ex:
            err, = ex.args
            print("Error code    = ", err.code)
            print("Error Message = ", err.message)
        return dataframe

    def pushDatatoOracle(self, dataframe, table_name):
        try:
            cur = self.connect.cursor()

            rows = [tuple(x) for x in dataframe.values]

            columns = ','.join(dataframe.columns)
            length_vals = [str(x + 1) for x in range(len(dataframe.columns))]
            indexes = ':' + ',:'.join(length_vals)

            insertion_string = """INSERT INTO """ + table_name + """ (""" + columns + """) VALUES (""" + indexes + ')'

            print(insertion_string)
            cur.executemany(insertion_string, rows, batcherrors=True)
            self.connect.commit()
        except cx_Oracle.DatabaseError as ex:
            err, = ex.args
            print("Error code    = ", err.code)
            print("Error Message = ", err.message)
    
    def close(self):
        self.connect.close()

        
class Query_parse:
    def __init__(self, query):
        self.query = query
        self.tables = []
        self.DMLs = []

    def callback_tabs(self, index, word):
        self.tables.append(word)
        
    def callback_DMLs(self, index, word):
        self.DMLs.append(word)
    
    def find_tables(self):
        ac_tables.find_all(self.query, self.callback_tabs)
        for c in range(2):
            for i in self.tables:
                for j in self.tables:
                    if len(i) < len(j) and i in j:
                        try:
                            self.tables.remove(i)
                        except:
                            pass
        return set(self.tables)
    
    def find_DMLs(self):
        try:
            string = next(ac_DML.iter(self.query))[1]
        except:
            string = ''
        return string

    
sql_query = """
select * from v$sqlarea
"""
Ora = OracleTable()
df = Ora.returnDataframe(sql_query)

all_tabs_query = """
select * from all_tables t
where t.OWNER not in ('LIST OF SYS TABLES OR UNWANTED TABLES')
"""
all_tabs = Ora.returnDataframe(all_tabs_query)
all_tabs['owner_table'] = all_tabs.OWNER + '.' + all_tabs['TABLE_NAME']
tabs_list = all_tabs.TABLE_NAME.tolist() + all_tabs.owner_table.tolist()
try:
    tabs_list.remove('T')
except:
    pass
tabs_list = [f'FROM {tab}' for tab in tabs_list] +\
[f'INTO {tab}' for tab in tabs_list] +\
[f'JOIN {tab}' for tab in tabs_list] +\
[f'UPDATE {tab}' for tab in tabs_list] +\
[f'"{tab}"' for tab in tabs_list] +\
all_tabs.owner_table.tolist()

vsqlarea_df = pd.read_parquet('vsqlarea/vsqlarea.parquet', filesystem=hdfs)
vsql_temp = df.drop([
    'SQL_FULLTEXT', 'OPTIMIZER_COST', 'EXACT_MATCHING_SIGNATURE', 'FORCE_MATCHING_SIGNATURE'
], axis=1)
vsql_temp['LAST_LOAD_TIME'] = vsql_temp['LAST_LOAD_TIME'].astype(str)
vsql_temp['LAST_ACTIVE_TIME'] = vsql_temp['LAST_ACTIVE_TIME'].astype(str)
vsqlarea_df.append(vsql_temp)\
.drop_duplicates(subset = ['SQL_ID'], keep='last')\
.to_parquet('/vsqlarea/vsqlarea.parquet', filesystem=hdfs)

parsed_df = pd.read_parquet('parsed_sql/parsed_sql.parquet', filesystem=hdfs)
not_parsed_df = df[~df.SQL_ID.isin(parsed_df.SQL_ID)]
FULLTEXT = [''.join(i.read()).upper() for i in not_parsed_df.SQL_FULLTEXT]
Ora.pushDatatoOracle(
    not_parsed_df.drop([
        'SQL_FULLTEXT', 'OPTIMIZER_COST', 'EXACT_MATCHING_SIGNATURE', 'FORCE_MATCHING_SIGNATURE'
    ], axis=1), 'MY_SCHEMA.VSQLAREA_HISTORICAL'
)
schema_dict = {key:value for key, value in zip(all_tabs.TABLE_NAME, all_tabs.OWNER)}

ac_tables = ahocorasick.Automaton()
for tname in tabs_list:
    ac_tables.add_word(
        tname,
        tname.replace('FROM ', '').replace('INTO ', '').replace('JOIN ', '').replace('"', '').replace("UPDATE", "").strip()
    )
ac_tables.make_automaton()

DML_list = ['SELECT', 'UPDATE', 'INSERT', 'DELETE', 'WITH', 'LOCK']
ac_DML = ahocorasick.Automaton()
for DML in DML_list:
    ac_DML.add_word(DML, DML)
ac_DML.make_automaton()

results = []
now = datetime.now()
timestamp = datetime(
    year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute, second=now.second, microsecond=0
)
for query, SQL_ID in zip(FULLTEXT, not_parsed_df.SQL_ID):
    q = Query_parse(query)
    tables, DMLs = q.find_tables(), q.find_DMLs()
    if len(tables)>0:
        for table in tables:
            if table.find('.') != -1:
                schema = table[:table.find('.')]
                t = table[table.find('.')+1:]
            else:
                try:
                    schema = schema_dict[table]
                except:
                    schema = ''
                t = table
            temp_dict = {
                'SQL_ID': SQL_ID,
                'TABLE_NAME': t,
                'DMLS': DMLs,
                'SYSMOMENT': timestamp,
                'OWNER' : schema
            }

    else:
        temp_dict = {
            'SQL_ID': SQL_ID,
            'TABLE_NAME': '',
            'DMLS': 'Procedure',
            'SYSMOMENT': timestamp,
            'OWNER' : ''
        }
    results.append(temp_dict)
if len(results)>0:
    temp_df = pd.DataFrame(results)
    Ora.pushDatatoOracle(temp_df, 'MY_SCHEMA.VSQLAREA_PARSED')
    temp_df.SYSMOMENT = temp_df.SYSMOMENT.astype(str)
    parsed_df.append(temp_df).to_parquet(
        '/hdfs_path/parsed_sql/parsed_sql.parquet', 
        filesystem=hdfs
    )
else:
    pass
Ora.close()
