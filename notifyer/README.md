# Notifyer (SQL + Python)

## Object
The purpose of the program is to notify users about order statuses.

## Algorithm description
The main script is in the `notifyer.py` file. The functions and authorization data is placed in a special connetor `notifyer_conn.py`. 

The main script was designed to work in DAG Aifrlow. So it allows to automate the launch of the script.
The `notifyer.py` included agruments for DAG detalis and the following tasks:
- get_orders: The function receives data on order statuses using a SQL request (PostgreSQL). The SQL query is complex and including using different functions (such as temp table, concat, joins). Task returns a dataframe with order details.
- check_df: The function accesses another database using SQL query (PostgreSQL as well). This database stores logs of previous runs. After connecting the data obtained at the previous task and with the current logs, duplicate entries excludes, to prevents from sending messages again. Task returns a dataframe.
- split_df: On this task the script divides the data into 3 different groups, according to the customer's requirement: id1 - orders, where customers need to provide additional data; id2 - orders where it is necessary to notify customers about new statuses; id3 - orders where it need to invite customers to pick up the order. For id2 and id3 it creates separate tables to account for the ids of users to whom messages need to be sent. Task retruns 5 dfs.
- check_df{1,2,3}: These tasks check to see if new data has arrived via  DAG `ShortCircuitOperator`. If there is no new data for any of the id-group, the further task is skipped. This saves resources. Tasks returns True/False to confirm further tasks run.
- id{1,2,3}_send_messages: These functions send messages using API (requests module). The pandas filtering functions of the input dataframes are used to extract data for the messages. Messages are sent with a if-loop. Each order for which messages were sent is logged by sending the df-record to the log table (using sqlalchemy module).

The `notifyer_conn.py` contains the necessary modules for connection. The file describes the functions for editing records, connecting to databases and data for authorization.