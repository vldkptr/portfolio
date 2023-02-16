from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

import sys
sys.path.append('/opt/airflow/dags/patrakhin/modules')
from notifier_conn import *

default_args = {
    "owner": "Patrakhin",
    "start_date": datetime(2021, 1, 19),
    "email_on_failure": False,
}

dag = DAG(
    "orders_notifyer",
    schedule_interval="40 */1 * * *",
    default_args=default_args,
    description="Notifications regarding order statuses",
    catchup=False,
    tags=["automat"],
)


def get_orders(**context):

    query = """
    drop table if exists OpenOrders;
    create temp table OpenOrders as
    select distinct
        Order.Order_id
        , Order.Order_entity as "Orderuid" 
        , concat('Order ', Orderdoc.Number, ' from ', to_char(Orderdoc.Date, 'DD.MM.YYYY'), ' ', to_char(Orderdoc.Date, 'HH24:MI:SS')) as OrderEntity
        , Orderdoc.Branch_id as "Branch"
        , Branches.Name as "Branch_name"
        , Orderdoc.User_id
        , Users.Name as "User_name"
    from
        main_op."Orders_elements" as Order
            inner join main_op."vop_D.FiksatsijaPotrebnostiZIP" as Orderdoc
                on Order.Order_entity = Orderdoc.Order_entity
            inner join "main"."Branches.Hierarchy" as Hierarchy
                on Orderdoc.Branch_id = Hierarchy.Branch_id 
            inner join "main"."Branches" as Branches
                on Orderdoc.Branch_id = Branches.Branch_id
            inner join "main"."Users" as Users
                on Orderdoc.User_id = Users.User_id
    where 
        Order."Status" = 1 --Open
        and (Hierarchy.Division_id in ('ed08738a-ac07-11ed-afa1-0242ac120002'   -- East id
                                                , 'f5f6599e-ac07-11ed-afa1-0242ac120002' -- South-East id
                                                , '77a15ffc-ac08-11ed-afa1-0242ac120002' -- North-East id
                                                , '839435b4-ac08-11ed-afa1-0242ac120002' -- South-West id
                                                )
            or Branches.Branch_id = '1a6b1209-c3ef-4baf-b01c-b560230e9e54')       -- East Branch id
        and Orderdoc.User_id not in ('8ceb96b5-a05b-40b3-b17e-3c162fbed120'     -- user1 exception id
                                , 'd6aff4a58-8e67-425e-91d9-2f349f677789'    -- user2 exception id
                                ) 
    distributed randomly
    ;

    drop table if exists group1;
    create temp table group1 as
    select
        Logs1."timestamp"
        , Logs1."Doc_type"
        , OpenOrders."Order_entity"
        , Logs1."Order_id"
        , 1 as "Group_id"
        , 'product_name to provide data'::text as "Status"
        , OpenOrders."User_id"
        , OpenOrders."User_name"
    from
        main_op."Orders_logs" as Logs1	
            inner join OpenOrders as OpenOrders
                on Logs1.Order_id = OpenOrders.Order_id
    where
        Logs1.Status_id = '12099894-080a-442a-bd2f-901936c1c904' --product_name to provide data id
        and Logs1.timestamp >= current_date
    distributed randomly ;

    drop table if exists group1check;
    create temp table group1check as
    select distinct 
        Logscheck1.Order_id
    from
        main_op."Orders_logs" as Logscheck1
            inner join OpenOrders as OpenOrders
                on Logscheck1.Order_id = OpenOrders.Order_id
            inner join group1 as group1
                on group1.Order_id = Logscheck1.Order_id
                    and Logscheck1.timestamp >= group1.timestamp
    where
    Logscheck1.Status_id = '6b5d91db-dcd9-4387-8ebb-99fedfaf9140' --Data provided id
    distributed randomly
    ;

    drop table if exists group2;
    create temp table group2 as
    select
        Logs2.timestamp
        , Logs2."Doc_type"
        , OpenOrders."Order_entity"
        , Logs2.Order_id
        , 2 as "Group_id"
        , case 
            when Logs2.Status_id = 'd7e28732-ec67-41b7-b4a6-00da99c3c292'
                then 'Purchase approved'::text
            else 'Purchase denied'::text
        end as Status
        , OpenOrders."User_id"
        , OpenOrders."User_name"
    from
        main_op."Orders_logs" as Logs2
            inner join OpenOrders as OpenOrders
                on Logs2.Order_id = OpenOrders.Order_id
    where
        Logs2.Status_id in (
            '85636b45-98ae-4b66-9fc4-751aaf3f2d8a' --Approved id 
            , 'ac4bdc9f-b6d3-4a9e-a51b-0559552967d8' --Denied id
            )
        and Logs2.timestamp >= current_date
    distributed randomly ;

    drop table if exists group2check;
    create temp table group2check as
    select distinct 
        Logscheck2.Order_id
    from
        main_op."Orders_logs" as Logscheck2
            inner join OpenOrders as OpenOrders
                on Logscheck2.Order_id = OpenOrders.Order_id
            inner join group2 as group2
                on group2.Order_id = Logscheck2.Order_id
                    and group2.Status = 'Purchase denied'
                    and Logscheck2.timestamp >= group2.timestamp
    where
        Logscheck2.Status in (
            '37c9400a-ef72-45a3-a568-c1c036a62e59' --Internal order id
            , '694ff80a-e6d3-4c64-83a5-0c5d93adc73d' --External order id
            ) 
    distributed randomly
    ;	


    drop table if exists group3;
    create temp table group3 as
    select 	
        Logs3.timestamp
        , Logs3."Doc_type"
        , OpenOrders.Order_entity
        , Logs3.Order_id
        , 3 as "Group_id"
        , 'Order delivered'::text as Status
        , OpenOrders.User_id 
        , OpenOrders."User_name"
    from 
        main_op."Orders_logs" as Logs3
            inner join OpenOrders as OpenOrders
                on Orderd.Order_id = OpenOrders.Order_id
    where
        Orderd.Status_id = 'df7d3a26-6dfb-4f9d-9300-0b14ddd47913' -- Order delivered id
        and Orderd.timestamp >= current_date
    distributed randomly
    ;

    drop table if exists group3check;
    create temp table group3check as
    select distinct 
        Logscheck3.Order_id
    from
        main_op."Orders_logs" as Logscheck3
            inner join OpenOrders as OpenOrders
                on Logscheck3.Order_id = OpenOrders.Order_id
            inner join group3 as group3
                on group3.Order_id = Logscheck3.Order_id
                    and Logscheck3.timestamp >= group3.timestamp
    where
        Logscheck3.Status = '118562ee-631c-465e-ad3e-3b2ded75c961' --Order received id
        and Logscheck3.timestamp >= current_date
    distributed randomly
    ;


    drop table if exists Results_table;
    create temp table Results_table as
    select
        group1.timestamp
        , group1.Doc_type
        , group1."Order_entity"
        , group1.Order_id
        , group1."Group_id"
        , group1.Status
        , group1.User_id
        , group1."User_name"
        , NULL as Receipt_id
        , NULL as "Receipt"
    from 
        group1 as group1
    where
        group1.Order_id not in (
                                select distinct 
                                    group1check.Order_id
                                from 
                                    group1check as group1check
                                )
    union
    select
        group2.timestamp
        , group2.Doc_type
        , group2."Order_entity"
        , group2.Order_id
        , group2."Group_id"
        , group2.Status
        , group2.User_id
        , group2."User_name"
        , NULL 
        , NULL 
    from
        group2 as group2
    where
        group2.Order_id not in (
                                select distinct 
                                    group2check.Order_id
                                from 
                                    group2check as group2check
                                )
    union
    select
        group3.timestamp
        , group3.Doc_type
        , group3.Order_entity
        , group3.Order_id
        , group3."Group_id"
        , group3.Status
        , group3.User_id
        , group3."User_name"
        , rsOrderlast.Status
        , rsOrderlast.Receipt_entity
    from
    group3 as group3
    inner join (select 
                    rsOrder2.Order_id
                    , rlogs.Receipt_id
                    , concat('Receipt ',
                            rlogs.Number,
                            ' from ',
                            to_char(rlogs.Timestamp, 'DD.MM.YYYY'),
                            ' ',
                            to_char(rlogs.Timestamp, 'HH24:MI:SS')
                            ) as Receipt_entity 
                from 
                    main_op."Orders_logs" as rsOrder2
                    inner join 
                                (select 
                                    rsOrder1.Order_id
                                    , rsOrder1.Status_type
                                    , max(rsOrder1.timestamp) as "timestamp"
                                from 
                                    main_op."Orders_logs" as rsOrder1
                                    inner join group3 as group3
                                        on group3.Order_id = rsOrder1.Order_id
                                where 
                                    rsOrder1."Status_type" = 'b3b54b5d-6f6e-4699-a136-bc9ea1695ed6'	--Receipt id
                                group by rsOrder1.Order_id, rsOrder1.Status_type
                                ) as rsOrderlast1
                        on rsOrder2.Order_id = rsOrderlast1.Order_id
                            and rsOrder2.Status_type = rsOrderlast1.Status_type
                            and rsOrder2.timestamp = rsOrderlast1.timestamp
                    inner join "main"."Receipt_logs" as rlogs 
                        on rlogs.Receipt_id = rsOrder2.Status
                            and rlogs.Active_mode = 1
                            and rlogs.Reserve_mode = 1
                ) as rsOrderlast
        on group3.Order_id = rsOrderlast.Order_id
    where
        group3.Order_id not in
                                (select 
                                    group3check.Order_id
                                from
                                    group3check
                                )
    distributed randomly
    ;

    select 
        Results_table.*
        , rsOrderlast2.Product_name
    from
        Results_table as Results_table
            left join (select 
                            rsOrder2.Order_id
                            , rsOrder2.Status
                            , Products.Product_name

                        from 
                            main_op."Orders_logs" as rsOrder2
                            inner join 
                                        (select 
                                            rsOrder3.Order_id
                                            , rsOrder3.Status_type
                                            , max(rsOrder3.timestamp) as "timestamp"
                                        from 
                                            main_op."Orders_logs" as rsOrder3
                                            inner join Results_table as Results_table
                                                on Results_table.Order_id = rsOrder3.Order_id
                                        where 
                                            rsOrder3."Status_type" = '7ee8d253-dce3-4bab-be84-6aea20870fb9'	--Name of ordered product
                                        group by rsOrder3.Order_id, rsOrder3.Status_type
                                        ) as rsOrderlast3
                                on rsOrder2.Order_id = rsOrderlast3.Order_id
                                    and rsOrder2.Status_type = rsOrderlast3.Status_type
                                    and rsOrder2.timestamp = rsOrderlast3.timestamp
                            inner join "main"."Products" as Products 
                                on Products.id = rsOrder2.Status
                        ) as rsOrderlast2
                on Results_table.Order_id = rsOrderlast2.Order_id
    order by "Group_id"
    ;
    """

    # Get a list of orders
    df=execute_GP(query)
    print('List of orders size:', df.shape)

    return df

def check_df(**context):
    df = context['task_instance'].xcom_pull('get_orders')

    query3 = """
        SELECT 
            t."Timestamp",
            t."Order_id",
            t."Status"
        FROM
            public."NotificationLogs" AS t
    """
    basedf = execute_patrakhin(query3)


    df["Order_id"] = df["Order_id"].astype(str)
    print("Table shape before editing control:", df.shape)
    df = (
        pd.merge(
            df,
            basedf,
            on=["Timestamp", "Order_id", "Status"],
            how="left",
            indicator=True,
        )
        .query('_merge!="both"')
        .drop(columns=("_merge"))
        .reset_index(drop=True)
    )

    print('Result table size:', df.shape)

    return df

def split_df(**context):
    df = context['task_instance'].xcom_pull('check_df')
    # Split into group of tasks
    df1 = df.loc[df['Group_id']==1].reset_index(drop=True)
    df2 = df.loc[df['Group_id']==2].reset_index(drop=True)
    df3 = df.loc[df['Group_id']==3].reset_index(drop=True)
    print('Control - sum of splited df equal original df shape:')
    if (int(df1.shape[0])+int(df2.shape[0])+int(df3.shape[0])) == int(df.shape[0]):
        print(True)
    else:
        print(False)

    # Assign an id to each unique client for further processing
    df1["Authorid"] = np.nan
    df2["Authorid"] = pd.factorize(df2["User_id"])[0]
    df3["Authorid"] = pd.factorize(df3["User_id"])[0]
    df2_author = (
        df2[["User_id", "User_name", "Authorid"]].drop_duplicates().reset_index(drop=True)
    )
    df3_author = (
        df3[["User_id", "User_name", "Authorid"]].drop_duplicates().reset_index(drop=True)
    )
    return [df1, df2, df3, df2_author, df3_author]

def check_df1(**context):
    df1 = context['task_instance'].xcom_pull('split_df')[0]
    # Checking the data in the table. If empty - all upstreamed tasks will be skipped
     
    if df1.shape[0] == 0:
        return False
    else:
        return True

def id1_send_messages(**context):
    df1 = context['task_instance'].xcom_pull('split_df')[0]    

    # Sending notifications for group id = 1
    engine = create_engine(PG_PRIM_ENGINE_PATR)

    for _ in range(len(df1)):

        if df1.shape[0] == 0:
            break

        name = str(df1["User_name"].loc[_])
        guid = str(df1["User_id"].loc[_])
        nameuuid = (
            "%s%s%s%s%s" % (guid[19:23], guid[24:], guid[14:18], guid[9:13], guid[:8])
        ).upper()
        recepient = getRecepients(name, nameuuid)

        link = str(df1["Order_entity"].loc[_])
        uuid = str(df1["Doc_type"].loc[_])
        order_link = getorder_linkLink(link, uuid)
        product_name = str(df1["Product_name"].loc[_])

        print(recepient)
        subject = "Required to provide data in the order"
        body = (
            "Hello, \n\nFor the order"
            + order_link
            + " status: **Need to provide data** was set. \nStatus set for product: "
            + product_name
            + "\nOpen the order using the link provided in this message and check what data or photo you need to provide. \n\nIf you believe that this letter was received in error, or you want to opt out of such notifications, please report it in the Skype chat available at the link: myskypelink.com \nMessage generation time: "
            + today
        )
        # print(subject, '\n', body, sep='')
        request = fillRequest(subject, body, recepient)
        # print(request)
        sendMessageNotepad(request)
        df1.loc[_:_].to_sql(
            name="NotificationLogs", 
            con=engine,
            if_exists="append",
            schema="public",
            index=False,
            method="multi",
            chunksize=1000,
            dtype={
                "Comment": String(2000),
            },
        )
        print("Document:", uuid, ". 1 value was inserted into patrakhin.NotificationLogs")
 

    engine.dispose()

    return('Messages for group id #1 has been successfully sent')

def check_df2(**context):
    df2 = context['task_instance'].xcom_pull('split_df')[1] 
    # Checking the data in the table. If empty - all upstreamed tasks will be skipped
     
    if df2.shape[0] == 0:
        return False
    else:
        return True

def id2_send_messages(**context):
    df2_author = context['task_instance'].xcom_pull('split_df')[3] 
    df2 = context['task_instance'].xcom_pull('split_df')[1]

    # Sending notifications for group id = 2

    engine = create_engine(PG_PRIM_ENGINE_PATR)

    for _ in range(len(df2_author)):

        print("Recipient #", _ + 1, ": ", str(df2_author["User_id"].loc[_]), sep="")
        name = str(df2_author["User_name"].loc[_])
        guid = str(df2_author["User_id"].loc[_])
        nameuuid = (
            "%s%s%s%s%s" % (guid[19:23], guid[24:], guid[14:18], guid[9:13], guid[:8])
        ).upper()
        recepient = getRecepients(name, nameuuid)
        print(recepient)

        subject = "Updating purchase statuses for an order"

        newdf = df2.loc[df2["Authorid"] == _].reset_index(drop=True)
        doclist = ""
        for x in range(len(newdf)):

            link = str(newdf["Order_entity"].loc[x])
            uuid = str(newdf["Doc_type"].loc[x])
            order_link = getorder_linkLink(link, uuid)
            product_name = str(newdf["Product_name"].loc[x])
            status = "**" + str(newdf["Status"].loc[x]) + "**"
            if str(newdf["Status"].loc[x]) == "Purchase denied":
                status = paint_red_text(status)
            doc = (
                str(x + 1)
                + ". Order: "
                + order_link
                + "\nProduct: "
                + product_name
                + "\nStatus set: "
                + status
                + "."
            )
            doclist = doclist + doc + "\n"
        body = (
            "Hello, \nAutomatic notification of purchase status updates for the following orders:\n"
            + doclist
            + "\n\nIf you believe that this letter was received in error, or you want to opt out of such notifications, please report it in the Skype chat available at the link: myskypelink.com \nMessage generation time: "
            + today
        )
        # print(subject, '\n', body, sep='')
        request = fillRequest(subject, body, recepient)
        # print(request)
        sendMessageNotepad(request)
        newdf.to_sql(
            name="NotificationLogs",
            con=engine,
            if_exists="append",
            schema="public",
            index=False,
            method="multi",
            chunksize=1000,
            dtype={
                "Комментарий": String(2000),
            },
        )
        print(str(len(newdf)), "values was inserted into patrakhin.NotificationLogs")


    engine.dispose()

    return('Messages for group id #2 has been successfully sent')

def check_df3(**context):
    df3 = context['task_instance'].xcom_pull('split_df')[2]
    # Checking the data in the table. If empty - all upstreamed tasks will be skipped
   
    if df3.shape[0] == 0:
        return False
    else:
        return True

def id3_send_messages(**context):
    df3_author = context['task_instance'].xcom_pull('split_df')[4] 
    df3 = context['task_instance'].xcom_pull('split_df')[2]

    # Sending notifications for group id = 2
    engine = create_engine(PG_PRIM_ENGINE_PATR)

    for _ in range(len(df3_author)):

        print("Recipient #", _ + 1, ": ", str(df3_author["User_id"].loc[_]), sep="")
        name = str(df3_author["User_name"].loc[_])
        guid = str(df3_author["User_id"].loc[_])
        nameuuid = (
            "%s%s%s%s%s" % (guid[19:23], guid[24:], guid[14:18], guid[9:13], guid[:8])
        ).upper()
        recepient = getRecepients(name, nameuuid)
        print(recepient)

        subject = "The goods arrived at the point of issue"

        newdf = df3.loc[df3["Authorid"] == _].reset_index(drop=True)
        doclist = ""
        for x in range(len(newdf)):

            link = str(newdf["Order_entity"].loc[x])
            uuid = str(newdf["Doc_type"].loc[x])
            order_link = getorder_linkLink(link, uuid)
            product_name = str(newdf["Product_name"].loc[x])
            move_link = str(newdf["Receipt"].loc[x])
            move_uuid = str(newdf["Receipt_id"].loc[x])
            move = getMoveLink(move_link, move_uuid)
            doc = (
                str(x + 1)
                + ". Order: "
                + order_link
                + "\nProduct: "
                + product_name
                + "\nReceipt: "
                + move
                + "."
            )
            doclist = doclist + doc + "\n"
        body = (
            "Hello, \nThe goods arrived at the point of issue for the following orders:\n"
            + doclist
            + "\n\nYou can come to the point of issue, provide a receipt and pick up the goods."
            + "\n\nIf you believe that this letter was received in error, or you want to opt out of such notifications, please report it in the Skype chat available at the link: myskypelink.com \nMessage generation time: "
            + today
        )
        # print(subject, '\n', body, sep='')
        request = fillRequest(subject, body, recepient)
        print(request)
        sendMessageNotepad(request)
        newdf.to_sql(
            name="NotificationLogs",
            con=engine,
            if_exists="append",
            schema="public",
            index=False,
            method="multi",
            chunksize=1000,
            dtype={
                "Комментарий": String(2000),
            },
        )
        print(str(len(newdf)), "values was inserted into patrakhin.NotificationLogs")


    engine.dispose()
    
    return('Messages for group id #3 has been successfully sent. \n The program has done.')


_get_orders = PythonOperator(
    task_id="get_orders", provide_context=True, python_callable=get_orders, dag=dag
)

_check_df = PythonOperator(
    task_id="check_df", provide_context=True, python_callable=check_df, dag=dag
)

_split_df = PythonOperator(
    task_id="split_df", provide_context=True, python_callable=split_df, dag=dag
)

_check_df1 = ShortCircuitOperator(
    task_id="check_df1", python_callable=check_df1, dag=dag
)

_id1_send_messages = PythonOperator(
    task_id="id1_send_messages",
    provide_context=True,
    python_callable=id1_send_messages,
    dag=dag,
)

_check_df2 = ShortCircuitOperator(
    task_id="check_df2", python_callable=check_df2, dag=dag
)

_id2_send_messages = PythonOperator(
    task_id="id2_send_messages",
    provide_context=True,
    python_callable=id2_send_messages,
    dag=dag,
)

_check_df3 = ShortCircuitOperator(
    task_id="check_df3", python_callable=check_df3, dag=dag
)

_id3_send_messages = PythonOperator(
    task_id="id3_send_messages",
    provide_context=True,
    python_callable=id3_send_messages,
    dag=dag,
)
                         
_get_orders >> _check_df >> _split_df
_split_df >> _check_df1 >> _id1_send_messages
_split_df >> _check_df2 >> _id2_send_messages
_split_df >> _check_df3 >> _id3_send_messages


