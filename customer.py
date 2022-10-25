from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

dag = DAG(
    dag_id = 'customer_360',
    start_date=days_ago(1)
)

sensor = HttpSensor(
    task_id = 'watch_orders',
    http_conn_id = 'order_s3',
    endpoint='orders.csv',
    response_check = lambda response: response.status_code == 200,
    dag=dag,
    retries = 20,
    retry_delay = timedelta(seconds=20)
)

download_orders = 'rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget https://trendytech-bigdata.s3.ap-south-1.amazonaws.com/orders.csv'


def fetch_customer_info():
    command_one= "hive -e 'DROP TABLE order_airflow.orders1'"
    command_two = "sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_export --username retail_user --password itversity --table order_airflow --num-mappers 1 --warehouse-dir /user/itv003829/project360/importedairflow/newdata --append"
    #command_three = "sqoop create-hive-table --connect jdbc:mysql://ms.itversity.com:3306/retail_export --username retail_user --password itversity --table order_airflow --hive-table /user/itv003829/warehouse/order_airflow/imported_orders"
    command_four = "sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user --password itversity --table customers --num-mappers 1 --warehouse-dir /user/itv003829/project360/importedcustomers --append"
    return f'{command_one} && {command_two} && {command_four}'

def get_order_filter():
    command_five = """sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_export --username retail_user --password itversity --table order_airflow --where "order_status in ('CLOSED')" --num-mappers 1 --warehouse-dir /user/itv003829/project360/importedclose --append"""
    return f'{command_five}'
    
import_orders_info = SSHOperator(
    task_id='download_orders',
    ssh_conn_id='itversity',
    command=fetch_customer_info(),
    dag = dag
)

filtered_order_info = SSHOperator(
    task_id = 'filtered_orders',
    ssh_conn_id='itversity',
    command=get_order_filter(),
    dag=dag
)


dummy = DummyOperator(
    task_id='dummy',
    dag = dag
)

sensor >> import_orders_info >> filtered_order_info >> dummy