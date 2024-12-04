from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
with DAG(
    'import_db',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['import','db'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "err report"
            """,
            trigger_rule="one_failed"
    )
    task_check = BashOperator(
            task_id = 'check',
            bash_command = "bash {{ var.value.CHECK_SH }} {{ds_nodash}}"
            #bash_command = """
            #   echo "check"
            #   DONE_PATH=~/data/done/{{ds_nodash}}
            #   DONE_PATH_FILE="${DONE_PATH}/_DONE"

            #    if [ -e "$DONE_PATH_FILE" ]; then
            #        figlet "Let's move on"
            #        exit 0
            #    else
            #        echo "I'll be back => $DONE_PATH_FILE"
            #        exit 1
            #    fi
            #"""
    )
    task_csv = BashOperator(
            task_id = 'to.csv',
            bash_command = """
                echo "to.csv"
                COUNT_PATH=~/data/count/{{ds_nodash}}
                CSV_PATH=~/data/csv/{{ds_nodash}}
                mkdir -p $CSV_PATH
                #cat "${COUNT_PATH}/count.log" | awk '{print "{{ds}}," $2 "," $1}' > ${CSV_PATH}/csv.csv
                #cat "${COUNT_PATH}/count.log" | awk '{print "\\"{{ds}}\\",\\"" $2 "\\",\\"" $1 "\\""}' > ${CSV_PATH}/csv.csv
                cat "${COUNT_PATH}/count.log" | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_PATH}/csv.csv
            """                         
            )
    task_create_table = BashOperator(
            task_id = "create.table",
            bash_command = """
                SQL={{ var.value.SQL_PATH }}/create_db_table.sql
                echo "SQL_PATH=$SQL"
                MYSQL_PWD='{{ var.value.DB_PASSWD }}' mysql -u root < $SQL
            """
            )
    task_tmp = BashOperator(
            task_id = 'to.tmp',
            bash_command = """
                echo "to.tmp"
                CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv
                echo $CSV_FILE
                bash {{ var.value.SH_HOME }}/csv2sql.sh $CSV_FILE {{ ds }}
            """
            )
    task_base = BashOperator(
            task_id = 'to.base',
            bash_command = """
                echo "to.base"
                SQL={{ var.value.SQL_PATH }}/temp2base.sql
                echo "SQL_PATH=$SQL"
                MYSQL_PWD='{{ var.value.DB_PASSWD }}' mysql -u root < $SQL                
            """                                                                                 )
    task_make = BashOperator(
            task_id = 'make.done',
            bash_command = """
                echo "make.done"
            """                                                                                 )
    task_start >> task_check >> task_csv
    task_csv >> task_create_table >> task_tmp >> task_base >> task_make >> task_end
    task_check >> task_err >> task_end
