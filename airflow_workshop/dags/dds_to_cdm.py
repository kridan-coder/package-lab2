import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def educational_programs ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO cdm.educational_programs (id, is_checked, selection_year, laboriousness, qualification)
    SELECT 
        id,
        CASE WHEN check_status IS NOT NULL AND check_status = 'verified' THEN TRUE ELSE FALSE
        END AS is_checked,
        selection_year,
        laboriousness,
        qualification
    FROM dds.up_details;
    """)

def disciplines ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO cdm.disciplines (id, is_annotated, is_checked, discipline_code, discipline_name, unit_id, unit_name)
    SELECT 
        wp.wp_id,
        CASE WHEN wp.wp_description IS NOT NULL THEN TRUE ELSE FALSE END AS is_annotated,
        CASE WHEN states.state_name IS NOT NULL AND states.state_name = 'одобрено' THEN TRUE ELSE FALSE END AS is_checked,
        wp.discipline_code::text,
        wp.wp_title AS discipline_name,
        wp.unit_id,
        units.unit_title AS unit_name
    FROM dds.wp AS wp
    LEFT JOIN dds.units ON wp.unit_id = units.id
    LEFT JOIN dds.states ON wp.wp_status = states.id
    ON CONFLICT (id) DO NOTHING;
    """)

def disciplines_redactors ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO cdm.discipline_redactors (discipline_id, redactor_id, editor_name)
    SELECT 
        wp_editor.wp_id as discipline_id,
        wp_editor.editor_id as redactor_id,
        CONCAT(editors.first_name, ' ', editors.last_name) AS editor_name
    FROM dds.wp_editor AS wp_editor
    LEFT JOIN dds.editors ON wp_editor.editor_id = editors.id
    ON CONFLICT (discipline_id, redactor_id) DO NOTHING;
    """)

with DAG(dag_id='dds_to_cmd', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 4 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='educational_programs',
    python_callable=educational_programs 
    ) 
    t2 = PythonOperator(
    task_id='disciplines',
    python_callable=disciplines 
    ) 
    t3 = PythonOperator(
    task_id='disciplines_redactors',
    python_callable=disciplines_redactors 
    )

t1 >> t2 >> t3