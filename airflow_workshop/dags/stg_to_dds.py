import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def directions ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.directions  restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.directions (direction_id, direction_code, direction_name, valid_from, valid_to, is_current)
    select distinct direction_id::integer, direction_code, direction_name, valid_from, valid_to, is_current
    from stg.up_description
    """)

def levels ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.levels  restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.levels (training_period, level_name, valid_from, valid_to, is_current)
    WITH t AS (
        SELECT DISTINCT training_period, valid_from, valid_to, is_current
        FROM stg.up_description 
        WHERE is_current = TRUE
    )
    SELECT training_period, 
        CASE 
            WHEN training_period = '2' THEN 'магистратура' 
            WHEN training_period = '4' THEN 'бакалавриат' 
            ELSE 'специалитет'
        END AS level_name,
        valid_from,
        valid_to,
        is_current
    FROM t
    """)

def editors ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.editors  restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.editors (id, username, first_name, last_name, email, isu_number, valid_from, valid_to, is_current)
    SELECT DISTINCT 
        (json_array_elements(wp_list::json->'editors')::json->>'id')::integer AS editor_id, 
        (json_array_elements(wp_list::json->'editors')::json->>'username') AS username,
        (json_array_elements(wp_list::json->'editors')::json->>'first_name') AS first_name,
        (json_array_elements(wp_list::json->'editors')::json->>'last_name') AS last_name,
        (json_array_elements(wp_list::json->'editors')::json->>'email') AS email,
        (json_array_elements(wp_list::json->'editors')::json->>'isu_number') AS isu_number,
        valid_from,
        valid_to,
        is_current
    FROM stg.su_wp 
    WHERE is_current = TRUE
    ON CONFLICT DO NOTHING
    """)

def states ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.states restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.states (cop_state, state_name, valid_from, valid_to, is_current)
    WITH t AS (
        SELECT DISTINCT 
            (json_array_elements(wp_in_academic_plan::json)->>'status') AS cop_states,
            valid_from,
            valid_to,
            is_current
        FROM stg.work_programs 
        WHERE is_current = TRUE
    )
    SELECT cop_states, 
        CASE 
            WHEN cop_states = 'AC' THEN 'одобрено' 
            WHEN cop_states = 'AR' THEN 'архив'
            WHEN cop_states = 'EX' THEN 'на экспертизе'
            WHEN cop_states = 'RE' THEN 'на доработке'
            ELSE 'в работе'
        END AS state_name,
        valid_from,
        valid_to,
        is_current
    FROM t
    """)

def units ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.units restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.units
    SELECT DISTINCT
        sw.fak_id,
        sw.fak_title,
        ud.faculty_id::integer,
        MAX(sw.valid_from) OVER (PARTITION BY sw.fak_title) AS valid_from,
        MIN(sw.valid_to) OVER (PARTITION BY sw.fak_title) AS valid_to,
        sw.is_current AND ud.is_current AS is_current
    FROM stg.su_wp sw 
    LEFT JOIN stg.up_description ud ON sw.fak_title = ud.faculty_name 
    WHERE sw.is_current = TRUE AND ud.is_current = TRUE
    """)

def up ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.up restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.up
    SELECT DISTINCT *
    FROM (
        SELECT ud.id, 
            ud.plan_type, 
            d.direction_id,
            ud.ns_id::integer,
            ud.edu_program_id::integer,
            ud.edu_program_name,
            u.id as unit_id,
            l.id as level_id,
            ud.university_partner, 
            ud.up_country, 
            ud.lang, 
            ud.military_department, 
            ud.selection_year::integer,
            ud.valid_from,
            ud.valid_to,
            ud.is_current
        from stg.up_description ud 
        left join dds.directions d on d.direction_code = ud.direction_code 
        left join dds.units u on u.unit_title  = ud.faculty_name 
        left join dds.levels l on ud.training_period = l.training_period 
    ) AS newT
    """)

def wp ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.wp restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.wp
    with wp_desc as (
        select distinct
            json_array_elements(wp_in_academic_plan::json)->>'id' as wp_id,
            json_array_elements(wp_in_academic_plan::json)->>'discipline_code' as discipline_code,
            json_array_elements(wp_in_academic_plan::json)->>'description' as wp_description,
            json_array_elements(wp_in_academic_plan::json)->>'status' as wp_status,
            valid_from,
            valid_to,
            is_current
        from stg.work_programs
    ),
    wp_unit as (
        select distinct
            fak_id,
            wp_list::json->>'id' as wp_id,
            wp_list::json->>'title' as wp_title,
            wp_list::json->>'discipline_code' as discipline_code,
            valid_from,
            valid_to,
            is_current
        from stg.su_wp
    ),
    wp_combined AS (
        SELECT DISTINCT
            wp_desc.wp_id::integer AS wp_id,
            wp_desc.discipline_code,
            wp_unit.wp_title,
            s.id AS wp_status,
            wp_unit.fak_id AS unit_id,
            wp_desc.wp_description,
            MAX(wp_desc.valid_from) OVER (PARTITION BY wp_desc.discipline_code) AS valid_from,
            MIN(wp_desc.valid_to) OVER (PARTITION BY wp_desc.discipline_code) AS valid_to,
            wp_desc.is_current AND wp_unit.is_current AS is_current,
            ROW_NUMBER() OVER (
                PARTITION BY wp_desc.wp_id, wp_desc.discipline_code, wp_desc.wp_description, wp_unit.fak_id
                ORDER BY wp_unit.wp_title
            ) AS row_num
        FROM wp_desc
        LEFT JOIN wp_unit ON wp_desc.discipline_code = wp_unit.discipline_code
        LEFT JOIN dds.states s ON wp_desc.wp_status = s.cop_state
    )
    SELECT 
        wp_id, 
        discipline_code, 
        wp_title, 
        wp_status, 
        unit_id, 
        wp_description
    FROM wp_combined JOIN dds.units ON unit_id = dds.units.id
    WHERE row_num = 1
    """)

def wp_inter ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.wp_editor restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.wp_up restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.wp_editor (wp_id, editor_id)
    select (wp_list::json->>'id')::integer as wp_id,
        (json_array_elements(wp_list::json->'editors')::json->>'id')::integer as editor_id
    from stg.su_wp
    WHERE (wp_list::json->>'id')::integer IN (SELECT wp_id FROM dds.wp)
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.wp_up (wp_id, up_id)
    with t as (
    select id,
        (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as wp_id,
        (json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id')::integer AS up_id
    from stg.work_programs wp)
    select DISTINCT t.wp_id, up_id from t
    JOIN stg.work_programs wp ON t.id = wp.id
    WHERE t.wp_id IN (SELECT wp_id FROM dds.wp) 
    AND up_id IS NOT NULL
    AND up_id IN (SELECT id FROM dds.up)
    """)


def wp_markup ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate dds.wp_markup restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.wp_markup (id, title, discipline_code, prerequisites, outcomes, prerequisites_cnt, outcomes_cnt)
    SELECT 
        id,
        title,
        discipline_code,
        prerequisites,
        outcomes,
        json_array_length(prerequisites::json) AS prerequisites_cnt,
        json_array_length(outcomes::json) AS outcomes_cnt
    FROM stg.wp_markup
    WHERE is_current = TRUE
    """)


with DAG(dag_id='stg_to_dds', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 4 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='directions',
    python_callable=directions 
    ) 
    t2 = PythonOperator(
    task_id='levels',
    python_callable=levels 
    ) 
    t3 = PythonOperator(
    task_id='editors',
    python_callable=editors 
    ) 
    t4 = PythonOperator(
    task_id='states',
    python_callable=states 
    ) 
    t5 = PythonOperator(
    task_id='units',
    python_callable=units 
    ) 
    t6 = PythonOperator(
    task_id='up',
    python_callable=up 
    ) 
    t7 = PythonOperator(
    task_id='wp',
    python_callable=wp 
    ) 
    t8 = PythonOperator(
    task_id='wp_inter',
    python_callable=wp_inter 
    ) 
    t9 = PythonOperator(
    task_id='wp_markup',
    python_callable=wp_markup 
    ) 

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9