-- Учебный план
CREATE TABLE cdm.educational_programs (
    id INTEGER,
    is_checked BOOLEAN, -- Есть ли статус "одобрен"
    selection_year INTEGER, -- Год набора
    laboriousness INTEGER, -- Трудоемкость
    qualification TEXT -- Уровень образования (бакалавр, магистр)
);
ALTER TABLE cdm.educational_programs ADD CONSTRAINT educational_programs_id_unique UNIQUE (id);


-- Дисциплина
CREATE TABLE cdm.disciplines (
    id INTEGER,
    is_annotated BOOLEAN,
    is_checked BOOLEAN, -- Есть ли статус "одобрена"
    discipline_code TEXT,
    discipline_name TEXT,
    unit_id INTEGER, -- ID структурного подразделения, связанного с дисциплиной
    unit_name TEXT
);
ALTER TABLE cdm.disciplines ADD CONSTRAINT disciplines_id_unique UNIQUE (id);


-- Редактор
CREATE TABLE cdm.discipline_redactors (
    redactor_id INTEGER,
    discipline_id INTEGER,
    editor_name TEXT,
    UNIQUE (redactor_id, discipline_id)
);

CREATE TABLE cdm.up_wp_statuses (
    edu_program_name TEXT,
    selection_year INTEGER,
    unit_title TEXT,
    level_name TEXT,
    up_id INTEGER,
    state_name TEXT,
    state_count INTEGER,
    annotated INTEGER
);

INSERT INTO cdm.up_wp_statuses (edu_program_name, selection_year, unit_title, level_name, up_id, state_name, state_count, annotated)
WITH t AS (
    SELECT u.edu_program_name,
           u.selection_year,
           u2.unit_title, 
           l.level_name,
           u.id AS up_id,
           wu.wp_id 
    FROM dds.up u
    JOIN dds.levels l ON u.level_id = l.id 
    JOIN dds.wp_up wu ON wu.up_id = u.id
    LEFT JOIN dds.units u2 ON u2.id = u.unit_id
),
t2 AS (
    SELECT t.edu_program_name, 
           t.selection_year,
           t.unit_title,
           t.level_name,
           t.up_id,
           w.discipline_code,
           w.wp_title,
           CASE WHEN w.wp_description IS NULL THEN 0 ELSE 1 END AS description_flag,
           s.state_name 
    FROM t
    JOIN dds.wp w ON t.wp_id = w.wp_id
    JOIN dds.states s ON w.wp_status = s.id
),
t3 AS (
    SELECT edu_program_name,
           selection_year::INTEGER,
           unit_title,
           level_name,
           up_id,
           state_name,
           COUNT(DISTINCT discipline_code) AS state_count,
           SUM(description_flag) AS annotated
    FROM t2
    GROUP BY edu_program_name, selection_year, unit_title, level_name, up_id, state_name
)
SELECT * FROM t3;

 

CREATE TABLE cdm.su_wp_statuses (
    wp_id INTEGER,
    discipline_code INTEGER,
    wp_title TEXT,
    state_name TEXT,
    unit_title TEXT,
    description_flag SMALLINT,
    number_od_editors INTEGER,
    editor_names TEXT
);

INSERT INTO cdm.su_wp_statuses (
    wp_id,
    discipline_code,
    wp_title,
    state_name,
    unit_title,
    description_flag,
    number_od_editors,
    editor_names
)
WITH t AS (
    SELECT 
		wp.wp_id,
		CAST(wp.discipline_code AS INTEGER) AS discipline_code,
		wp.wp_title,
		s.state_name,
		u2.unit_title,
		CASE WHEN wp.wp_description IS NULL THEN 0 ELSE 1 END AS description_flag,
		ed.first_name || ' ' || last_name  as editor_name
	FROM dds.wp wp
	JOIN dds.states s ON wp.wp_status = s.id
	LEFT JOIN dds.units u2 ON u2.id = wp.unit_id
	LEFT JOIN dds.wp_editor we ON wp.wp_id = we.wp_id
	LEFT JOIN dds.editors ed ON ed.id = we.editor_id
)
SELECT 
    wp_id,
    discipline_code,
    wp_title,
    state_name,
    unit_title,
    description_flag,
    COUNT(editor_name) AS number_od_editors,
    STRING_AGG(editor_name, ', ') AS editor_names
FROM t
GROUP BY 
    wp_id, 
    discipline_code, 
    wp_title, 
    state_name, 
    unit_title, 
    description_flag;

