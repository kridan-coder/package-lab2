-- Таблицы

-- Учебный план
CREATE TABLE cdm.educational_program (
    id INTEGER PRIMARY KEY,
    is_checked BOOLEAN, -- Есть ли статус "одобрен"
    selection_year INTEGER, -- Год набора
    laboriousness INTEGER, -- Трудоемкость
    qualification TEXT -- Уровень образования (бакалавр, специалист, магистр)
);

-- Дисциплина
CREATE TABLE cdm.disciplines (
    id INTEGER PRIMARY KEY,
    is_checked BOOLEAN, -- Есть ли статус "одобрена"
    discipline_name TEXT, -- Название дисциплины
    unit_name TEXT, -- Название структурного подразделения, связанного с дисциплиной
    annotated BOOLEAN -- Есть ли аннотация для программы
);

-- Редактор
CREATE TABLE cdm.discipline_redactors (
    discipline_id INTEGER FOREIGN KEY, -- ID дисциплины, которую курирует редактор
    editor_name TEXT, -- Имя редактора
);

--******************--
-- Вставка данных

-- Учебный план
-- Придумать, как переводить up_details.check_status в integer
insert into cdm.educational_program
select up.id, up_details.check_status, up.selection_year, up_details.laboriousness, up_details.qualification
from dds.up as up join dds.up_details as up_details on up.id = up_details.id;

-- Дисциплина
insert into cdm.disciplines
select wp.id, wp.wp_status, wp.wp_title, unist.unit_title, wp.wp_description IS NULL OR wp.wp_description = ''
from dds.wp as wp join dds.units as units on wp.unit_id = units.id;

-- Редактор
insert into cdm.discipline_redactors
select wp_editor.wp_id, concat(editors.first_name, ' ', editors.last_name)
from dds.wp_editor as wp_editor join dds.editors as editors
       on dds.wp_editor.editor_id = dds.editors.id;
