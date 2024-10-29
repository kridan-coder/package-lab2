-- Таблицы

create table stg.work_programs (
    id integer, 
    academic_plan_in_field_of_study text, 
    wp_in_academic_plan text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

create table stg.up_description (
    id integer,
    plan_type text, 
    direction_id text, 
    ns_id text, 
    direction_code text, 
    direction_name text, 
    edu_program_id text, 
    edu_program_name text, 
    faculty_id text, 
    faculty_name text, 
    training_period text, 
    university_partner text, 
    up_country text, 
    lang text, 
    military_department boolean, 
    total_intensity text, 
    ognp_id text, 
    ognp_name text, 
    selection_year text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);
ALTER TABLE stg.up_description ADD CONSTRAINT up_description_uindex UNIQUE (id);

create table stg.su_wp (
    fak_id integer, 
    fak_title text, 
    wp_list text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

create table stg.wp_markup (
    id integer, 
    title text, 
    discipline_code integer, 
    prerequisites text, 
    outcomes text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);
ALTER TABLE stg.wp_markup ADD CONSTRAINT wp_id_uindex UNIQUE (id);

create table stg.online_courses (
    id integer, 
    institution text, 
    title text, 
    topic_with_online_course text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

create table stg.evaluation_tools (
    id integer, 
    type text, 
    "name" text, 
    description text, 
    check_point bool, 
    deadline integer, 
    semester integer, 
    "min" numeric, 
    "max" numeric, 
    descipline_sections text,
    evaluation_criteria text, 
    wp_id integer,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);
ALTER TABLE stg.evaluation_tools ADD CONSTRAINT et_id_uindex UNIQUE (id);

create table stg.disc_by_year (
    id integer, 
    ap_isu_id integer, 
    title text, 
    work_programs text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

create table stg.up_detail (
    id integer,
    ap_isu_id integer, 
    on_check varchar(20), 
    laboriousness integer, 
    academic_plan_in_field_of_study text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

create table stg.up_isu (
    id integer, 
    plan_type text, 
    direction_id text, 
    ns_id text, 
    direction_code text, 
    direction_name text, 
    edu_program_id text, 
    edu_program_name text, 
    faculty_id text, 
    faculty_name text,
    training_period text,
    university_partner text,
    up_country text, 
    lang text, 
    military_department boolean,
    total_intensity text, 
    ognp_id text, 
    ognp_name text, 
    selection_year text, 
    disciplines_blocks text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

create table stg.wp_detail (
    id integer, 
    discipline_code varchar(20), 
    title text, 
    description text, 
    structural_unit varchar(100), 
    prerequisites text, 
    discipline_sections text, 
    bibliographic_reference text, 
    outcomes text, 
    certification_evaluation_tools text, 
    expertise_status varchar(3),
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

create table stg.practice (
    id integer, 
    discipline_code varchar(20), 
    title text, 
    year text, 
    authors text, 
    op_leader text, 
    language varchar(20), 
    qualification text, 
    kind_of_practice text,
    type_of_practice text, 
    way_of_doing_practice text, 
    format_practice text, 
    features_content_and_internship text, 
    features_internship text, 
    additional_reporting_materials text, 
    form_of_certification_tools text,
    passed_great_mark text, 
    passed_good_mark text, 
    passed_satisfactorily_mark text, 
    not_passed_mark text, 
    evaluation_tools_current_control text,
    prac_isu_id text, 
    ze_v_sem varchar(30), 
    evaluation_tools_v_sem text, 
    number_of_semesters text, 
    practice_base varchar(20),  
    structural_unit varchar(30), 
    editors text, 
    bibliographic_reference text, 
    prerequisites text, 
    outcomes text,
    valid_from timestamp DEFAULT NOW(),
    valid_to timestamp DEFAULT '2999-12-31',
    is_current boolean DEFAULT TRUE
);

-- Триггеры

CREATE OR REPLACE FUNCTION scd2_work_programs_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.work_programs
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.work_programs (id, academic_plan_in_field_of_study, wp_in_academic_plan, valid_from, valid_to, is_current)
    VALUES (NEW.id, NEW.academic_plan_in_field_of_study, NEW.wp_in_academic_plan, NOW(), '2999-12-31', TRUE);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_work_programs
BEFORE UPDATE ON stg.work_programs
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_work_programs_trigger();

CREATE OR REPLACE FUNCTION scd2_up_description_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.up_description
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.up_description (
        id, plan_type, direction_id, ns_id, direction_code, direction_name,
        edu_program_id, edu_program_name, faculty_id, faculty_name,
        training_period, university_partner, up_country, lang, military_department,
        total_intensity, ognp_id, ognp_name, selection_year,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.plan_type, NEW.direction_id, NEW.ns_id, NEW.direction_code, NEW.direction_name,
        NEW.edu_program_id, NEW.edu_program_name, NEW.faculty_id, NEW.faculty_name,
        NEW.training_period, NEW.university_partner, NEW.up_country, NEW.lang, NEW.military_department,
        NEW.total_intensity, NEW.ognp_id, NEW.ognp_name, NEW.selection_year,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_up_description
BEFORE UPDATE ON stg.up_description
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_up_description_trigger();

CREATE OR REPLACE FUNCTION scd2_su_wp_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.su_wp
    SET valid_to = NOW(), is_current = FALSE
    WHERE fak_id = OLD.fak_id AND is_current = TRUE;

    INSERT INTO stg.su_wp (
        fak_id, fak_title, wp_list,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.fak_id, NEW.fak_title, NEW.wp_list,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_su_wp
BEFORE UPDATE ON stg.su_wp
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_su_wp_trigger();

CREATE OR REPLACE FUNCTION scd2_wp_markup_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.wp_markup
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.wp_markup (
        id, title, discipline_code, prerequisites, outcomes,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.title, NEW.discipline_code, NEW.prerequisites, NEW.outcomes,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_wp_markup
BEFORE UPDATE ON stg.wp_markup
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_wp_markup_trigger();

CREATE OR REPLACE FUNCTION scd2_online_courses_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.online_courses
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.online_courses (
        id, institution, title, topic_with_online_course,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.institution, NEW.title, NEW.topic_with_online_course,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_online_courses
BEFORE UPDATE ON stg.online_courses
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_online_courses_trigger();

CREATE OR REPLACE FUNCTION scd2_evaluation_tools_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.evaluation_tools
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.evaluation_tools (
        id, type, name, description, check_point, deadline, semester,
        min, max, descipline_sections, evaluation_criteria, wp_id,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.type, NEW.name, NEW.description, NEW.check_point, NEW.deadline, NEW.semester,
        NEW.min, NEW.max, NEW.descipline_sections, NEW.evaluation_criteria, NEW.wp_id,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_evaluation_tools
BEFORE UPDATE ON stg.evaluation_tools
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_evaluation_tools_trigger();

CREATE OR REPLACE FUNCTION scd2_disc_by_year_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.disc_by_year
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.disc_by_year (
        id, ap_isu_id, title, work_programs,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.ap_isu_id, NEW.title, NEW.work_programs,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_disc_by_year
BEFORE UPDATE ON stg.disc_by_year
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_disc_by_year_trigger();

CREATE OR REPLACE FUNCTION scd2_up_detail_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.up_detail
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.up_detail (
        id, ap_isu_id, on_check, laboriousness, academic_plan_in_field_of_study,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.ap_isu_id, NEW.on_check, NEW.laboriousness, NEW.academic_plan_in_field_of_study,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_up_detail
BEFORE UPDATE ON stg.up_detail
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_up_detail_trigger();

CREATE OR REPLACE FUNCTION scd2_up_isu_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.up_isu
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.up_isu (
        id, plan_type, direction_id, ns_id, direction_code, direction_name, 
        edu_program_id, edu_program_name, faculty_id, faculty_name, training_period, 
        university_partner, up_country, lang, military_department, total_intensity, 
        ognp_id, ognp_name, selection_year, disciplines_blocks,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.plan_type, NEW.direction_id, NEW.ns_id, NEW.direction_code, NEW.direction_name, 
        NEW.edu_program_id, NEW.edu_program_name, NEW.faculty_id, NEW.faculty_name, NEW.training_period, 
        NEW.university_partner, NEW.up_country, NEW.lang, NEW.military_department, NEW.total_intensity, 
        NEW.ognp_id, NEW.ognp_name, NEW.selection_year, NEW.disciplines_blocks,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_up_isu
BEFORE UPDATE ON stg.up_isu
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_up_isu_trigger();

CREATE OR REPLACE FUNCTION scd2_wp_detail_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.wp_detail
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.wp_detail (
        id, discipline_code, title, description, structural_unit, prerequisites, 
        discipline_sections, bibliographic_reference, outcomes, certification_evaluation_tools, expertise_status,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.discipline_code, NEW.title, NEW.description, NEW.structural_unit, NEW.prerequisites, 
        NEW.discipline_sections, NEW.bibliographic_reference, NEW.outcomes, NEW.certification_evaluation_tools, NEW.expertise_status,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_wp_detail
BEFORE UPDATE ON stg.wp_detail
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_wp_detail_trigger();

CREATE OR REPLACE FUNCTION scd2_practice_trigger()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE stg.practice
    SET valid_to = NOW(), is_current = FALSE
    WHERE id = OLD.id AND is_current = TRUE;

    INSERT INTO stg.practice (
        id, discipline_code, title, year, authors, op_leader, language, qualification, kind_of_practice, 
        type_of_practice, way_of_doing_practice, format_practice, features_content_and_internship, 
        features_internship, additional_reporting_materials, form_of_certification_tools,
        passed_great_mark, passed_good_mark, passed_satisfactorily_mark, not_passed_mark, 
        evaluation_tools_current_control, prac_isu_id, ze_v_sem, evaluation_tools_v_sem, 
        number_of_semesters, practice_base, structural_unit, editors, bibliographic_reference, 
        prerequisites, outcomes,
        valid_from, valid_to, is_current
    )
    VALUES (
        NEW.id, NEW.discipline_code, NEW.title, NEW.year, NEW.authors, NEW.op_leader, NEW.language, 
        NEW.qualification, NEW.kind_of_practice, NEW.type_of_practice, NEW.way_of_doing_practice, 
        NEW.format_practice, NEW.features_content_and_internship, NEW.features_internship, 
        NEW.additional_reporting_materials, NEW.form_of_certification_tools,
        NEW.passed_great_mark, NEW.passed_good_mark, NEW.passed_satisfactorily_mark, NEW.not_passed_mark, 
        NEW.evaluation_tools_current_control, NEW.prac_isu_id, NEW.ze_v_sem, NEW.evaluation_tools_v_sem, 
        NEW.number_of_semesters, NEW.practice_base, NEW.structural_unit, NEW.editors, 
        NEW.bibliographic_reference, NEW.prerequisites, NEW.outcomes,
        NOW(), '2999-12-31', TRUE
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_practice
BEFORE UPDATE ON stg.practice
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION scd2_practice_trigger();