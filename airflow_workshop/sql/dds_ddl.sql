create schema dds;

create table dds.states(
    id serial primary key,
    cop_state varchar(2),
    state_name varchar(20),
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

create table dds.directions(
    id serial primary key,
    direction_id integer unique,
    direction_code varchar(10),
    direction_name varchar(100),
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

create table dds.levels(
    id serial primary key,
    training_period varchar(5),
    level_name varchar(20),
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

create table dds.editors(
    id serial primary key,
    username varchar(50),
    first_name varchar(50),
    last_name varchar(50),
    email varchar(50),
    isu_number varchar(6),
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

create table dds.units(
    id serial primary key,
    unit_title varchar(100),
    faculty_id integer,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

create table dds.up(
    id serial primary key,
    plan_type varchar(8),
    direction_id integer references dds.directions(direction_id),
    ns_id integer,
    edu_program_id integer,
    edu_program_name text,
    unit_id integer references dds.units(id),
    level_id integer references dds.levels(id),
    university_partner text,
    up_country text,
    lang text,
    military_department boolean,
    selection_year integer,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

create table dds.wp(
    wp_id serial primary key,
    discipline_code text,
    wp_title text,
    wp_status integer references dds.states(id),
    unit_id integer references dds.units(id),
    wp_description text,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

create table dds.wp_editor(
    wp_id integer references dds.wp(wp_id),
    editor_id integer references dds.editors(id)
);

create table dds.wp_up(
    wp_id integer references dds.wp(wp_id),
    up_id integer references dds.up(id)
);

CREATE TABLE dds.wp_markup (
    id serial primary key,
    title text,
    discipline_code integer,
    prerequisites text,
    outcomes text,
    prerequisites_cnt smallint,
    outcomes_cnt smallint
);
