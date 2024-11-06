create table dds.states(
    id serial primary key,
    cop_state varchar(2),
    state_name varchar(20),
    CONSTRAINT state_name_uindex UNIQUE (state_name),
);

create table dds.directions(
    id serial primary key,
    direction_id integer,
    direction_code varchar(10),
    direction_name varchar(100),
    CONSTRAINT direction_code_uindex UNIQUE (direction_code)
);

create table dds.levels(
    id serial primary key,
    training_period varchar(5),
    level_name varchar(20),
    CONSTRAINT level_name_uindex UNIQUE (level_name)
);

create table dds.editors(
    id integer primary key,
    username varchar(50),
    first_name varchar(50),
    last_name varchar(50),
    email varchar(50),
    isu_number varchar(6),
    CONSTRAINT editors_uindex UNIQUE (id)
);

create table dds.units(
    id integer,
    unit_title varchar(100),
    faculty_id integer,
    CONSTRAINT units_uindex UNIQUE (id)
);

create table dds.up(
    id integer primary key,
    plan_type varchar(8),
    direction_id integer,
    ns_id integer,
    edu_program_id integer,
    edu_program_name text,
    unit_id integer,
    level_id integer,
    university_partner text,
    up_country text,
    lang text,
    military_department
    boolean,
    selection_year integer
);

create table dds.wp(
    wp_id integer primary key,
    discipline_code text,
    wp_title text,
    wp_status integer FOREIGN KEY REFERENCES dds.states(id),
    unit_id integer FOREIGN KEY REFERENCES dds.units(id),
    wp_description text
);

create table dds.wp_editor(
    wp_id integer FOREIGN KEY REFERENCES dds.wp(wp_id),
    editor_id integer FOREIGN KEY REFERENCES dds.editors(id)
);

create table dds.wp_up(
    wp_id integer,
    up_id integer
);

create table dds.wp_markup(
    id integer,
    title text,
    discipline_code integer,
    prerequisites text,
    outcomes text,
    prerequisites_cnt smallint,
    outcomes_cnt smallint,
    CONSTRAINT wp_id_uindex UNIQUE (id)
);

create table dds.online_courses(
    id integer,
    title text,
    institution varchar (100),
    discipline_code integer
);
