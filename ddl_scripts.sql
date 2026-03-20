---------- DIMENSION TABLES ----------

CREATE TABLE IF NOT EXISTS dim_date (
    date_skey            INT        NOT NULL,
    full_date            DATE       NOT NULL,
    day                  SMALLINT,
    month                SMALLINT,
    quarter              SMALLINT,
    year                 SMALLINT,
    day_of_week          SMALLINT,
    is_workday           BOOLEAN,
    reporting_month      DATE,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_skey)
)
USING DELTA
PARTITIONED BY (year);


CREATE TABLE IF NOT EXISTS dim_person (
    person_skey      BIGINT      NOT NULL,
    full_name        STRING,
    gender           STRING,
    date_of_birth    DATE,
    CONSTRAINT pk_dim_person PRIMARY KEY (person_skey)
)
USING DELTA;


CREATE TABLE IF NOT EXISTS dim_officer (
    officer_skey      BIGINT      NOT NULL,
    full_name         STRING,
    officer_cluster   STRING,
    officer_team      STRING,
    row_start_date    DATE        NOT NULL,
    row_end_date      DATE,
    CONSTRAINT pk_dim_officer PRIMARY KEY (officer_skey)
)
USING DELTA
PARTITIONED BY (officer_cluster, officer_team);


CREATE TABLE IF NOT EXISTS dim_case (
    case_skey                 BIGINT     NOT NULL,
    accused_skey              BIGINT     NOT NULL,
    case_no                   STRING     NOT NULL,
    directorate               STRING,
    CONSTRAINT pk_dim_case PRIMARY KEY (case_skey),
    CONSTRAINT fk_case_accused FOREIGN KEY (accused_skey)
        REFERENCES dim_person(person_skey)
)
USING DELTA
PARTITIONED BY (directorate);


------------ FACT TABLES -------------

CREATE TABLE IF NOT EXISTS fact_case_officer (
    case_skey                 BIGINT     NOT NULL,
    officer_skey              BIGINT     NOT NULL,
    assigned_from_date_key    INT        NOT NULL,
    assigned_to_date_key      INT,
    first_mention_date_skey   INT        NOT NULL,
    case_status               STRING,
    case_type                 STRING,
    first_mention_year        SMALLINT   NOT NULL,
    CONSTRAINT pk_fact_case PRIMARY KEY (case_skey, officer_skey, assigned_from_date_key),
    CONSTRAINT fk_case FOREIGN KEY (case_skey)
        REFERENCES dim_case(case_skey),
    CONSTRAINT fk_officer FOREIGN KEY (officer_skey)
        REFERENCES dim_officer(officer_skey),
    CONSTRAINT fk_case_assigned_from FOREIGN KEY (assigned_from_date_key)
        REFERENCES dim_date(date_skey),
    CONSTRAINT fk_case_assigned_to FOREIGN KEY (assigned_to_date_key)
        REFERENCES dim_date(date_skey),
    CONSTRAINT fk_case_first_mention FOREIGN KEY (first_mention_date_skey)
        REFERENCES dim_date(date_skey)
)
USING DELTA
PARTITIONED BY (first_mention_year);


CREATE TABLE IF NOT EXISTS fact_event_officer (
    event_skey                BIGINT     NOT NULL,
    case_skey                 BIGINT     NOT NULL,
    officer_skey              BIGINT,
    court_event_type          STRING,
    court_event_date_skey     INT        NOT NULL,
    court_event_hearing_days  INT,
    court_event_year          SMALLINT   NOT NULL,
    CONSTRAINT pk_fact_event PRIMARY KEY (event_skey),
    CONSTRAINT fk_event_case FOREIGN KEY (case_skey)
        REFERENCES dim_case(case_skey),
    CONSTRAINT fk_event_officer FOREIGN KEY (officer_skey)
        REFERENCES dim_officer(officer_skey),
    CONSTRAINT fk_event_date FOREIGN KEY (court_event_date)
        REFERENCES dim_date(date_skey)
)
USING DELTA
PARTITIONED BY (court_event_year);


CREATE TABLE IF NOT EXISTS fact_case_charge (
    charge_skey          BIGINT     NOT NULL,
    case_skey            BIGINT     NOT NULL,
    victim_skey          BIGINT,
    relation_to_accused  STRING,
    charge_status        STRING,
    committed_date_skey  INT        NOT NULL,
    offence_type         STRING,
    offence_group        STRING     NOT NULL,
    CONSTRAINT pk_fact_charge PRIMARY KEY (charge_skey),
    CONSTRAINT fk_charge_case FOREIGN KEY (case_skey)
        REFERENCES dim_case(case_skey),
    CONSTRAINT fk_charge_victim FOREIGN KEY (victim_skey)
        REFERENCES dim_person(person_skey),
    CONSTRAINT fk_charge_date FOREIGN KEY (committed_date_skey)
        REFERENCES dim_date(date_skey)
)
USING DELTA
PARTITIONED BY (offence_group);
