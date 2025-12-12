CREATE schema IF NOT EXISTS raw;
CREATE schema IF NOT EXISTS stg;
CREATE schema IF NOT EXISTS dwh;


CREATE TABLE IF NOT EXISTS raw.raw_crime (
    dr_no              INTEGER,
    date_occ           DATE,
    time_occ           INTEGER,
    area               INTEGER,
    area_name          TEXT,
    crm_cd             INTEGER,
    crm_cd_desc        TEXT,
    vict_age           INTEGER,
    vict_sex           TEXT,
    status             TEXT,
    status_desc        TEXT
);

