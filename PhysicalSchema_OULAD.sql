CREATE SCHEMA oulad;
USE oulad;

-- ---------- 1. DIMENSIONS ----------
-- 1.1 Module-presentation (“course offering”)
CREATE TABLE IF NOT EXISTS courses (
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    length INT,
    PRIMARY KEY (code_module, code_presentation)
);

-- 1.2 Student
CREATE TABLE IF NOT EXISTS student_info (
    id_student INT,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    gender VARCHAR(10),
    region VARCHAR(100),
    highest_education VARCHAR(100),
    imd_band VARCHAR(20),
    age_band VARCHAR(20),
    num_of_prev_attempts INT,
    studied_credits INT,
    disability VARCHAR(20),
    final_result VARCHAR(20),
    PRIMARY KEY (id_student, code_module, code_presentation),
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
        ON DELETE CASCADE
);


-- 1.3 Assessment (TMA/CMA/Exam)
CREATE TABLE IF NOT EXISTS assessments (
    id_assessment INT PRIMARY KEY,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    assessment_type VARCHAR(20),
    date INT,
    weight INT,
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
        ON DELETE CASCADE
);


-- 1.4 VLE material

CREATE TABLE IF NOT EXISTS vle (
    id_site INT PRIMARY KEY,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    activity_type VARCHAR(50),
    week_from INT,
    week_to INT,
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
        ON DELETE CASCADE
);


-- 1.5 Lookup tables requested in the rubric (“Full Domain”)
CREATE TABLE assessment_type (
    type_id     SMALLINT PRIMARY KEY,
    type_label  VARCHAR(10) UNIQUE
);


CREATE TABLE activity_type (
    type_id        SMALLINT PRIMARY KEY,
    type_label     VARCHAR(40) UNIQUE
);

CREATE TABLE gender_domain (
    gender VARCHAR(10) PRIMARY KEY
);



-- ---------- 2. FACTS ----------
-- 2.1 Registration timeline
CREATE TABLE IF NOT EXISTS student_registration (
    id_student INT,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    date_registration INT,
    date_unregistration INT,
    PRIMARY KEY (id_student, code_module, code_presentation),
    FOREIGN KEY (id_student, code_module, code_presentation)
        REFERENCES student_info(id_student, code_module, code_presentation)
        ON DELETE CASCADE
);


-- 2.2 Assessment results
CREATE TABLE IF NOT EXISTS student_assessment (
    id_assessment INT,
    id_student INT,
    date_submitted INT,
    is_banked BOOLEAN,
    score FLOAT,
    PRIMARY KEY (id_assessment, id_student),
    FOREIGN KEY (id_assessment)
        REFERENCES assessments(id_assessment)
        ON DELETE CASCADE
);


-- 2.3 VLE interactions (can be huge → partition later)
CREATE TABLE IF NOT EXISTS student_vle (
    id_student INT,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    id_site INT,
    date INT,
    sum_click INT,
    PRIMARY KEY (id_student, code_module, code_presentation, id_site, date),
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation),
    FOREIGN KEY (id_site)
        REFERENCES vle(id_site)
);

-- Agregar constraints --

ALTER TABLE student_info
ADD CONSTRAINT fk_gender
FOREIGN KEY (gender) REFERENCES gender_domain(gender);
