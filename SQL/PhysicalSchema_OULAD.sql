CREATE SCHEMA IF NOT EXISTS oulad;
USE oulad;

-- ---------- 1. DOMAIN TABLES (Full Domain) ----------
-- 1.1 Gender domain
CREATE TABLE IF NOT EXISTS gender_domain (
    gender_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    gender VARCHAR(10) UNIQUE NOT NULL
);

-- 1.2 Region domain
CREATE TABLE IF NOT EXISTS region_domain (
    region_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    region VARCHAR(100) UNIQUE NOT NULL
);

-- 1.3 Education level domain
CREATE TABLE IF NOT EXISTS education_domain (
    education_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    highest_education VARCHAR(100) UNIQUE NOT NULL
);

-- 1.4 IMD band domain
CREATE TABLE IF NOT EXISTS imd_band_domain (
    imd_band_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    imd_band VARCHAR(20) UNIQUE NOT NULL
);

-- 1.5 Age band domain
CREATE TABLE IF NOT EXISTS age_band_domain (
    age_band_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    age_band VARCHAR(20) UNIQUE NOT NULL
);

-- 1.6 Disability domain
CREATE TABLE IF NOT EXISTS disability_domain (
    disability_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    disability VARCHAR(20) UNIQUE NOT NULL
);

-- 1.7 Final result domain
CREATE TABLE IF NOT EXISTS final_result_domain (
    final_result_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    final_result VARCHAR(20) UNIQUE NOT NULL
);

-- 1.8 Assessment type domain
CREATE TABLE IF NOT EXISTS assessment_type_domain (
    assessment_type_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    assessment_type VARCHAR(20) UNIQUE NOT NULL
);

-- 1.9 Activity type domain (VLE)
CREATE TABLE IF NOT EXISTS activity_type_domain (
    activity_type_id SMALLINT PRIMARY KEY AUTO_INCREMENT,
    activity_type VARCHAR(50) UNIQUE NOT NULL
);

-- ---------- 2. DIMENSIONS ----------
-- 2.1 Module-presentation ("course offering")
CREATE TABLE IF NOT EXISTS courses (
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    length INT,
    PRIMARY KEY (code_module, code_presentation)
);

-- 2.2 Student (with ordinal fields)
CREATE TABLE IF NOT EXISTS student_info (
    id_student INT,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    gender VARCHAR(10),
    gender_ordinal SMALLINT,
    region VARCHAR(100),
    region_ordinal SMALLINT,
    highest_education VARCHAR(100),
    education_ordinal SMALLINT,
    imd_band VARCHAR(20),
    imd_band_ordinal SMALLINT,
    age_band VARCHAR(20),
    age_band_ordinal SMALLINT,
    num_of_prev_attempts INT,
    studied_credits INT,
    disability VARCHAR(20),
    disability_ordinal SMALLINT,
    final_result VARCHAR(20),
    final_result_ordinal SMALLINT,
    PRIMARY KEY (id_student, code_module, code_presentation),
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
        ON DELETE CASCADE,
    FOREIGN KEY (gender_ordinal) REFERENCES gender_domain(gender_id),
    FOREIGN KEY (region_ordinal) REFERENCES region_domain(region_id),
    FOREIGN KEY (education_ordinal) REFERENCES education_domain(education_id),
    FOREIGN KEY (imd_band_ordinal) REFERENCES imd_band_domain(imd_band_id),
    FOREIGN KEY (age_band_ordinal) REFERENCES age_band_domain(age_band_id),
    FOREIGN KEY (disability_ordinal) REFERENCES disability_domain(disability_id),
    FOREIGN KEY (final_result_ordinal) REFERENCES final_result_domain(final_result_id)
);

-- 2.3 Assessment (TMA/CMA/Exam) with ordinal field
CREATE TABLE IF NOT EXISTS assessments (
    id_assessment INT PRIMARY KEY,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    assessment_type VARCHAR(20),
    assessment_type_ordinal SMALLINT,
    date INT,
    weight INT,
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
        ON DELETE CASCADE,
    FOREIGN KEY (assessment_type_ordinal) REFERENCES assessment_type_domain(assessment_type_id)
);

-- 2.4 VLE material with ordinal field
CREATE TABLE IF NOT EXISTS vle (
    id_site INT PRIMARY KEY,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    activity_type VARCHAR(50),
    activity_type_ordinal SMALLINT,
    week_from INT,
    week_to INT,
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
        ON DELETE CASCADE,
    FOREIGN KEY (activity_type_ordinal) REFERENCES activity_type_domain(activity_type_id)
);

-- ---------- 3. FACTS ----------
-- 3.1 Registration timeline
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

-- 3.2 Assessment results (FIXED: Added missing FKs)
CREATE TABLE IF NOT EXISTS student_assessment (
    id_assessment INT,
    id_student INT,
    date_submitted INT,
    is_banked BOOLEAN,
    score FLOAT,
    PRIMARY KEY (id_assessment, id_student),
    FOREIGN KEY (id_assessment)
        REFERENCES assessments(id_assessment)
        ON DELETE CASCADE,
    INDEX idx_student (id_student)
);

-- 3.3 VLE interactions (FIXED: Added FK to student_info)
CREATE TABLE IF NOT EXISTS student_vle (
    id_student INT,
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    id_site INT,
    date INT,
    sum_click INT,
    PRIMARY KEY (id_student, code_module, code_presentation, id_site, date),
    FOREIGN KEY (id_student, code_module, code_presentation)
        REFERENCES student_info(id_student, code_module, code_presentation)
        ON DELETE CASCADE,
    FOREIGN KEY (id_site)
        REFERENCES vle(id_site)
        ON DELETE CASCADE
);

-- ---------- 4. INDEXES FOR PERFORMANCE ----------
CREATE INDEX idx_assessment_module ON assessments(code_module, code_presentation);
CREATE INDEX idx_vle_module ON vle(code_module, code_presentation);
CREATE INDEX idx_student_result ON student_info(final_result);
CREATE INDEX idx_registration_dates ON student_registration(date_registration, date_unregistration);
CREATE INDEX idx_assessment_date ON student_assessment(date_submitted);
CREATE INDEX idx_vle_date ON student_vle(date);
