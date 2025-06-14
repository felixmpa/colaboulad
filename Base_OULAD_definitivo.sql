-- Creación del esquema (base de datos) oulad
CREATE SCHEMA IF NOT EXISTS oulad;
USE oulad;

-- ---------- 1. DIMENSIONES ----------

-- 1.1 Módulos y Presentaciones de Cursos ("course offering")
-- Esta tabla define las ofertas de cursos únicos por módulo y presentación.
CREATE TABLE IF NOT EXISTS courses (
    code_module VARCHAR(10) NOT NULL,
    code_presentation VARCHAR(10) NOT NULL,
    module_presentation_length INT, -- Duración en días de la presentación del módulo
    PRIMARY KEY (code_module, code_presentation)
);

-- 1.2 Tabla de Búsqueda para Tipos de Evaluación
-- Tabla separada para definir los tipos de evaluación posibles (TMA, CMA, Exam)
CREATE TABLE IF NOT EXISTS assessment_types (
    type_id SMALLINT AUTO_INCREMENT PRIMARY KEY,
    type_label VARCHAR(20) UNIQUE NOT NULL
);

INSERT INTO assessment_types (type_label) VALUES
('TMA'),   -- Asignación Marcada por el Tutor
('CMA'),   -- Asignación Marcada por Computadora
('Exam');  -- Examen Final


-- 1.3 Tabla de Búsqueda para Tipos de Actividad VLE
-- Tabla separada para definir los tipos de actividades en el Entorno Virtual de Aprendizaje
CREATE TABLE IF NOT EXISTS activity_types (
    type_id SMALLINT AUTO_INCREMENT PRIMARY KEY,
    type_label VARCHAR(50) UNIQUE NOT NULL
);

INSERT INTO activity_types (type_label) VALUES
('resource'),
('oucontent'),
('url'),
('homepage'),
('subpage'),
('glossary'),
('forumng'),
('oucollaborate'),
('dataplus'),
('quiz'),
('ouelluminate'),
('sharedsubpage'),
('questionnaire'),
('page'),
('externalquiz'),
('ouwiki'),
('dualpane'),
('repeatactivity'),
('folder'),
('htmlactivity')


-- 1.4 Tabla de Búsqueda para Género
-- Se mantiene la tabla de género para consistencia
CREATE TABLE IF NOT EXISTS gender_domain (
    gender VARCHAR(10) PRIMARY KEY NOT NULL
);

-- Insertar valores iniciales para género
INSERT INTO gender_domain (gender) VALUES ('M'), ('F') ON DUPLICATE KEY UPDATE gender=gender;



-- 1.5 Estudiantes (Información Demográfica)
-- Nueva tabla para almacenar información demográfica única de cada estudiante.
-- Esto reduce la redundancia si un estudiante se inscribe en múltiples cursos.
CREATE TABLE IF NOT EXISTS students (
    id_student INT PRIMARY KEY,
    gender VARCHAR(10),
    region VARCHAR(100),
    highest_education VARCHAR(100),
    imd_band VARCHAR(20),
    age_band VARCHAR(20),
    disability VARCHAR(20),
    FOREIGN KEY (gender) REFERENCES gender_domain(gender)
);
-----------------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS temp_student_info (
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    id_student INT,
    gender VARCHAR(10),
    region VARCHAR(100),
    highest_education VARCHAR(100),
    imd_band VARCHAR(20),
    age_band VARCHAR(20),
    num_of_prev_attempts INT,
    studied_credits INT,
    disability VARCHAR(20),
    final_result VARCHAR(20)
);

drop table  temp_student_info

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/studentInfo.csv'
INTO TABLE temp_student_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' -- O '\n' si tu CSV usa solo salto de línea LF
IGNORE 1 LINES; -- Ignorar la fila de encabezado

INSERT IGNORE INTO students (id_student, gender, region, highest_education, imd_band, age_band, disability)
SELECT DISTINCT
    id_student,
    gender,
    region,
    highest_education,
    imd_band,
    age_band,
    disability
FROM temp_student_info
WHERE id_student IS NOT NULL; -- Asegúrate de que no haya IDs nulos

-- Eliminar la tabla temporal
DROP TEMPORARY TABLE IF EXISTS temp_student_info;

-----------------------------------------------------------------------------

-- 1.6 Presentación del Estudiante (Inscripción en el Curso)
-- Esta tabla enlaza a los estudiantes con las presentaciones de los cursos
-- y contiene información específica de esa inscripción.
CREATE TABLE IF NOT EXISTS student_enrollments (
    code_module VARCHAR(10) NOT NULL,
    code_presentation VARCHAR(10) NOT NULL,
    id_student INT NOT NULL,
    num_of_prev_attempts INT,
    studied_credits INT,
    final_result VARCHAR(20),
    PRIMARY KEY (id_student, code_module, code_presentation),
    FOREIGN KEY (id_student) REFERENCES students(id_student) ON DELETE CASCADE,
    FOREIGN KEY (code_module, code_presentation) REFERENCES courses(code_module, code_presentation) ON DELETE CASCADE
);


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/studentInfo.csv'
INTO TABLE student_enrollments
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(code_module, code_presentation,id_student, @dummy_gender, @dummy_region,
 @dummy_highest_education, @dummy_imd_band, @dummy_age_band,
 num_of_prev_attempts, studied_credits, @dummy_disability, final_result);

-- 1.7 Evaluaciones (TMA/CMA/Examen)
-- Se actualiza para usar el 'type_id' de la tabla 'assessment_types'.
CREATE TABLE IF NOT EXISTS assessments (
    id_assessment INT PRIMARY KEY,
    code_module VARCHAR(10) NOT NULL,
    code_presentation VARCHAR(10) NOT NULL,
    assessment_type_id SMALLINT, -- Cambiado de VARCHAR a SMALLINT para FK
    date INT, -- La fecha relativa al inicio de la presentación del curso
    weight INT, -- Peso de la evaluación
    FOREIGN KEY (code_module, code_presentation) REFERENCES courses(code_module, code_presentation) ON DELETE CASCADE,
    FOREIGN KEY (assessment_type_id) REFERENCES assessment_types(type_id)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/assessments.csv'
INTO TABLE assessments
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' -- O '\n' si tu CSV usa solo salto de línea LF
IGNORE 1 LINES
(code_module, code_presentation,id_assessment, @assessment_type_label, @date_csv, weight)
SET
    assessment_type_id = (SELECT type_id FROM assessment_types WHERE type_label = @assessment_type_label),
    date = NULLIF(TRIM(REPLACE(REPLACE(REPLACE(@date_csv, '"', ''), '\r', ''), '\n', '')), '');



-- 1.8 Material VLE (Entorno Virtual de Aprendizaje)
-- Se actualiza para usar el 'type_id' de la tabla 'activity_types'.
CREATE TABLE IF NOT EXISTS vle (
    id_site INT NOT NULL,
    code_module VARCHAR(10) NOT NULL,
    code_presentation VARCHAR(10) NOT NULL,
    activity_type_id SMALLINT, -- Cambiado de VARCHAR a SMALLINT para FK
    week_from INT NULL,
    week_to INT NULL,
    PRIMARY KEY (id_site, code_module, code_presentation),
    FOREIGN KEY (code_module, code_presentation) REFERENCES courses(code_module, code_presentation) ON DELETE CASCADE,
    FOREIGN KEY (activity_type_id) REFERENCES activity_types(type_id)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/vle.csv'
INTO TABLE vle
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' -- O '\n' si tu CSV usa solo salto de línea LF
IGNORE 1 LINES
(id_site, code_module, code_presentation, @activity_type_label, @week_from, @week_to)
SET
    activity_type_id = (SELECT type_id FROM activity_types WHERE type_label = @activity_type_label),
    week_from = NULLIF(TRIM(REPLACE(REPLACE(REPLACE(@week_from, '"', ''), '\r', ''), '\n', '')), ''),
    week_to = NULLIF(TRIM(REPLACE(REPLACE(REPLACE(@week_to, '"', ''), '\r', ''), '\n', '')), '');



-- ---------- 2. HECHOS (FACTS) ----------

-- 2.1 Línea de tiempo de Registro de Estudiantes
-- Ahora referencia a 'student_enrollments' que contiene la clave primaria compuesta
CREATE TABLE IF NOT EXISTS student_registration (
    id_student INT NOT NULL,
    code_module VARCHAR(10) NOT NULL,
    code_presentation VARCHAR(10) NOT NULL,
    date_registration INT,
    date_unregistration INT,
    PRIMARY KEY (id_student, code_module, code_presentation),
    FOREIGN KEY (id_student, code_module, code_presentation)
        REFERENCES student_enrollments(id_student, code_module, code_presentation)
        ON DELETE CASCADE
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/studentRegistration.csv'
INTO TABLE student_registration
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' -- O '\n' si tu CSV usa solo salto de línea LF
IGNORE 1 LINES
(code_module, code_presentation,id_student, @date_registration, @date_unregistration)
SET
    -- Convierte cadenas vacías a NULL para columnas INT/DATE
    date_registration = NULLIF(TRIM(@date_registration), ''),
    date_unregistration = NULLIF(TRIM(@date_unregistration), '');

-- 2.2 Resultados de Evaluación de Estudiantes
-- Se mantiene la estructura ya que es adecuada para los resultados.
CREATE TABLE IF NOT EXISTS student_assessment (
    id_assessment INT NOT NULL,
    id_student INT NOT NULL,
    date_submitted INT,
    is_banked BOOLEAN,
    score FLOAT,
    PRIMARY KEY (id_assessment, id_student),
    FOREIGN KEY (id_assessment) REFERENCES assessments(id_assessment) ON DELETE CASCADE
    -- No se añade FK a student_enrollments aquí directamente porque id_student
    -- por sí solo no es una clave en student_enrollments.
    -- La relación se da a través de la id_assessment, la cual está ligada a un curso/presentación,
    -- y id_student que se vincula con la tabla 'students'.
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/studentAssessment.csv'
INTO TABLE student_assessment
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' -- O '\n' si tu CSV usa solo salto de línea LF
IGNORE 1 LINES
(id_assessment, id_student, @date_submitted_csv, @is_banked_csv, @score_csv) -- Lee los valores en variables temporales
SET
    date_submitted = NULLIF(TRIM(@date_submitted_csv), ''), -- Convierte cadena vacía a NULL para INT
    is_banked = CASE                                      -- Maneja la conversión para BOOLEAN
                    WHEN TRIM(@is_banked_csv) = '1' THEN TRUE
                    WHEN TRIM(@is_banked_csv) = '0' THEN FALSE
                    WHEN UPPER(TRIM(@is_banked_csv)) = 'TRUE' THEN TRUE
                    WHEN UPPER(TRIM(@is_banked_csv)) = 'FALSE' THEN FALSE
                    ELSE NULL -- Si no es 0, 1, TRUE, FALSE, inserta NULL
                END,
    score = NULLIF(TRIM(@score_csv), ''); -- Convierte cadena vacía a NULL para FLOAT, también maneja "Data truncated" si el problema es string vacío



-- 2.3 Interacciones VLE de Estudiantes
-- Se corrige la clave foránea a 'vle' para incluir todas las columnas de su clave primaria.
CREATE TABLE IF NOT EXISTS student_vle (
    code_module VARCHAR(10) NOT NULL,
    code_presentation VARCHAR(10) NOT NULL,
    id_student INT NOT NULL,
    id_site INT NOT NULL,
    date INT, -- Fecha de la interacción
    sum_click INT, -- Número de clics
    PRIMARY KEY (id_student, code_module, code_presentation, id_site, date),
    FOREIGN KEY (id_student) REFERENCES students(id_student) ON DELETE CASCADE, -- Ahora referencia a la tabla 'students'
    FOREIGN KEY (id_site, code_module, code_presentation) -- FK corregida para 'vle'
        REFERENCES vle(id_site, code_module, code_presentation)
        ON DELETE CASCADE
);

select count(*) from student_vle 

CREATE TEMPORARY TABLE IF NOT EXISTS temp_student_vle_raw (
    code_module VARCHAR(10),
    code_presentation VARCHAR(10),
    id_student INT,
    id_site INT,
    date INT,
    sum_click INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/studentVle.csv'
INTO TABLE temp_student_vle_raw
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;



INSERT INTO student_vle (id_student, code_module, code_presentation, id_site, date, sum_click)
SELECT
    id_student,
    code_module,
    code_presentation,
    id_site,
    date,
    SUM(sum_click) AS total_sum_click
FROM temp_student_vle_raw
GROUP BY
    id_student, code_module, code_presentation, id_site, date
ON DUPLICATE KEY UPDATE -- Si por alguna razón ya existe (menos común aquí), actualiza
    sum_click = VALUES(sum_click);

DROP TEMPORARY TABLE IF EXISTS temp_student_vle_raw;



