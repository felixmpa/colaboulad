# OULAD ETL System

Sistema ETL para el dataset Open University Learning Analytics Dataset (OULAD).

## Descripción del Proyecto

Caso práctico 3 [COLABORATIVO]: Exporte el modelo del dataset OULAD a MySQL, PostgreSQL o MS SQL Server: Cleaning, PK, FK and FullDomain y haga un ETL, EDA extendido en Python.

Este proyecto implementa:
- Schema SQL con integridad referencial completa (PK/FK)
- Campos ordinales para categorías alfanuméricas
- Tablas de dominio (Full Domain) para todas las categorías
- Limpieza y validación de datos
- Proceso ETL automatizado

## Requisitos

- Python 3.8+
- MySQL 8.0+
- Docker y Docker Compose (opcional)

## Instalación y Configuración

### Paso 1 - Configurar Credenciales

Crear una copia del archivo `.env.example` con tus credenciales:
```bash
cp .env.example .env
```

Editar `.env`:
```env
DB_HOST=localhost
DB_USER=your_username
DB_PORT=3306
DB_PASSWORD=your_password
DB_DATABASE=oulad
```

### Paso 2 - Configurar Entorno Python

Activar el entorno virtual (macOS/Linux):
```bash
python3 -m venv venv
source venv/bin/activate
```

Windows:
```bash
python -m venv venv
venv\Scripts\activate
```

Instalar dependencias:
```bash
pip install -r requirements.txt
```

### Paso 3 - Configurar Base de Datos

Opción A - Usar Docker (recomendado):
```bash
docker-compose up -d
```

Opción B - Usar MySQL existente:
- Asegurarse de que MySQL esté corriendo
- Actualizar credenciales en `.env`

### Paso 4 - Descargar Datasets OULAD

```bash
python Datasets/downloadDatasets.py Datasets/
```

Esto descargará y extraerá todos los archivos CSV necesarios.

## Uso del Sistema

Ejecutar el programa principal:
```bash
python main.py
```

### Opciones del Menú

1. **Ejecutar ETL**: Carga completa de datos en MySQL
   - Crea el schema y tablas
   - Carga tablas de dominio
   - Importa todos los datos con limpieza
   
2. **Ejecutar EDA**: Análisis exploratorio (próximamente)

3. **Verificar datasets**: Verifica integridad de archivos

## Estructura del Proyecto

```
colaboulad/
├── Datasets/                    # Datos CSV de OULAD
│   ├── DataDescription.md       # Descripción de los datasets
│   └── downloadDatasets.py      # Script de descarga
├── ETL/                        # Módulos del proceso ETL
│   ├── __init__.py
│   ├── database.py             # Conexión y operaciones MySQL
│   ├── data_cleaner.py         # Limpieza y validación de datos
│   └── etl_process.py          # Proceso ETL principal
├── SQL/                        # Scripts SQL
│   └── PhysicalSchema_OULAD.sql # Schema completo de la BD
├── .env.example                # Plantilla de configuración
├── docker-compose.yaml         # Configuración Docker para MySQL
├── main.py                     # Punto de entrada principal
├── requirements.txt            # Dependencias Python
└── README.md                   # Este archivo
```

## Schema de Base de Datos

### Tablas de Dominio (Full Domain)
- `gender_domain`: Valores de género
- `region_domain`: Regiones geográficas
- `education_domain`: Niveles educativos
- `imd_band_domain`: Bandas de privación múltiple
- `age_band_domain`: Rangos de edad
- `disability_domain`: Estados de discapacidad
- `final_result_domain`: Resultados finales
- `assessment_type_domain`: Tipos de evaluación
- `activity_type_domain`: Tipos de actividad VLE

### Tablas Principales

#### Dimensiones
- `courses`: Información de módulos y presentaciones
- `student_info`: Información demográfica con campos ordinales
- `assessments`: Evaluaciones con tipo ordinal
- `vle`: Materiales VLE con tipo de actividad ordinal

#### Hechos
- `student_registration`: Línea temporal de registro
- `student_assessment`: Resultados de evaluaciones
- `student_vle`: Interacciones con materiales VLE

## Características Implementadas

### 1. Integridad Referencial
- Todas las Primary Keys (PK) definidas
- Foreign Keys (FK) con ON DELETE CASCADE
- Índices para optimización de consultas

### 2. Campos Ordinales
- Conversión automática de categorías a valores numéricos
- Mapeo mediante tablas de dominio
- Preservación de valores originales

### 3. Limpieza de Datos
- Manejo de valores nulos
- Validación de rangos
- Normalización de formatos
- Agregación de duplicados

### 4. Performance
- Carga por lotes (batch inserts)
- Índices estratégicos
- Transacciones optimizadas

## Troubleshooting

### Error de conexión MySQL
- Verificar que MySQL esté corriendo
- Revisar credenciales en `.env`
- Comprobar puerto (default: 3306)

### Archivos CSV no encontrados
- Ejecutar script de descarga: `python Datasets/downloadDatasets.py Datasets/`
- Verificar conexión a internet

### Error de memoria en carga
- El dataset studentVle es muy grande (>10M registros)
- Se procesa en lotes automáticamente
- Considerar aumentar memoria de MySQL si es necesario

## Próximas Mejoras

- [ ] Implementar EDA (Análisis Exploratorio)
- [ ] Agregar soporte para PostgreSQL
- [ ] Dashboard de visualización
- [ ] Optimización de consultas analíticas