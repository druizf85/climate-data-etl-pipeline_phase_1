# Temperatura Ambiente del Aire · Fase 1 — ETL Pipeline

> **Objetivo de la Fase 1**: construir un pipeline de datos **confiable, reproducible y eficiente** para integrar el dataset público *Temperatura Ambiente del Aire* (IDEAM, Colombia), consolidar el histórico **2005–2024** y habilitar una ingesta **diaria** desde **2025** en adelante. El resultado es una tabla única en PostgreSQL (`hist_temp`) lista para análisis y dashboards.

---

## 📌 Resumen del proyecto

* **Fuente**: API de Datos Abiertos de Colombia (IDEAM) — *Temperatura Ambiente del Aire*.
* **Periodo**: 2005-01-01 a la fecha.
* **Formato de respaldo**: Parquet (capas **raw** y **clean**).
* **Base de datos destino**: PostgreSQL.
* **Carga**: Bulk load vía `COPY` → *staging* temporal → *merge* a `hist_temp` con `ON CONFLICT DO NOTHING` para controlar duplicidad en la información.
* **Orquestación**: Airflow (DAG diario para ingesta incremental).
* **Trazabilidad**: batches con `batch` y backups por cortes de extracción.

---

## 🗂️ Estructura del repositorio

```
project/
├───data
│   ├───clean_data
│   │   ├───hist_data
│   │   └───new_data
│   └───raw_data
│       ├───hist_data
│       └───new_data
├───models
├───queries_src
│   ├───queries_hist_extraction
│   └───queries_new_extraction
├───src (paquetes de extracción, transformación y carga)

```

**Convenciones**

* `raw_*`: datos tal cual se extraen de la API (preprocesados, respaldo).
* `clean_*`: datos limpios y listos para carga (revisión de datos duplicados, revisión de datos nulos, inclusión de información, tipificación y derivaciones).
* `hist_*`: cortes históricos (2005–2024).
* `new_*`: ingestas diarias (2025–presente).

---

## 🔗 Fuente de datos

**Dataset**: *Temperatura Ambiente del Aire* — IDEAM (Datos Abiertos de Colombia).

* Medición **horaria** por estación y sensor en el territorio colombiano.
* Verificados bajo control de calidad básico (OMM).

> El proyecto conserva los archivos en Parquet y registra los cortes de extracción, manteniendo **respaldo** y **reproducibilidad**.

---

## 🧱 Esquema de la tabla destino (`hist_temp`)

```sql
CREATE TABLE IF NOT EXISTS hist_temp (
  identificador      VARCHAR(100) PRIMARY KEY, -- codigoestacion || '-' || codigosensor || '-' || fechaobservacion
  batch              VARCHAR(100) NOT NULL,    -- etiqueta del corte/ingesta
  codigoestacion     VARCHAR(100) NOT NULL,
  codigosensor       VARCHAR(100) NOT NULL,
  fechaobservacion   TIMESTAMP    NOT NULL,
  mesobservacion     INT          NOT NULL,
  valorobservado     FLOAT        NOT NULL,
  nombreestacion     VARCHAR(100) NOT NULL,
  departamento       VARCHAR(100) NOT NULL,
  municipio          VARCHAR(100) NOT NULL,
  zonahidrografica   VARCHAR(100) NOT NULL,
  latitud            FLOAT        NOT NULL,
  longitud           FLOAT        NOT NULL,
  descripcionsensor  VARCHAR(100) NOT NULL,
  unidadmedida       VARCHAR(100) NOT NULL
);
```

**identificador**: `identificador = codigoestacion + codigosensor + fechaobservacion`. Se controlan las inserciones con `ON CONFLICT (identificador) DO NOTHING`.

---

## ⚙️ Pipeline (Fase 1)

### 1) Extracción

* Histórico **2005–2024** (ejecución única) y extracción **incremental diaria** desde **2025-01-01**.
* Preprocesamiento mínimo:

  * Conversión de `fechaobservacion` a `TIMESTAMP`.
  * Conversión a `FLOAT` en `valorobservado`, `latitud`, `longitud`.
* Almacenamiento de backups en formato Parquet: `data/raw_data/{hist_data|new_data}`.

### 2) Transformación

* **Limpieza**: eliminación de duplicados y nulos.
* **Estandarización**: renombrado y tipificación de columnas.
* **Derivadas**: `mesobservacion`, `batch` (fecha/corte), `identificador` (PK).
* Almacenamiento de backups en formato Parquet: `data/clean_data/{hist_data|new_data}`.

### 3) Carga (PostgreSQL)

1. Exportar el DataFrame limpio a **CSV temporal**.
2. Crear **tabla temporal** (`tmp_hist_temp`) con la misma estructura que `hist_temp`.
3. Cargar CSV → `tmp_hist_temp` con **`COPY`** .
4. Insertar de `tmp_hist_temp` → `hist_temp` con **`ON CONFLICT (identificador) DO NOTHING`**.
5. Limpiar: `DROP` de la temporal, eliminar CSV temporal y cerrar conexiones.

---

## 📈 Volumen histórico (resumen de cortes cargados)

1. 2005-01-01 00:00:00 → 2008-12-31 23:00:00 — **2,494,861** registros
2. 2009-01-01 00:00:00 → 2013-12-31 23:45:00 — **7,407,674**
3. 2014-01-01 00:00:00 → 2015-12-31 23:45:00 — **3,971,491**
4. 2016-01-01 00:00:00 → 2017-12-31 23:45:00 — **11,317,877**
5. 2018-01-01 00:00:00 → 2018-12-31 23:45:00 — **8,671,255**
6. 2019-01-01 00:00:00 → 2019-12-31 23:45:00 — **13,205,603**
7. 2020-01-01 00:00:00 → 2020-12-31 23:45:00 — **9,632,775**
8. 2021-01-01 00:00:00 → 2021-12-31 23:45:00 — **6,324,941**
9. 2022-01-01 00:00:00 → 2023-12-31 23:44:00 — **9,888,260**
10. 2024-01-01 00:00:00 → 2024-12-31 23:44:00 — **6,443,780**

---

## 🧪 Calidad de datos y controles

* **PK** y **UNIQUE** implícito en `identificador` → evita duplicados en destino.
* **Null handling** en transformación (rechazo o imputación según regla).
* **Tipificación**: control de fechas formato estándar, unificación de datos numéricos.
* **Logs/estadísticas** por batch (filas en raw, filas insertadas, control de tiempos en extracción y en ejecución del dag).

---

## 🧭 Orquestación (Airflow)

* DAG diario: extrae **desde el último `batch`** hasta `now()-1 day`.
* Tareas: `extract_function` → `transform_function` → `load_function` 
* Reintentos, alertas y métricas de duración por tarea.

---

## 🚀 Ejecución

1. **Histórico**: ejecutar el flujo `hist` (una sola vez) hasta poblar `hist_temp`.
2. **Diario**: habilitar DAG incremental (agenda diaria; ventana desde el último `batch`).

---

## 🔧 Tecnología

* **Python** (extracción, transformación, utilidades).
* **DuckDB** (Parquet → Conversión a CSV para inserción de datos).
* **PostgreSQL** (tabla `hist_temp`, bulk load con `COPY`).
* **SQLAlchemy/psycopg2** (conexión y `raw_connection`).
* **Airflow** (orquestación de histórico e incremental).

---

## ❗ Limitaciones y notas a considerar

Se encontraron algunas limitantes en el proceso:

* Dependencia de disponibilidad y límites de la API de origen (1000 registros por consulta) periodos de mantenimiento de la plataforma, entre otros.
* Lecturas horarias: huecos posibles por caída de estaciones o API.


---

## 🗺️ Próximos pasos (Fase 2 y 3 — fuera de alcance de esta fase)

* **Fase 2 (dbt)**: tests (unique, not null, valores permitidos, transformaciones y vistas personalizadas), documentación.
* **Fase 3 (BI)**: dashboards en Power BI (tendencias, estacionalidad, mapas, anomalías) y KPIs por zona/estación.
