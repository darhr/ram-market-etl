
# RAM Market ETL

Un pipeline ETL automatizado End-to-End para el monitoreo de precios de hardware de PC (kits de memoria RAM) en tiendas retail de tecnología peruanas (3 tiendas iniciales). El sistema ingesta datos crudos de e-commerce en un **Data Lake compatible con S3**, normaliza especificaciones técnicas mediante **fuzzy matching y expresiones regulares**, valida registros usando schemas de **Pydantic v2**, realiza seguimiento histórico de precios mediante **Slowly Changing Dimensions (SCD-2)** en **PostgreSQL**, y entrega insights accionables de mercado a través de un dashboard interactivo en **Power BI**. El dashboard muestra el funcionamiento del pipeline.

---

## Vista General de la Arquitectura

<img src="images/architecture.png" alt="Diagrama de Arquitectura" width=70%>

---

## Runs en prefect

<img src="images/prefect-last-run.png" alt="Ultima ejecucion en prefect" width=100%>
<img src="images/prefect-past-runs.png" alt="Historial de ejecuciones en prefect" width=100%>


---

## Retailers

|Retailer|
|--------|
|Compuvision|
|CyC|
|Sercoplus|

> Este proyecto es para fines demostrativos únicamente. No soy dueño de ninguno de los sitios web. Los datos recopilados son para uso personal y no están destinados a uso comercial.

---

## Arquitectura Medallón y Flujo de Datos

### 1. Capa Bronze (Data Lake Crudo)
- **Almacenamiento:** Cloudflare R2 (100% compatible con la API S3 vía `boto3`).
- **Formato:** Parquet columnar (motor `pyarrow`) con compresión.
- **Estrategia de Particionamiento:** Particionado por fecha y timestamp UTC:
  ```
  r2://ram-market-lake/
  └── bronze/
      └── raw_data/
          └── YYYY-MM-DD/
              └── YYYY-MM-DDTHH-MM-SSZ/
                  └── raw_ram_data.parquet
  ```
- **Bronze como Fuente de la Verdad:** Durante cada ejecución del ETL, los datos crudos se scrapean, se escriben inmediatamente en Parquet dentro de R2 y luego **se vuelven a leer desde R2** para su transformación. Esto garantiza la inmutabilidad de los datos crudos, proporciona un registro de auditoría completo y permite reejecutar transformaciones históricas sin necesidad de volver a scrapear las tiendas.

### 2. Capa Silver (Datos Limpios, Validados y SCD-2)
- **Almacenamiento:** PostgreSQL (Neon serverless) bajo el esquema `silver`.
- **Estructura:** 
  - `silver.store`: Catálogo maestro de tiendas (`id`, `name`, `country`, `is_active`).
  - `silver.product`: Catálogo maestro de productos con especificaciones normalizadas (`part_number`, `brand`, `series`, `capacity_gb`, `speed_mts`, `ddr_gen`, `kit_modules`, `has_rgb`).
  - `silver.price_snapshot`: Dimensión de Cambio Lento Tipo 2 (SCD-2) para seguimiento de precios con Change Data Capture (CDC).
  - `silver.etl_runs`: Metadatos de ejecución, duraciones del pipeline, resultados por tienda y métricas de volumen.
  - `silver.invalid_records`: Tabla de cuarentena que captura registros que no superaron la validación de Pydantic con su motivo exacto de error.

### 3. Capa Gold (Vistas Preparadas para Analítica)
- **Almacenamiento:** PostgreSQL bajo el esquema `gold`.
- **Propósito:** Desacopla el consumo de reportes de las operaciones internas de las tablas:
  - `gold.v_best_prices_today`: Precio actual más bajo por producto en todas las tiendas activas.
  - `gold.v_price_history`: Evolución histórica completa de precios con límites temporales.
  - `gold.v_price_diff_between_stores`: Comparativa de precios entre tiendas que muestra diferencias absolutas ($ USD) y porcentuales (%).
  - `gold.v_etl_runs` y `gold.v_etl_store_status`: Historial detallado de ejecuciones y estado desglosado por tienda.

---

## Componentes Principales

### 1. Scraping Modular (Patrón Factory)
- Scrapers modulares que heredan de una clase base común `BaseScraper`.
- **Extensibilidad**: Añadir una nueva tienda retail requiere únicamente crear un módulo scraper y registrarlo en `scrapers/factory.py` sin modificar la lógica aguas abajo del pipeline.
- **Resiliencia integrada**: Headers de user-agent personalizados, reutilización de sesiones y parsing defensivo.
- Los scrapers obtienen los productos (Kits de RAM) via API (endpoints internos de las paginas web).

### 2. Normalización, Extracción con Regex y Fuzzy Matching
- **Extracción de Especificaciones:** Expresiones regulares extraen la capacidad total (`capacity_gb`), generación de memoria (`ddr_gen`), frecuencia (`speed_mts`) y multiplicador de kits (`kit_modules`).
- **Resolución de Marca y Serie:** Los títulos en e-commerce suelen tener errores tipográficos o nombres inconsistentes (ej. `"KNGSTON FURY"`). El pipeline utiliza `rapidfuzz` para asociar nombres contra un diccionario canónico de marcas y series, asignando un puntaje de confianza.
- **Identificación de Clave Natural:** El `part_number` estandarizado actúa como identificador único de hardware cuando algún atributo no monitoreado determina la unicidad del producto.

### 3. Calidad de Datos y Cuarentena (Pydantic v2)
- Aplica contratos de datos estrictos mediante modelos tipados de Pydantic:
  - Valida rangos numéricos (ej. $1600 \le \text{speed\_mts} \le 10000$, $1 \le \text{capacity\_gb} \le 256$, $\text{price} > 0$).
  - Valida literales permitidos ($\text{ddr\_gen} \in [3, 4, 5]$).
  - Exige campos obligatorios no vacíos como `part_number` y `store`.
- **Cuarentena Tolerante a Fallos:** Los registros que fallan la validación se separan con `split_valid_invalid()` y se dirigen a `silver.invalid_records` con el detalle completo del `ValidationError`. El pipeline continúa procesando los registros válidos sin detenerse.

### 4. Change Data Capture (CDC) y Seguimiento Histórico SCD-2
- Evita snapshots diarios redundantes insertando filas en `silver.price_snapshot` **únicamente cuando el precio cambia**.
- Al detectar un cambio de precio:
  1. Se cierra el registro vigente anterior: `valid_to = NOW()`, `is_current = FALSE`.
  2. Se abre el nuevo registro: `valid_from = NOW()`, `valid_to = NULL`, `is_current = TRUE`.

### 5. Orquestación, Testing y CI/CD
- **Orquestación:** Prefect Cloud programa el pipeline en un cron de cada 6 horas (`0 */6 * * *`) con lógica de reintentos automáticos.
- **Testing:** Suite de pruebas integral con **137 pruebas unitarias** cubriendo scrapers, transformaciones, esquemas Pydantic y carga a base de datos usando `pytest`.
- **Containerización y CI/CD:** Builds multi-etapa con Docker (`python:3.12-slim`, usuario no-root). GitHub Actions ejecuta linters (`ruff`), tests (`pytest`) y publica imágenes de contenedor en GitHub Container Registry (GHCR).

---

## Decisiones Clave de Ingeniería y Trade-offs

| Decisión | Elección | Alternativa Considerada | Justificación |
|---|---|---|---|
| **Paradigma del Pipeline** | **ETL** | ELT | Las extracciones complejas con regex y fuzzy matching (`rapidfuzz`) se manejan eficientemente en memoria con Python. Enviar texto crudo y desordenado a Postgres para transformarlo dentro de la base de datos añadiría una carga de cómputo innecesaria. |
| **Persistencia del Raw** | **Data Lake Primero (Bronze)** | Carga Directa a BD | Escribir Parquet crudo a R2 antes de transformar garantiza una fuente inmutable de la verdad, auditoría completa y la posibilidad de reprocesar el histórico sin volver a scrapear. |
| **Almacenamiento del Data Lake** | **Cloudflare R2** | AWS S3 | Capa gratuita generosa (10 GB) y **cero costos de egress (transferencia de salida)**, manteniéndose 100% compatible con la API de S3 mediante el cliente `boto3`. |
| **Validación de Datos** | **Pydantic v2** | Great Expectations | Pydantic ofrece validación tipada rápida, integración nativa con el tipado de Python y mínima complejidad operativa para validaciones a nivel de fila. |
| **Seguimiento Histórico de Precios**| **SCD-2 con CDC** | Snapshots Diarios Completos | Capturar únicamente cambios de precio reduce el crecimiento del almacenamiento en más del 90% preservando los intervalos temporales exactos (`valid_from` / `valid_to`). |
| **Orquestación** | **Prefect Cloud** | Apache Airflow | Definición de flujos nativa en Python, coordinación serverless ligera y mínima sobrecarga de infraestructura para un proyecto individual. |
| **Manejo de Moneda** | **USD Normalizado** | Moneda Local Mixta (PEN/USD) | Las tiendas publican precios en ambas monedas. Estandarizar a USD en la capa ETL elimina la ambigüedad en consultas analíticas y vistas Gold. |
| **Diseño del Esquema Silver** | **Dimensional Híbrido** | Star Schema Completo | Silver utiliza un modelo relacional híbrido donde `price_snapshot` actúa como tabla de hechos central y product / store como dimensiones. Esto provee estructura dimensional para la analítica preservando las necesidades operacionales y de calidad de datos de Silver sin requerir un star schema estricto. |
| **Motor de Base de Datos** | PostgreSQL (OLTP) | OLAP / DWH (ej. Snowflake, BigQuery) | La escala de datos del proyecto (~cientos de registros por corrida, miles de snapshots al año) no justifica la complejidad, latencia de arranque en frío ni costos de un motor OLAP distribuido. PostgreSQL provee transacciones ACID esenciales para upserts atómicos de SCD-2, separación nativa de esquemas (Silver/Gold), índices parciales para lookups rápidos de CDC y excelente rendimiento para el consumo de BI a costo cero vía Neon serverless. En el futuro, si el volumen crece considerablemente, se podría evaluar migrar a un motor OLAP / DWH.|

---

## Dashboard en Power BI

La capa analítica se conecta a PostgreSQL mediante un rol de solo lectura (`bi_reader`) y modela los datos en un **Star Schema** (`dim_product`, `dim_store`, `dim_date`, `fact_price`, `etl_runs`, `etl_store_status`).

### Página 1: Monitoreo del Pipeline

<img src="images/dashboard-1.png" alt="Dashboard de Monitoreo del Pipeline"/>

- **Tarjetas KPI:** Fecha/hora de última ejecución exitosa, duración promedio del ETL (segundos) y total de productos activos monitoreados.
- **Semáforo de Estado de Tiendas:** Indicador visual (`ok` / `failed`) por tienda basado en `gold.v_etl_store_status`.
- **Salud y Tendencias de Ejecución:** Gráficos de línea y barras mostrando la duración histórica.

---

### Página 2: Análisis de Precios de RAM

<img src="images/dashboard-2.png" alt="Dashboard de Análisis de Precios de RAM"/>

#### 1. Catálogo de Mercado
- **Decisión de Diseño (Tabla sobre Matriz):** Una **Tabla** estructurada actúa como una ficha técnica profesional. Muestra Marca, Serie, Capacidad, Generación DDR, Configuración de Kit, Part Number y Precio Actual.

#### 2. Medidas y Columnas Calculadas DAX

- **`Current Price`:**
  ```dax
  Current Price = 
  CALCULATE(
      AVERAGE(fact_price[price]),
      fact_price[is_current] = TRUE
  )
  ```

- **`Avg Price per GB` (Métrica $/GB):**
  ```dax
  Avg Price per GB = 
  CALCULATE(
      AVERAGEX(
          fact_price,
          DIVIDE(fact_price[price], RELATED(dim_product[capacity_gb]))
      ),
      fact_price[is_current] = TRUE
  )
  ```

- **`Effective Price (as-of)` (Reconstrucción Histórica Point-in-Time para SCD-2):**
  ```dax
  Effective Price (as-of) = 
  VAR selectedDate = MAX(dim_date[date])
  VAR selectedDateTime = selectedDate + TIME(23, 59, 59)
  RETURN
  CALCULATE(
      AVERAGE(fact_price[price]),
      REMOVEFILTERS(dim_date),
      fact_price[valid_from] <= selectedDateTime,
      ISBLANK(fact_price[valid_to]) || fact_price[valid_to] > selectedDateTime
  )
  ```

- **`is_outlier` (Columna Calculada en `fact_price` para Detección de Outliers):** Usa la mediana por generación DDR para detectar outliers (30% - 500% del precio mediano).
  ```dax
  is_outlier = 
  VAR vCurrentPrice = fact_price[price]
  VAR vCurrentDDR = RELATED(dim_product[ddr_gen])

  VAR vMedianaDDR = 
      CALCULATE(
          MEDIAN(fact_price[price]),
          ALL(fact_price),
          dim_product[ddr_gen] = vCurrentDDR
      )

  RETURN
      IF(
          ISBLANK(vCurrentPrice) || vCurrentPrice <= 0 ||
          vCurrentPrice > (vMedianaDDR * 5) || 
          vCurrentPrice < (vMedianaDDR * 0.3),
          TRUE(),
          FALSE()
      )
  ```

#### 3. Insights Visuales Principales
- **Gráfico de Evolución de Precios:** Rastrea trayectorias históricas por tienda, identificando bajas de precio, promociones y tendencias de mercado.
- **Precio Promedio por GB por Marca:** Evalúa el posicionamiento de precios de cada fabricante en segmentos de memoria equivalentes.
- **Mayores Oportunidades de Ahorro:** Basado en `gold.v_price_diff_between_stores`, destaca oportunidades de arbitraje donde un mismo part number presenta amplias diferencias de precio entre tiendas.

---

## Cómo Visualizar el Dashboard

1. Clonar el repositorio:

  ```bash
  git clone <repository-url>
  ```

2. Abrir el archivo de Power BI: **`dashboard/dashboard.pbip`**
3. Los datos no pueden actualizarse, solo visualizarlos.

---

## Stack Tecnológico

- **Extracción de Datos:** Python 3.12, `requests`, `curl_cffi`
- **Transformación y Normalización:** `pandas`, `rapidfuzz`, `pyarrow`
- **Calidad de Datos y Validación:** `pydantic` v2
- **Data Lake (Bronze):** Cloudflare R2 (Object Storage / API S3 vía `boto3`)
- **Base de Datos y Almacenamiento (Silver / Gold):** PostgreSQL 15+ (Neon Serverless)
- **Orquestación:** Prefect Cloud
- **Business Intelligence:** Power BI Desktop (formato PBIP, DAX, Star Schema)
- **Containerización y CI/CD:** Docker, GitHub Actions, GHCR
- **Testing y Herramientas:** `pytest` (137 tests), `ruff`, `poethepoet`, `uv`
