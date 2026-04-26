# ETL Medallion — Ingeniería de Datos con Databricks

Pipeline ETL completo sobre Azure Databricks usando arquitectura **Bronze → Silver → Gold** con datasets de ventas E-commerce y Superstore.

## Arquitectura

```
Azure Data Lake Storage Gen2 (container: raw)
         ↓  Unity Catalog External Location
┌─────────────────────────────────────────────────────────┐
│  Bronze  │  Datos crudos ingestados como tablas Delta   │
│  Silver  │  Limpieza, tipos correctos, reglas negocio   │
│  Gold    │  Agregaciones listas para BI y dashboards    │
└─────────────────────────────────────────────────────────┘
         ↓
Databricks SQL Dashboard / Power BI
```

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Plataforma | Azure Databricks (Premium) |
| Almacenamiento | Azure Data Lake Storage Gen2 |
| Formato de tablas | Delta Lake |
| Gobernanza | Unity Catalog |
| Autenticación | Azure Managed Identity (Access Connector) |
| Orquestación | Databricks Asset Bundles + Serverless Compute |
| CI/CD | GitHub Actions |
| Lenguaje | Python / PySpark |

## Datasets

| Dataset | Filas | Descripción |
|---|---|---|
| E-commerce Sales | 2,000 | Ventas online con categoría, región y margen |
| Superstore | 9,994 | Ventas retail con tiempos de envío y rentabilidad |

## Estructura del repositorio

```
├── .github/workflows/ci_cd.yml     # Pipeline CI/CD GitHub Actions
├── notebooks/
│   ├── 01_bronze_ingest.py         # Ingesta desde ADLS Gen2
│   ├── 02_silver_transform.py      # Limpieza y transformación
│   └── 03_gold_aggregate.py        # Agregaciones para BI
├── src/
│   └── utils.py                    # Funciones utilitarias PySpark
├── tests/
│   └── test_transforms.py          # Unit tests
├── databricks.yml                  # Databricks Asset Bundle
└── requirements.txt
```

## Tablas Gold

| Tabla | Descripción |
|---|---|
| `gold.ecom_sales_by_category` | Ventas e-commerce por categoría, mes y año |
| `gold.super_performance_by_region` | Rendimiento Superstore por región y segmento |
| `gold.category_comparison` | Comparativa cruzada entre ambos datasets |
| `gold.top10_products` | Top 10 productos más rentables |

## Infraestructura Azure

- **Resource Group**: `rg-databricks-etl`
- **Storage Account**: `oscaretlstorage` (ADLS Gen2, container `raw`)
- **Databricks Workspace**: `dbw-etl-medallion` (Premium, East US)
- **Access Connector**: autenticación via Managed Identity
- **Unity Catalog**: catálogo `etl_medallion` con schemas `bronze`, `silver`, `gold`

## CI/CD

| Rama | Trigger | Acción |
|---|---|---|
| `develop` | Push | Lint → Tests → Validate → Deploy Dev → Run Dev |
| `main` | Push | Lint → Tests → Validate → Deploy Prod → Run Prod |
| PR a `main` | Pull Request | Lint → Tests → Validate |

## Despliegue manual

```bash
# Activar entorno virtual
source .venv/bin/activate

# Validar bundle
databricks bundle validate

# Desplegar en dev
databricks bundle deploy --target dev

# Ejecutar pipeline
databricks bundle run etl_medallion_pipeline --target dev --var="storage_account=oscaretlstorage"
```
