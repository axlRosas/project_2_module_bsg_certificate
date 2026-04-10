# project_2_module_bsg_certificate

Este repositorio contiene mi proyecto para el taller "Certified Python Data Cloud Architect" segundo módulo "Modelado y Diseño de Datos con Python".

En este README se explica cómo se llevó a cabo el proyecto, su estructura, instalación y ejecución.

## Planteamiento

En la carpeta raíz se encuentra el archivo PDF **description_final_work_bsg_2_module.pdf** el cual contiene los criterios de evaluación para este proyecto.

Los puntos a evaluar son:

1. **Modelado Transaccional**
   - 1.1 Identificar las entidades del modelo transaccional
   - 1.2 Representación en Python de las entidades del modelo transaccional
   - 1.3 Generar el diagrama transaccional de las entidades

2. **Buenas prácticas y validación**
   - 2.1 Añadir validaciones estrictas con Pydantic
   - 2.2 Código modularizado

3. **Modelado analítico**
   - 3.1 Diseño de esquema estrella
   - 3.2 Implementación pipeline transaccional a estrella
   - 3.3 Consultas BI

## Instalación y Ejecución

### Prerrequisitos

- Python 3.8+
- Virtualenv (opcional pero recomendado)

### Configuración del Entorno

1. Clona el repositorio:
   ```bash
   git clone https://github.com/axlRosas/project_2_module_bsg_certificate.git
   cd project_2_module_bsg_certificate
   ```

2. Crea y activa un entorno virtual:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # En Windows: .venv\Scripts\activate
   ```

3. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

### Ejecución Completa

Para ejecutar todo el flujo del proyecto (creación de BD, carga de datos, ETL y consultas), simplemente ejecuta:

```bash
python main.py
```

Este script ejecutará automáticamente todos los pasos necesarios.

## Resolución

### 1. Modelado Transaccional

En la carpeta `entidades/` se encuentran los módulos para las entidades:

- `cliente.py`: Modelo del cliente
- `detalle_orden.py`: Detalles de las órdenes de compra
- `orden_compra.py`: Órdenes de compra
- `producto.py`: Productos disponibles
- `sucursal.py`: Sucursales
- `dimensiones.py`: Dimensiones para el esquema estrella

Cada entidad está representada como una clase Pydantic con validaciones estrictas.

Los diagramas se encuentran en la carpeta `diagramas/` en formato PDF y DBML.

### 2. Buenas Prácticas y Validación

- **Validaciones Pydantic**: Todas las clases incluyen validaciones como tipos de datos, rangos, formatos de email, etc.
- **Código Modularizado**: El proyecto está organizado en módulos separados por responsabilidad.

### 3. Modelado Analítico

#### 3.1 Diseño de Esquema Estrella

El esquema estrella incluye:

**Dimensiones:**
- `DimCliente`: Información de clientes
- `DimProducto`: Información de productos
- `DimSucursal`: Información de sucursales
- `DimFecha`: Dimensiones temporales
- `DimEstadoOrden`: Estados de órdenes
- `DimMetodoPago`: Métodos de pago

**Tabla de Hechos:**
- `FactVentas`: Ventas con medidas como cantidad, precio unitario, subtotal

#### 3.2 Pipeline ETL

El archivo `pipeline_transactional_to_star.py` contiene la clase `TransactionalToStarPipeline` que:

1. **Extrae** datos de la base de datos transaccional
2. **Transforma** los datos al formato de esquema estrella
3. **Carga** los datos en la base de datos `star_schema.sqlite`

Para ejecutar solo el pipeline:

```bash
python pipeline_transactional_to_star.py
```

#### 3.3 Consultas BI

El proyecto incluye consultas analíticas en `db_interactions.py`:

- Ventas por cliente
- Ingresos por mes
- Otras métricas de negocio

Ejemplo de uso:

```python
from db_interactions import get_sales_by_cliente

df = get_sales_by_cliente()
print(df)
```

## Estructura del Proyecto

```
.
├── main.py                          # Script principal de ejecución
├── create_database.py               # Creación de bases de datos
├── load_sample_data.py              # Carga de datos de ejemplo
├── pipeline_transactional_to_star.py # Pipeline ETL
├── db_interactions.py               # Funciones de consulta
├── examples.py                      # Ejemplos interactivos
├── query_database.py                # Exploración interactiva de BD
├── populate_database.py             # Poblado de BD (versión básica)
├── populate_database_pandas.py      # Poblado de BD con pandas
├── sample_data.json                 # Datos de ejemplo
├── QUICK_REFERENCE.py               # Referencia rápida de comandos
├── db/                              # Bases de datos SQLite
│   ├── transactional.sqlite         # BD transaccional
│   └── star_schema.sqlite           # BD esquema estrella
├── entidades/                       # Modelos Pydantic
│   ├── cliente.py
│   ├── detalle_orden.py
│   ├── orden_compra.py
│   ├── producto.py
│   ├── sucursal.py
│   ├── dimensiones.py               # Dimensiones del esquema estrella
│   ├── test_models.py               # Pruebas de modelos
│   └── test_star_schema.py          # Pruebas de esquema estrella
├── diagramas/                       # Diagramas de BD
│   ├── dbml_structure_star.txt
│   └── dbml_structure_transactional.txt
├── utils/                           # Utilidades
└── *.md                             # Documentación
```

## Archivos Importantes

- **README_ETL_PIPELINE.md**: Documentación detallada del pipeline ETL
- **STAR_SCHEMA_SUMMARY.md**: Resumen del esquema estrella
- **STAR_SCHEMA_MODELS.md**: Documentación de los modelos Pydantic
- **DATABASE_SETUP.md**: Configuración de bases de datos
- **ETL_PIPELINE_GUIDE.md**: Guía completa del pipeline

## Próximos Pasos

- Usar `db_interactions.py` para consultas personalizadas
- Explorar `examples.py` para ver patrones de uso
- Usar `query_database.py` para exploración interactiva

---

**Autor:** Axel Rosas  
**Fecha:** 2026-04-10


