# 🏠 Proyecto ETL - Análisis de Datos de Airbnb

## 📋 Descripción del Proyecto y Objetivo

Este proyecto implementa un **pipeline ETL (Extract, Transform, Load)** completo para analizar datos de Airbnb de la Ciudad de México. El objetivo es extraer datos desde MongoDB, realizar un análisis exploratorio de datos (EDA), transformar y limpiar los datos, y finalmente cargarlos en una base de datos MySQL para análisis y reportes.

### Objetivos Específicos:
- **Extracción**: Obtener datos de MongoDB (listings, reviews, calendar)
- **Exploración**: Analizar la calidad de los datos y detectar problemas
- **Transformación**: Limpiar, normalizar y transformar los datos
- **Carga**: Insertar datos limpios en MySQL para análisis posterior

## 🗂️ Estructura del Proyecto

```
Airbnb/
├── scr/                           # Código fuente Python
│   ├── __init__.py               # Paquete Python
│   ├── extraccion.py             # Paso 1: Extracción desde MongoDB
│   ├── transformacion.py         # Paso 3: Transformación de datos
│   ├── carga.py                  # Paso 4: Carga en MySQL
│   └── logs.py                   # Sistema de logging
├── notebooks/                     # Jupyter Notebooks
│   └── exploracion_airbnb.ipynb  # Paso 2: Análisis exploratorio
├── data/                         # Datos originales en CSV.gz
│   ├── calendar.csv.gz
│   ├── listings.csv.gz
│   └── reviews.csv.gz
├── datos_transformados/           # CSV transformados
│   ├── calendar_transformado.csv
│   ├── listings_transformado.csv
│   └── reviews_transformado.csv
├── imagenes/                     # Gráficos y análisis
├── logs/                         # Archivos de log
├── main.py                       # Script principal con argumentos
├── requirements.txt              # Dependencias del proyecto
└── README.md                     # Este archivo
```

## 🚀 Instrucciones de Instalación

### Prerrequisitos

- Python 3.8 o superior
- MySQL Server instalado y ejecutándose
- MongoDB (para extracción inicial de datos)
- Git (opcional)

### Paso 1: Crear Entorno Virtual

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### Paso 2: Instalar Dependencias

```bash
pip install -r requirements.txt
```

### Paso 3: Obtener los Datos CSV

⚠️ **IMPORTANTE**: Debido a las limitaciones de tamaño de GitHub, los archivos CSV comprimidos no están incluidos en el repositorio.

Debes colocar los siguientes archivos en la carpeta `data/`:

```
data/
├── calendar.csv.gz
├── listings.csv.gz
└── reviews.csv.gz
```

**Forma de obtener los datos:**

1. **Opción 1 - Generar desde MongoDB** (si tienes acceso):
   - Ejecuta: `python main.py` → Opción 1 (Extracción)
   - Los archivos se generarán automáticamente en `data/`

2. **Opción 2 - Descargar desde fuente externa**:
   - Descarga los archivos comprimidos desde una fuente externa
   - Colócalos manualmente en la carpeta `data/`
   - Asegúrate de que estén en formato `.csv.gz`

### Paso 4: Configurar MySQL

1. **Crear la base de datos en MySQL:**
```sql
CREATE DATABASE airbnb;
```

2. **Configurar credenciales en `scr/carga.py`:**
Abre el archivo `scr/carga.py` y busca las siguientes líneas (alrededor de la línea 340):

```python
cargador = CargaMySQL(
    host='localhost',
    port=3306,
    database='airbnb',
    user='root',              # Cambiar si es necesario
    password=''                # ⚠️ INGRESA TU CONTRASEÑA DE MYSQL AQUÍ
)
```

**⚠️ IMPORTANTE**: Reemplaza los valores según tu configuración de MySQL:
- `user`: Usuario de MySQL (por defecto: 'root')
- `password`: Tu contraseña de MySQL
- `host`: Host de MySQL (por defecto: 'localhost')
- `port`: Puerto de MySQL (por defecto: 3306)
- `database`: Nombre de la base de datos (por defecto: 'airbnb')

### Paso 5: Ejecutar el Pipeline ETL

**⚠️ NUEVO: Interfaz con Menú Interactivo**

Simplemente ejecuta:
```bash
python main.py
```

Luego selecciona:
- **1** → Extracción de datos (MongoDB → CSV)
- **2** → Transformación de datos (CSV → CSV limpio)
- **3** → Carga de datos (CSV → MySQL)
- **4** → Análisis Exploratorio (abrir notebook)
- **5** → Pipeline completo (1+2+3)
- **0** → Salir

---

**Opción alternativa: Argumentos de línea de comandos**

```bash
# Pipeline completo
python main.py --all

# Pasos individuales
python main.py --extract      # Extracción
python main.py --transform    # Transformación
python main.py --load         # Carga
python main.py --eda          # Instrucciones para EDA

# Combinaciones
python main.py --transform --load    # Solo transformación y carga
python main.py --help                # Ver ayuda completa
```

## 👥 Integrantes del Grupo y Responsabilidades

### Equipo de Desarrollo

**Integrante 1**: [Nombre]
- **Responsabilidades**: 
  - Extracción de datos (extraccion.py)
  - Configuración del sistema de logging
  
**Integrante 2**: [Nombre]
- **Responsabilidades**:
  - Análisis exploratorio de datos (exploracion_airbnb.ipynb)
  - Identificación de problemas de calidad de datos

**Integrante 3**: [Nombre]
- **Responsabilidades**:
  - Transformación y limpieza de datos (transformacion.py)
  - Manejo de tipos no hasheables y datos anidados

**Integrante 4**: [Nombre]
- **Responsabilidades**:
  - Carga de datos en MySQL (carga.py)
  - Optimización de inserción de datos masivos

## 📊 Ejemplo de Ejecución del ETL

### Ejemplo Completo: Pipeline ETL de Airbnb

```bash
# 1. ACTIVAR ENTORNO VIRTUAL
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# 2. EJECUTAR PIPELINE COMPLETO
python main.py --all

# Salida esperada:
# ============================================================
# 🏠 ETL PIPELINE - ANÁLISIS DE DATOS DE AIRBNB
# ============================================================
# Fecha de ejecución: 2025-10-25 17:30:00
# ✅ Todas las dependencias están instaladas
# 
# 🔄 INICIANDO EXTRACCIÓN DE DATOS
# ----------------------------------------
# Conectando a MongoDB...
# Extrayendo datos...
#   listings: 22,109 registros extraídos
#   reviews: 1,388,218 registros extraídos
#   calendar: 9,636,365 registros extraídos
# ✅ Extracción completada exitosamente
# 
# 🔄 INICIANDO TRANSFORMACIÓN DE DATOS
# ----------------------------------------
# Cargando datos desde archivos CSV...
# Transformando datos...
#   listings: 22,109 registros, 90 columnas
#   reviews: 1,388,218 registros, 14 columnas
#   calendar: 9,636,365 registros, 13 columnas
# ✅ Transformación completada exitosamente
# 
# 🔄 INICIANDO CARGA DE DATOS
# ----------------------------------------
# ⚠️  IMPORTANTE: Configura las credenciales de MySQL en scr/carga.py
# Cargando datos transformados...
# Conectando a MySQL...
# Insertando tabla: listings
#   Chunk 1 insertado: 1000 registros
#   Chunk 2 insertado: 1000 registros
#   ... (continúa para todas las tablas)
# ✅ Carga completada exitosamente
#   Total de registros cargados: 11,046,692
# 
# ============================================================
# 🎉 PIPELINE EJECUTADO EXITOSAMENTE
# 📊 Datos listos para análisis en MySQL
# ============================================================
```

### Ejemplo de Pasos Individuales

```bash
# Solo extracción
python main.py --extract

# Solo transformación (requiere datos extraídos)
python main.py --transform

# Solo carga (requiere datos transformados)
python main.py --load

# Análisis exploratorio
python main.py --eda
# Luego abrir: jupyter notebook notebooks/exploracion_airbnb.ipynb
```

### Verificar Datos en MySQL

```sql
-- Conectar a MySQL
mysql -u root -p airbnb

-- Verificar tablas creadas
SHOW TABLES;

-- Contar registros por tabla
SELECT 'listings' as tabla, COUNT(*) as registros FROM listings
UNION ALL
SELECT 'reviews', COUNT(*) FROM reviews
UNION ALL
SELECT 'calendar', COUNT(*) FROM calendar;

-- Ver estructura de la tabla listings
DESCRIBE listings;

-- Consulta de ejemplo: Top 10 propiedades con más reviews
SELECT name, number_of_reviews, review_scores_rating
FROM listings
ORDER BY number_of_reviews DESC
LIMIT 10;
```

## 📈 Estadísticas del Proyecto

- **Total de registros**: 11,046,692
- **Tablas**: 3 (listings, reviews, calendar)
- **Columnas en listings**: 91
- **Tiempo de carga**: ~45 segundos

## 🛠️ Tecnologías Utilizadas

- **Python 3.8+**
- **Pandas**: Manipulación y análisis de datos
- **NumPy**: Operaciones numéricas
- **Matplotlib/Seaborn**: Visualización de datos
- **Jupyter Notebook**: Análisis exploratorio interactivo
- **MySQL**: Base de datos relacional
- **PyMongo**: Conexión con MongoDB
- **OpenPyXL**: Exportación a Excel

## 📝 Notas Importantes

1. **Credenciales MySQL**: Asegúrate de configurar correctamente las credenciales en `carga.py` antes de ejecutar el paso de carga.

2. **Volumen de Datos**: El proceso carga más de 11 millones de registros. Asegúrate de tener suficiente espacio en disco y memoria.

3. **Logs**: Todos los logs se guardan automáticamente en la carpeta `logs/` con timestamps.

4. **Errores Comunes**:
   - Error de conexión MySQL: Verifica que el servidor esté ejecutándose
   - Error de memoria: Reduce el tamaño de los chunks en `carga.py`
   - Errores de encoding: Los archivos CSV usan UTF-8

## 📞 Soporte

Para problemas o preguntas, consulta los archivos de log en la carpeta `logs/` o revisa la documentación en cada módulo Python.

## 📄 Licencia

Este proyecto es de uso educativo y de análisis de datos.
