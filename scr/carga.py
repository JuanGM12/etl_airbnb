import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from .logs import Logs

class CargaMySQL:
    """
    Clase para la carga de datos transformados en MySQL y XLSX.
    
    Esta clase maneja la inserción de datos limpios en MySQL
    y exportación a Excel, con verificación de integridad y logging completo.
    """
    
    def __init__(self, host='localhost', port=3306, database='airbnb', user='root', password=''):
        """
        Inicializa la clase CargaMySQL.
        
        Args:
            host (str): Host de MySQL
            port (int): Puerto de MySQL
            database (str): Nombre de la base de datos
            user (str): Usuario de MySQL
            password (str): Contraseña de MySQL
        """
        self.logs = Logs("CargaMySQL")
        self.datos_transformados = {}
        self.estadisticas_carga = {}
        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        
        self.logs.info("Clase CargaMySQL inicializada")
    
    def crear_conexion_mysql(self):
        """
        Crea conexión con MySQL.
        
        Returns:
            mysql.connector.connection.MySQLConnection: Conexión a MySQL
        """
        self.logs.registrar_inicio_operacion(f"conexion a MySQL en {self.host}")
        
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            
            if self.connection.is_connected():
                self.logs.info(f"Conexion exitosa a MySQL: {self.host}/{self.database}")
                self.logs.registrar_fin_operacion("conexion a MySQL")
                return self.connection
                
        except Error as e:
            self.logs.error(f"Error al conectar a MySQL: {e}")
            self.logs.registrar_error_detallado(e, "conexion a MySQL")
            raise
    
    def cargar_datos_transformados(self, datos: Dict[str, pd.DataFrame]):
        """
        Carga los datos transformados para inserción.
        
        Args:
            datos (Dict[str, pd.DataFrame]): Diccionario con DataFrames transformados
        """
        self.datos_transformados = datos.copy()
        self.logs.registrar_inicio_operacion("carga de datos transformados")
        
        for nombre, df in datos.items():
            self.logs.info(f"Datos cargados: {nombre} - {len(df):,} registros, {len(df.columns)} columnas")
        
        self.logs.registrar_fin_operacion("carga de datos transformados")
    
    def insertar_datos_mysql(self) -> Dict[str, int]:
        """
        Inserta los datos transformados en MySQL.
        
        Returns:
            Dict[str, int]: Estadísticas de inserción por tabla
        """
        self.logs.registrar_inicio_operacion("insercion de datos en MySQL")
        
        estadisticas = {}
        
        try:
            cursor = self.connection.cursor()
            
            for nombre_tabla, df in self.datos_transformados.items():
                self.logs.info(f"Insertando tabla: {nombre_tabla}")
                
                registros_antes = len(df)
                
                # Limpiar datos para MySQL
                df_limpio = df.copy()
                
                # Eliminar columnas con ObjectId de MongoDB
                if '_id' in df_limpio.columns:
                    df_limpio = df_limpio.drop(columns=['_id'])
                    self.logs.info(f"Columna _id eliminada para {nombre_tabla}")
                
                # Limpiar nombres de columnas para MySQL (sin espacios, sin caracteres especiales)
                df_limpio.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df_limpio.columns]
                
                # Crear tabla si no existe
                self._crear_tabla_mysql(cursor, nombre_tabla, df_limpio)
                
                # Vaciar tabla antes de insertar
                cursor.execute(f"TRUNCATE TABLE `{nombre_tabla}`")
                
                # Insertar datos usando prepared statements (más seguro y eficiente)
                chunk_size = 1000
                columnas = df_limpio.columns.tolist()
                placeholders = ', '.join(['%s'] * len(columnas))
                query = f"INSERT INTO `{nombre_tabla}` (`{'`, `'.join(columnas)}`) VALUES ({placeholders})"
                
                # Convertir NaN, NaT, None a None (NULL en MySQL)
                df_limpio = df_limpio.replace({pd.NA: None, pd.NaT: None})
                df_limpio = df_limpio.where(pd.notnull(df_limpio), None)
                
                # Insertar en chunks
                for i in range(0, len(df_limpio), chunk_size):
                    chunk = df_limpio.iloc[i:i+chunk_size]
                    valores = []
                    for _, row in chunk.iterrows():
                        # Convertir valores a lista, reemplazar NaN por None
                        fila_values = []
                        for val in row:
                            if pd.isna(val):
                                fila_values.append(None)
                            else:
                                fila_values.append(val)
                        valores.append(tuple(fila_values))
                    
                    try:
                        cursor.executemany(query, valores)
                        self.connection.commit()
                        self.logs.info(f"Chunk {i//chunk_size + 1} insertado: {len(chunk)} registros")
                    except Error as e:
                        self.logs.error(f"Error al insertar chunk {i//chunk_size + 1}: {e}")
                        self.connection.rollback()
                        raise
                
                # Verificar inserción
                cursor.execute(f"SELECT COUNT(*) FROM `{nombre_tabla}`")
                registros_insertados = cursor.fetchone()[0]
                
                estadisticas[nombre_tabla] = {
                    'registros_originales': registros_antes,
                    'registros_insertados': registros_insertados,
                    'exito': registros_antes == registros_insertados
                }
                
                if registros_antes == registros_insertados:
                    self.logs.info(f"Tabla {nombre_tabla}: {registros_insertados:,} registros insertados correctamente")
                else:
                    self.logs.warning(f"Tabla {nombre_tabla}: Discrepancia - {registros_antes:,} originales vs {registros_insertados:,} insertados")
            
            self.logs.registrar_fin_operacion("insercion de datos en MySQL")
            return estadisticas
            
        except Error as e:
            self.connection.rollback()
            self.logs.registrar_error_detallado(e, "insercion de datos en MySQL")
            raise
    
    def _crear_tabla_mysql(self, cursor, nombre_tabla: str, df: pd.DataFrame):
        """
        Crea la tabla en MySQL si no existe.
        
        Args:
            cursor: Cursor de MySQL
            nombre_tabla (str): Nombre de la tabla
            df (pd.DataFrame): DataFrame con los datos
        """
        try:
            # Determinar tipos de datos MySQL
            column_definitions = []
            for col in df.columns:
                dtype = df[col].dtype
                
                if pd.api.types.is_integer_dtype(dtype):
                    mysql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    mysql_type = "DOUBLE"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    mysql_type = "DATETIME"
                elif pd.api.types.is_bool_dtype(dtype):
                    mysql_type = "TINYINT(1)"
                else:
                    # String o tipos complejos -> TEXT
                    mysql_type = "TEXT"
                
                column_definitions.append(f"`{col}` {mysql_type}")
            
            # Crear query CREATE TABLE
            query = f"""
            CREATE TABLE IF NOT EXISTS `{nombre_tabla}` (
                {', '.join(column_definitions)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            cursor.execute(query)
            self.logs.info(f"Tabla `{nombre_tabla}` creada o ya existe")
            
        except Error as e:
            self.logs.registrar_error_detallado(e, f"creacion de tabla {nombre_tabla}")
    
    def exportar_a_xlsx(self, ruta_archivo: str = "datos_airbnb_mysql.xlsx") -> bool:
        """
        Exporta los datos transformados a archivo XLSX.
        
        Args:
            ruta_archivo (str): Ruta del archivo XLSX a crear
            
        Returns:
            bool: True si la exportación fue exitosa
        """
        self.logs.registrar_inicio_operacion(f"exportacion a XLSX: {ruta_archivo}")
        
        try:
            with pd.ExcelWriter(ruta_archivo, engine='openpyxl') as writer:
                for nombre_hoja, df in self.datos_transformados.items():
                    nombre_hoja_limpio = nombre_hoja[:31]
                    
                    df.to_excel(
                        writer,
                        sheet_name=nombre_hoja_limpio,
                        index=False,
                        engine='openpyxl'
                    )
                    
                    self.logs.info(f"Hoja '{nombre_hoja_limpio}': {len(df):,} registros exportados")
            
            self.logs.info(f"Archivo XLSX creado exitosamente: {ruta_archivo}")
            self.logs.registrar_fin_operacion("exportacion a XLSX")
            return True
            
        except Exception as e:
            self.logs.registrar_error_detallado(e, "exportacion a XLSX")
            return False
    
    def ejecutar_carga_completa(self, datos: Dict[str, pd.DataFrame]) -> Dict:
        """
        Ejecuta el proceso completo de carga de datos.
        
        Args:
            datos (Dict[str, pd.DataFrame]): Datos transformados a cargar
            
        Returns:
            Dict: Reporte completo de la carga
        """
        self.logs.info("INICIANDO CARGA COMPLETA EN MYSQL")
        self.logs.info("=" * 50)
        
        try:
            # 1. Conectar a MySQL
            self.crear_conexion_mysql()
            
            # 2. Cargar datos transformados
            self.cargar_datos_transformados(datos)
            
            # 3. Insertar datos en MySQL
            estadisticas_mysql = self.insertar_datos_mysql()
            
            # 4. Exportar a XLSX
            archivo_xlsx = "datos_airbnb_mysql.xlsx"
            xlsx_exitoso = self.exportar_a_xlsx(archivo_xlsx)
            
            # 5. Cerrar conexión
            if self.connection and self.connection.is_connected():
                self.connection.close()
                self.logs.info("Conexion a MySQL cerrada")
            
            # 6. Generar reporte
            import json
            reporte = {
                'fecha_carga': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'motor': 'MySQL',
                'database': self.database,
                'estadisticas_mysql': estadisticas_mysql,
                'archivos_generados': {
                    'xlsx': archivo_xlsx
                },
                'resumen': {
                    'total_tablas': len(self.datos_transformados),
                    'total_registros': sum(len(df) for df in self.datos_transformados.values())
                }
            }
            
            with open('reporte_carga_mysql.json', 'w', encoding='utf-8') as f:
                json.dump(reporte, f, indent=2, ensure_ascii=False)
            
            self.logs.info("CARGA COMPLETA FINALIZADA EXITOSAMENTE")
            self.logs.info("=" * 50)
            
            return reporte
            
        except Exception as e:
            self.logs.registrar_error_detallado(e, "carga completa en MySQL")
            if self.connection and self.connection.is_connected():
                self.connection.close()
            raise
        finally:
            self.logs.cerrar_log()


# Ejemplo de uso
if __name__ == "__main__":
    try:
        import pandas as pd
        import os
        
        # Cargar datos transformados desde archivos CSV
        print("Cargando datos transformados desde CSV...")
        print("=" * 60)
        
        ruta_datos_transformados = 'datos_transformados'
        
        if not os.path.exists(ruta_datos_transformados):
            raise FileNotFoundError(f"La carpeta '{ruta_datos_transformados}' no existe.")
        
        datos_transformados = {}
        archivos_csv = {
            'listings': 'listings_transformado.csv',
            'reviews': 'reviews_transformado.csv',
            'calendar': 'calendar_transformado.csv'
        }
        
        for nombre, archivo in archivos_csv.items():
            ruta_completa = os.path.join(ruta_datos_transformados, archivo)
            if os.path.exists(ruta_completa):
                print(f"  Cargando {nombre}...")
                df = pd.read_csv(ruta_completa, encoding='utf-8')
                datos_transformados[nombre] = df
                print(f"    [OK] {nombre}: {len(df):,} registros, {len(df.columns)} columnas")
        
        if not datos_transformados:
            raise ValueError("No se encontraron archivos CSV transformados.")
        
        print("\nDatos CSV cargados exitosamente!")
        print("=" * 60)
        cargador = CargaMySQL(
            host='localhost',
            port=3306,
            database='airbnb',
            user='root',
            password=''  # Cambiar esto
        )
        
        reporte = cargador.ejecutar_carga_completa(datos_transformados)
        
        print("\n" + "=" * 60)
        print("PROCESO DE CARGA EN MYSQL COMPLETADO EXITOSAMENTE!")
        print("=" * 60)
        print(f"\nTotal de registros cargados: {reporte['resumen']['total_registros']:,}")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
