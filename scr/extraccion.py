import os
import pandas as pd
from pymongo import MongoClient, errors
from typing import Dict, Optional, Tuple
from datetime import datetime
from .logs import Logs


class ExtraccionWindows:
    """
    Clase para la extracción de datos de Airbnb desde MongoDB.
    
    Esta clase maneja la conexión a MongoDB y la extracción de datos
    de las colecciones listings, reviews y calendar, convirtiéndolos
    en DataFrames de pandas.
    """
    
    def __init__(self, mongo_uri: str = None, database_name: str = "airbnb"):
        """
        Inicializa la clase Extraccion.
        
        Args:
            mongo_uri (str, optional): URI de conexión a MongoDB. 
                                     Por defecto usa mongodb://localhost:27017/
            database_name (str, optional): Nombre de la base de datos. 
                                         Por defecto "Airbnb"
        """
        # Configurar sistema de logs unificado
        self.logs = Logs("Extraccion")
        
        # Establecer URI de conexión
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.database_name = database_name
        
        # Inicializar atributos de conexión
        self.client = None
        self.db = None
        
        # Conectar a MongoDB
        self._conectar_mongodb()
    
    
    def _conectar_mongodb(self):
        """
        Establece conexión con MongoDB y verifica la conectividad.
        """
        try:
            # Crear cliente con timeout de 5 segundos
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            
            # Verificar conexión con ping
            self.client.admin.command("ping")
            
            # Obtener referencia a la base de datos
            self.db = self.client[self.database_name]
            
            # Registrar conexión exitosa
            self.logs.info(f"Conexión exitosa a MongoDB: {self.mongo_uri}")
            self.logs.info(f"Base de datos: {self.database_name}")
            print("Conectado exitosamente a MongoDB.")
            
        except errors.ServerSelectionTimeoutError as e:
            error_msg = f"No se pudo conectar a MongoDB: {e}"
            self.logs.error(error_msg)
            print(f"ERROR: {error_msg}")
            raise SystemExit(1)
        except Exception as e:
            error_msg = f"Error inesperado al conectar: {e}"
            self.logs.error(error_msg)
            print(f"ERROR: {error_msg}")
            raise SystemExit(1)
    
    def obtener_datos_coleccion(self, nombre_coleccion: str, 
                              filtro: Optional[Dict] = None, 
                              limite: Optional[int] = None) -> pd.DataFrame:
        """
        Extrae datos de una colección específica y los convierte en DataFrame.
        
        Args:
            nombre_coleccion (str): Nombre de la colección a extraer
            filtro (Dict, optional): Filtro de MongoDB para la consulta
            limite (int, optional): Límite de registros a extraer
            
        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        try:
            # Verificar que la colección existe
            if nombre_coleccion not in self.db.list_collection_names():
                error_msg = f"La colección '{nombre_coleccion}' no existe"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Obtener referencia a la colección
            coleccion = self.db[nombre_coleccion]
            
            # Construir consulta
            consulta = filtro or {}
            cursor = coleccion.find(consulta)
            
            # Aplicar límite si se especifica
            if limite:
                cursor = cursor.limit(limite)
            
            # Convertir a lista y luego a DataFrame
            datos = list(cursor)
            
            if not datos:
                self.logger.warning(f"No se encontraron datos en la colección '{nombre_coleccion}'")
                return pd.DataFrame()
            
            # Convertir a DataFrame
            df = pd.DataFrame(datos)
            
            # Registrar extracción exitosa
            cantidad_registros = len(df)
            self.logs.info(f"Extraídos {cantidad_registros} registros de '{nombre_coleccion}'")
            print(f"Extraídos {cantidad_registros} registros de '{nombre_coleccion}'")
            
            return df
            
        except Exception as e:
            error_msg = f"Error al extraer datos de '{nombre_coleccion}': {e}"
            self.logs.error(error_msg)
            print(f"ERROR: {error_msg}")
            raise
    
    def obtener_listings(self, filtro: Optional[Dict] = None, 
                        limite: Optional[int] = None) -> pd.DataFrame:
        """
        Extrae datos de la colección 'listings'.
        """
        return self.obtener_datos_coleccion('listings', filtro, limite)
    
    def obtener_reviews(self, filtro: Optional[Dict] = None, 
                       limite: Optional[int] = None) -> pd.DataFrame:
        """
        Extrae datos de la colección 'reviews'.
        """
        return self.obtener_datos_coleccion('reviews', filtro, limite)
    
    def obtener_calendar(self, filtro: Optional[Dict] = None, 
                        limite: Optional[int] = None) -> pd.DataFrame:
        """
        Extrae datos de la colección 'calendar'.
        """
        return self.obtener_datos_coleccion('calendar', filtro, limite)
    
    def obtener_estadisticas_colecciones(self) -> Dict[str, int]:
        """
        Obtiene estadísticas básicas de todas las colecciones.
        
        Returns:
            Dict[str, int]: Diccionario con el conteo de documentos por colección
        """
        estadisticas = {}
        colecciones = self.db.list_collection_names()
        
        self.logs.info("Obteniendo estadísticas de colecciones")
        print("Obteniendo estadísticas de colecciones...")
        
        for coleccion in colecciones:
            try:
                conteo = self.db[coleccion].count_documents({})
                estadisticas[coleccion] = conteo
                self.logs.info(f"Colección '{coleccion}': {conteo} documentos")
                print(f"{coleccion}: {conteo:,} documentos")
            except Exception as e:
                self.logs.error(f"Error al contar documentos en '{coleccion}': {e}")
                print(f"ERROR al contar '{coleccion}': {e}")
        
        return estadisticas
    
    def cerrar_conexion(self):
        """
        Cierra la conexión con MongoDB.
        """
        if self.client:
            self.client.close()
            self.logs.info("Conexión a MongoDB cerrada")
            print("Conexión a MongoDB cerrada")
            self.logs.cerrar_log()


# Ejemplo de uso
if __name__ == "__main__":
    """
    Ejemplo de uso de la clase Extraccion.
    
    Este bloque demuestra cómo usar la clase para:
    1. Conectar a MongoDB
    2. Obtener estadísticas de las colecciones
    3. Extraer datos de cada colección
    4. Cerrar la conexión
    """
    try:
        # Crear instancia de la clase Extraccion
        print("Iniciando extracción de datos de Airbnb...")
        extractor = ExtraccionWindows()
        
        # Obtener estadísticas de las colecciones
        print("\nEstadísticas de las colecciones:")
        estadisticas = extractor.obtener_estadisticas_colecciones()
        
        # Extraer datos de cada colección (TODOS los registros)
        print("\nExtrayendo datos de las colecciones...")
        
        # Extraer listings (TODOS los registros)
        df_listings = extractor.obtener_listings()
        print(f"Listings extraídos: {len(df_listings)} registros")
        
        # Extraer reviews (TODOS los registros)
        df_reviews = extractor.obtener_reviews()
        print(f"Reviews extraídos: {len(df_reviews)} registros")
        
        # Extraer calendar (TODOS los registros)
        df_calendar = extractor.obtener_calendar()
        print(f"Calendar extraídos: {len(df_calendar)} registros")
        
        # Mostrar información básica de los DataFrames
        print("\nInformación de los DataFrames:")
        print(f"Listings - Forma: {df_listings.shape}, Columnas: {list(df_listings.columns)[:5]}...")
        print(f"Reviews - Forma: {df_reviews.shape}, Columnas: {list(df_reviews.columns)[:5]}...")
        print(f"Calendar - Forma: {df_calendar.shape}, Columnas: {list(df_calendar.columns)[:5]}...")
        
        # Cerrar conexión
        extractor.cerrar_conexion()
        print("\nProceso de extracción completado exitosamente!")
        
    except Exception as e:
        print(f"ERROR durante la extracción: {e}")
        print("Revisa el archivo 'extraccion_airbnb_windows.log' para más detalles.")
