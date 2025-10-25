import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import warnings
from .logs import Logs

warnings.filterwarnings('ignore')

class Transformacion:
    """
    Clase para la transformación y limpieza de datos de Airbnb.
    
    Esta clase implementa todas las transformaciones necesarias para preparar
    los datos extraídos de MongoDB para su uso en Data Warehouse o análisis
    avanzado, incluyendo limpieza, normalización y derivación de variables.
    """
    
    def __init__(self):
        """
        Inicializa la clase Transformacion.
        """
        # Configurar sistema de logs unificado
        self.logs = Logs("Transformacion")
        
        # Almacenar datos transformados
        self.datos_originales = {}
        self.datos_transformados = {}
        self.estadisticas_transformacion = {}
        
        self.logs.info("Clase Transformacion inicializada")
        print("Clase Transformacion inicializada correctamente")
    
    
    def cargar_datos_para_transformacion(self, datos: Dict[str, pd.DataFrame]):
        """
        Carga los datos originales para transformación.
        
        Args:
            datos (Dict[str, pd.DataFrame]): Diccionario con DataFrames de cada colección
        """
        self.datos_originales = datos.copy()
        self.logs.info(f"Datos cargados para transformación: {list(datos.keys())}")
        print(f"Datos cargados para transformación: {list(datos.keys())}")
        
        # Mostrar estadísticas iniciales
        for nombre, df in datos.items():
            print(f"  {nombre}: {len(df):,} registros, {len(df.columns)} columnas")
    
    def cargar_datos_desde_csv(self) -> Dict[str, pd.DataFrame]:
        """
        Carga los datos desde archivos CSV comprimidos.
        
        Returns:
            Dict[str, pd.DataFrame]: Diccionario con DataFrames de cada colección
        """
        import os
        
        archivos_csv = {
            'listings': 'data/listings.csv.gz',
            'reviews': 'data/reviews.csv.gz',
            'calendar': 'data/calendar.csv.gz'
        }
        
        datos = {}
        
        for nombre, archivo in archivos_csv.items():
            if os.path.exists(archivo):
                self.logs.info(f"Cargando {nombre} desde {archivo}")
                print(f"Cargando {nombre}...")
                datos[nombre] = pd.read_csv(archivo, compression='gzip', low_memory=False)
                print(f"  {nombre}: {len(datos[nombre]):,} registros cargados")
            else:
                self.logs.warning(f"Archivo no encontrado: {archivo}")
                print(f"⚠️ Archivo no encontrado: {archivo}")
        
        # Cargar datos para transformación
        self.cargar_datos_para_transformacion(datos)
        
        return datos
    
    def limpiar_valores_nulos_y_duplicados(self, df: pd.DataFrame, nombre_coleccion: str) -> pd.DataFrame:
        """
        Limpia valores nulos y duplicados del DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame a limpiar
            nombre_coleccion (str): Nombre de la colección para logging
            
        Returns:
            pd.DataFrame: DataFrame limpio
        """
        registros_iniciales = len(df)
        
        # Análisis de valores nulos por columna ANTES de eliminar duplicados
        nulos_por_columna = df.isnull().sum()
        columnas_con_nulos = nulos_por_columna[nulos_por_columna > 0]
        
        if len(columnas_con_nulos) > 0:
            self.logs.info(f"{nombre_coleccion}: Columnas con nulos: {len(columnas_con_nulos)}")
            print(f"  {nombre_coleccion}: {len(columnas_con_nulos)} columnas con valores nulos")
            
            # Estrategias de limpieza por tipo de columna
            for columna, nulos in columnas_con_nulos.items():
                # Calcular porcentaje sobre el DataFrame original
                porcentaje_nulos = (nulos / registros_iniciales) * 100
                
                if porcentaje_nulos > 50:
                    # Si más del 50% son nulos, eliminar columna
                    df = df.drop(columns=[columna])
                    self.logs.info(f"{nombre_coleccion}: Columna '{columna}' eliminada ({porcentaje_nulos:.1f}% nulos)")
                    print(f"    Columna '{columna}' eliminada ({porcentaje_nulos:.1f}% nulos)")
                elif porcentaje_nulos > 10:
                    # Si entre 10-50% son nulos, llenar con valores apropiados
                    if df[columna].dtype in ['int64', 'float64']:
                        df[columna] = df[columna].fillna(df[columna].median())
                        self.logs.info(f"{nombre_coleccion}: Columna '{columna}' llenada con mediana")
                    else:
                        df[columna] = df[columna].fillna('No especificado')
                        self.logs.info(f"{nombre_coleccion}: Columna '{columna}' llenada con 'No especificado'")
                else:
                    # Si menos del 10% son nulos, eliminar filas
                    df = df.dropna(subset=[columna])
                    self.logs.info(f"{nombre_coleccion}: Filas con nulos en '{columna}' eliminadas")
        
        # Eliminar duplicados DESPUÉS de limpiar nulos
        try:
            # Identificar columnas con tipos no hasheables (listas, dicts, etc.)
            columnas_problemas = []
            for col in df.columns:
                # Intentar verificar el tipo de datos usando dtype
                if df[col].dtype == 'object':
                    # Para columnas de tipo object, verificar si contienen listas o dicts
                    try:
                        muestra_valores = df[col].dropna().head(100)  # Muestra más grande
                        if len(muestra_valores) > 0:
                            # Verificar si el primer valor es una lista o dict
                            primer_valor = muestra_valores.iloc[0]
                            if isinstance(primer_valor, (list, dict)):
                                columnas_problemas.append(col)
                            else:
                                # Intentar hash en algunos valores
                                for i in range(min(10, len(muestra_valores))):
                                    try:
                                        hash(muestra_valores.iloc[i])
                                    except (TypeError, ValueError):
                                        columnas_problemas.append(col)
                                        break
                    except Exception:
                        columnas_problemas.append(col)
            
            # Si hay columnas problemáticas, excluirlas del análisis de duplicados
            if columnas_problemas:
                self.logs.info(f"{nombre_coleccion}: Columnas con tipos no hasheables excluidas: {len(columnas_problemas)}")
                columnas_validas = [col for col in df.columns if col not in columnas_problemas]
                if len(columnas_validas) > 0:
                    duplicados = df[columnas_validas].duplicated().sum()
                else:
                    duplicados = 0
            else:
                duplicados = df.duplicated().sum()
            
            if duplicados > 0:
                # También excluir columnas problemáticas al eliminar duplicados
                if columnas_problemas:
                    df = df.drop_duplicates(subset=columnas_validas)
                else:
                    df = df.drop_duplicates()
                self.logs.info(f"{nombre_coleccion}: Eliminados {duplicados} duplicados")
                print(f"  {nombre_coleccion}: Eliminados {duplicados} duplicados")
        except Exception as e:
            self.logs.warning(f"Error al verificar duplicados en {nombre_coleccion}: {e}")
            print(f"  Advertencia: Error al verificar duplicados en {nombre_coleccion}: {e}")
        
        registros_finales = len(df)
        eliminados = registros_iniciales - registros_finales
        
        self.logs.info(f"{nombre_coleccion}: Limpieza completada - {eliminados} registros eliminados")
        print(f"  {nombre_coleccion}: {eliminados} registros eliminados ({registros_finales:,} restantes)")
        
        return df
    
    def normalizar_precios(self, df: pd.DataFrame, columnas_precio: List[str]) -> pd.DataFrame:
        """
        Normaliza campos de precio removiendo símbolos y convirtiendo a numérico.
        
        Args:
            df (pd.DataFrame): DataFrame a transformar
            columnas_precio (List[str]): Lista de columnas de precio a normalizar
            
        Returns:
            pd.DataFrame: DataFrame con precios normalizados
        """
        df_normalizado = df.copy()
        
        for columna in columnas_precio:
            if columna in df.columns:
                # Limpiar precios: remover $, comas, espacios
                df_normalizado[columna] = df_normalizado[columna].astype(str).str.replace('$', '')
                df_normalizado[columna] = df_normalizado[columna].str.replace(',', '')
                df_normalizado[columna] = df_normalizado[columna].str.strip()
                
                # Convertir a numérico
                df_normalizado[columna] = pd.to_numeric(df_normalizado[columna], errors='coerce')
                
                # Crear columna de precio normalizado
                columna_normalizada = f"{columna}_normalizado"
                df_normalizado[columna_normalizada] = df_normalizado[columna]
                
                self.logs.info(f"Precios normalizados en columna '{columna}'")
                print(f"  Precios normalizados en columna '{columna}'")
        
        return df_normalizado
    
    def convertir_fechas_a_iso(self, df: pd.DataFrame, columnas_fecha: List[str]) -> pd.DataFrame:
        """
        Convierte campos de fecha a formato ISO (YYYY-MM-DD).
        
        Args:
            df (pd.DataFrame): DataFrame a transformar
            columnas_fecha (List[str]): Lista de columnas de fecha a convertir
            
        Returns:
            pd.DataFrame: DataFrame con fechas en formato ISO
        """
        df_fechas = df.copy()
        
        for columna in columnas_fecha:
            if columna in df.columns:
                # Convertir a datetime
                df_fechas[columna] = pd.to_datetime(df_fechas[columna], errors='coerce')
                
                # Crear columna en formato ISO
                columna_iso = f"{columna}_iso"
                df_fechas[columna_iso] = df_fechas[columna].dt.strftime('%Y-%m-%d')
                
                self.logs.info(f"Fechas convertidas a ISO en columna '{columna}'")
                print(f"  Fechas convertidas a ISO en columna '{columna}'")
        
        return df_fechas
    
    def derivar_variables_temporales(self, df: pd.DataFrame, columna_fecha: str) -> pd.DataFrame:
        """
        Deriva variables temporales (mes, año, día, trimestre) a partir de una columna de fecha.
        
        Args:
            df (pd.DataFrame): DataFrame a transformar
            columna_fecha (str): Columna de fecha base
            
        Returns:
            pd.DataFrame: DataFrame con variables temporales derivadas
        """
        df_temporal = df.copy()
        
        if columna_fecha in df.columns:
            # Asegurar que la columna sea datetime
            df_temporal[columna_fecha] = pd.to_datetime(df_temporal[columna_fecha], errors='coerce')
            
            # Derivar variables temporales
            df_temporal[f'{columna_fecha}_año'] = df_temporal[columna_fecha].dt.year
            df_temporal[f'{columna_fecha}_mes'] = df_temporal[columna_fecha].dt.month
            df_temporal[f'{columna_fecha}_dia'] = df_temporal[columna_fecha].dt.day
            df_temporal[f'{columna_fecha}_trimestre'] = df_temporal[columna_fecha].dt.quarter
            df_temporal[f'{columna_fecha}_dia_semana'] = df_temporal[columna_fecha].dt.day_name()
            df_temporal[f'{columna_fecha}_mes_nombre'] = df_temporal[columna_fecha].dt.month_name()
            
            self.logs.info(f"Variables temporales derivadas de '{columna_fecha}'")
            print(f"  Variables temporales derivadas de '{columna_fecha}'")
        
        return df_temporal
    
    def categorizar_precios(self, df: pd.DataFrame, columna_precio: str) -> pd.DataFrame:
        """
        Categoriza precios en rangos para análisis.
        
        Args:
            df (pd.DataFrame): DataFrame a transformar
            columna_precio (str): Columna de precio a categorizar
            
        Returns:
            pd.DataFrame: DataFrame con categorías de precio
        """
        df_categorizado = df.copy()
        
        if columna_precio in df.columns:
            # Definir rangos de precios
            precios_validos = df_categorizado[columna_precio].dropna()
            
            if len(precios_validos) > 0:
                # Calcular percentiles para rangos dinámicos
                p25 = precios_validos.quantile(0.25)
                p50 = precios_validos.quantile(0.50)
                p75 = precios_validos.quantile(0.75)
                
                # Crear categorías
                def categorizar_precio(precio):
                    if pd.isna(precio):
                        return 'No especificado'
                    elif precio <= p25:
                        return 'Económico'
                    elif precio <= p50:
                        return 'Moderado'
                    elif precio <= p75:
                        return 'Caro'
                    else:
                        return 'Muy caro'
                
                df_categorizado[f'{columna_precio}_categoria'] = df_categorizado[columna_precio].apply(categorizar_precio)
                
                # Mostrar distribución de categorías
                distribucion = df_categorizado[f'{columna_precio}_categoria'].value_counts()
                self.logs.info(f"Categorías de precio creadas: {dict(distribucion)}")
                print(f"  Categorías de precio creadas:")
                for categoria, cantidad in distribucion.items():
                    print(f"    {categoria}: {cantidad:,} propiedades")
        
        return df_categorizado
    
    def expandir_campos_anidados(self, df: pd.DataFrame, columnas_anidadas: List[str]) -> pd.DataFrame:
        """
        Expande campos anidados (JSON) en columnas separadas.
        
        Args:
            df (pd.DataFrame): DataFrame a transformar
            columnas_anidadas (List[str]): Lista de columnas anidadas a expandir
            
        Returns:
            pd.DataFrame: DataFrame con campos anidados expandidos
        """
        df_expandido = df.copy()
        
        for columna in columnas_anidadas:
            if columna in df.columns:
                try:
                    # Intentar parsear como JSON solo si es string
                    def parsear_valor(valor):
                        if pd.isna(valor):
                            return valor
                        if isinstance(valor, (list, dict)):
                            return valor
                        if isinstance(valor, str):
                            try:
                                if valor.startswith('[') or valor.startswith('{'):
                                    return json.loads(valor)
                                # Si es string vacío, retornar lista vacía
                                if valor.strip() == '':
                                    return []
                            except (json.JSONDecodeError, ValueError):
                                return valor
                        return valor
                    
                    df_expandido[columna] = df_expandido[columna].apply(parsear_valor)
                    
                    # Expandir amenities en columnas binarias
                    if columna == 'amenities':
                        # Obtener todas las amenities únicas
                        todas_amenities = set()
                        for amenities_list in df_expandido[columna].dropna():
                            # Convertir a lista si es array de numpy
                            if isinstance(amenities_list, (list, tuple)):
                                todas_amenities.update(amenities_list)
                            elif isinstance(amenities_list, np.ndarray):
                                todas_amenities.update(amenities_list.tolist())
                        
                        # Crear columnas binarias para cada amenity
                        def verificar_amenity(amenity_item):
                            def check_en_lista(valor):
                                if pd.isna(valor):
                                    return 0
                                try:
                                    if isinstance(valor, (list, tuple)):
                                        return 1 if amenity_item in valor else 0
                                    elif isinstance(valor, np.ndarray):
                                        return 1 if amenity_item in valor.tolist() else 0
                                    else:
                                        return 0
                                except Exception:
                                    return 0
                            return check_en_lista
                        
                        for amenity in todas_amenities:
                            columna_amenity = f'amenity_{amenity.lower().replace(" ", "_").replace("-", "_")}'
                            df_expandido[columna_amenity] = df_expandido[columna].apply(verificar_amenity(amenity))
                        
                        self.logs.info(f"Amenities expandidas: {len(todas_amenities)} columnas creadas")
                        print(f"  Amenities expandidas: {len(todas_amenities)} columnas creadas")
                    
                    # Expandir host_verifications
                    elif columna == 'host_verifications':
                        # Obtener todas las verificaciones únicas
                        todas_verificaciones = set()
                        for verif_list in df_expandido[columna].dropna():
                            # Convertir a lista si es array de numpy
                            if isinstance(verif_list, (list, tuple)):
                                todas_verificaciones.update(verif_list)
                            elif isinstance(verif_list, np.ndarray):
                                todas_verificaciones.update(verif_list.tolist())
                        
                        # Crear columnas binarias para cada verificación
                        def verificar_verification(verificacion_item):
                            def check_en_lista(valor):
                                if pd.isna(valor):
                                    return 0
                                try:
                                    if isinstance(valor, (list, tuple)):
                                        return 1 if verificacion_item in valor else 0
                                    elif isinstance(valor, np.ndarray):
                                        return 1 if verificacion_item in valor.tolist() else 0
                                    else:
                                        return 0
                                except Exception:
                                    return 0
                            return check_en_lista
                        
                        for verificacion in todas_verificaciones:
                            columna_verif = f'verification_{verificacion.lower().replace(" ", "_")}'
                            df_expandido[columna_verif] = df_expandido[columna].apply(verificar_verification(verificacion))
                        
                        self.logs.info(f"Verificaciones expandidas: {len(todas_verificaciones)} columnas creadas")
                        print(f"  Verificaciones expandidas: {len(todas_verificaciones)} columnas creadas")
                
                except Exception as e:
                    self.logs.warning(f"Error al expandir columna '{columna}': {e}")
                    print(f"    Advertencia: Error al expandir columna '{columna}': {e}")
        
        return df_expandido
    
    def transformar_coleccion_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma específicamente la colección listings.
        
        Args:
            df (pd.DataFrame): DataFrame de listings
            
        Returns:
            pd.DataFrame: DataFrame transformado
        """
        self.logs.info("Iniciando transformación de colección listings")
        print("Transformando colección listings...")
        
        # 1. Limpiar valores nulos y duplicados
        df_limpio = self.limpiar_valores_nulos_y_duplicados(df, 'listings')
        
        # 2. Normalizar precios
        columnas_precio = ['price', 'host_response_rate', 'host_acceptance_rate']
        df_precios = self.normalizar_precios(df_limpio, columnas_precio)
        
        # 3. Convertir fechas
        columnas_fecha = ['host_since', 'first_review', 'last_review', 'last_scraped']
        df_fechas = self.convertir_fechas_a_iso(df_precios, columnas_fecha)
        
        # 4. Derivar variables temporales de host_since
        df_temporal = self.derivar_variables_temporales(df_fechas, 'host_since')
        
        # 5. Categorizar precios
        df_categorizado = self.categorizar_precios(df_temporal, 'price')
        
        # 6. Expandir campos anidados
        columnas_anidadas = ['amenities', 'host_verifications']
        df_final = self.expandir_campos_anidados(df_categorizado, columnas_anidadas)
        
        self.logs.info("Transformación de listings completada")
        print("  Transformación de listings completada")
        
        return df_final
    
    def transformar_coleccion_reviews(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma específicamente la colección reviews.
        
        Args:
            df (pd.DataFrame): DataFrame de reviews
            
        Returns:
            pd.DataFrame: DataFrame transformado
        """
        self.logs.info("Iniciando transformación de colección reviews")
        print("Transformando colección reviews...")
        
        # 1. Limpiar valores nulos y duplicados
        df_limpio = self.limpiar_valores_nulos_y_duplicados(df, 'reviews')
        
        # 2. Convertir fechas
        columnas_fecha = ['date']
        df_fechas = self.convertir_fechas_a_iso(df_limpio, columnas_fecha)
        
        # 3. Derivar variables temporales
        df_temporal = self.derivar_variables_temporales(df_fechas, 'date')
        
        self.logs.info("Transformación de reviews completada")
        print("  Transformación de reviews completada")
        
        return df_temporal
    
    def transformar_coleccion_calendar(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma específicamente la colección calendar.
        
        Args:
            df (pd.DataFrame): DataFrame de calendar
            
        Returns:
            pd.DataFrame: DataFrame transformado
        """
        self.logs.info("Iniciando transformación de colección calendar")
        print("Transformando colección calendar...")
        
        # 1. Limpiar valores nulos y duplicados
        df_limpio = self.limpiar_valores_nulos_y_duplicados(df, 'calendar')
        
        # 2. Convertir fechas
        columnas_fecha = ['date']
        df_fechas = self.convertir_fechas_a_iso(df_limpio, columnas_fecha)
        
        # 3. Derivar variables temporales
        df_temporal = self.derivar_variables_temporales(df_fechas, 'date')
        
        # 4. Normalizar precios (si existen)
        columnas_precio = ['price', 'adjusted_price']
        df_precios = self.normalizar_precios(df_temporal, columnas_precio)
        
        self.logs.info("Transformación de calendar completada")
        print("  Transformación de calendar completada")
        
        return df_precios
    
    def ejecutar_transformacion_completa(self) -> Dict[str, pd.DataFrame]:
        """
        Ejecuta la transformación completa de todas las colecciones.
        
        Returns:
            Dict[str, pd.DataFrame]: Diccionario con DataFrames transformados
        """
        print("INICIANDO TRANSFORMACION COMPLETA DE DATOS")
        print("=" * 60)
        
        self.datos_transformados = {}
        
        try:
            # Transformar cada colección
            if 'listings' in self.datos_originales:
                self.datos_transformados['listings'] = self.transformar_coleccion_listings(
                    self.datos_originales['listings']
                )
            
            if 'reviews' in self.datos_originales:
                self.datos_transformados['reviews'] = self.transformar_coleccion_reviews(
                    self.datos_originales['reviews']
                )
            
            if 'calendar' in self.datos_originales:
                self.datos_transformados['calendar'] = self.transformar_coleccion_calendar(
                    self.datos_originales['calendar']
                )
            
            # Generar estadísticas de transformación
            self._generar_estadisticas_transformacion()
            
            print("\nTRANSFORMACION COMPLETADA EXITOSAMENTE!")
            print("=" * 60)
            
            return self.datos_transformados
            
        except Exception as e:
            self.logs.error(f"Error durante la transformación: {e}")
            print(f"ERROR durante la transformación: {e}")
            raise
    
    def _generar_estadisticas_transformacion(self):
        """
        Genera estadísticas de la transformación realizada.
        """
        print("\nESTADISTICAS DE TRANSFORMACION:")
        print("-" * 40)
        
        for nombre, df_original in self.datos_originales.items():
            if nombre in self.datos_transformados:
                df_transformado = self.datos_transformados[nombre]
                
                registros_originales = len(df_original)
                registros_finales = len(df_transformado)
                columnas_originales = len(df_original.columns)
                columnas_finales = len(df_transformado.columns)
                
                self.estadisticas_transformacion[nombre] = {
                    'registros_originales': registros_originales,
                    'registros_finales': registros_finales,
                    'registros_eliminados': registros_originales - registros_finales,
                    'columnas_originales': columnas_originales,
                    'columnas_finales': columnas_finales,
                    'columnas_agregadas': columnas_finales - columnas_originales
                }
                
                print(f"{nombre.upper()}:")
                print(f"  Registros: {registros_originales:,} -> {registros_finales:,} ({registros_originales - registros_finales:,} eliminados)")
                print(f"  Columnas: {columnas_originales} -> {columnas_finales} ({columnas_finales - columnas_originales} agregadas)")
        
        # Guardar estadísticas en archivo
        with open('estadisticas_transformacion.json', 'w', encoding='utf-8') as f:
            json.dump(self.estadisticas_transformacion, f, indent=2, ensure_ascii=False)
        
        print(f"\nEstadísticas guardadas en: estadisticas_transformacion.json")
    
    def obtener_dataframe_limpio(self, nombre_coleccion: str) -> pd.DataFrame:
        """
        Obtiene el DataFrame limpio y transformado de una colección específica.
        
        Args:
            nombre_coleccion (str): Nombre de la colección
            
        Returns:
            pd.DataFrame: DataFrame transformado
        """
        if nombre_coleccion in self.datos_transformados:
            return self.datos_transformados[nombre_coleccion]
        else:
            raise ValueError(f"Colección '{nombre_coleccion}' no encontrada en datos transformados")
    
    def guardar_datos_transformados(self, ruta_destino: str = 'datos_transformados'):
        """
        Guarda los datos transformados en archivos CSV.
        
        Args:
            ruta_destino (str): Ruta donde guardar los archivos
        """
        import os
        
        if not os.path.exists(ruta_destino):
            os.makedirs(ruta_destino)
        
        for nombre, df in self.datos_transformados.items():
            archivo = f"{ruta_destino}/{nombre}_transformado.csv"
            df.to_csv(archivo, index=False, encoding='utf-8')
            self.logs.info(f"Datos transformados guardados: {archivo}")
            print(f"Datos transformados guardados: {archivo}")


# Ejemplo de uso
if __name__ == "__main__":
    """
    Ejemplo de uso de la clase Transformacion.
    
    Este bloque demuestra cómo usar la clase para transformar datos de Airbnb.
    """
    try:
        # Importar las clases necesarias
        from extraccion import ExtraccionWindows
        
        # Crear instancia del extractor
        print("Conectando a MongoDB...")
        extractor = ExtraccionWindows()
        
        # Extraer datos
        print("Extrayendo datos...")
        datos_originales = {
            'listings': extractor.obtener_listings(),
            'reviews': extractor.obtener_reviews(),
            'calendar': extractor.obtener_calendar()
        }
        
        # Crear instancia de transformación
        transformador = Transformacion()
        
        # Cargar datos para transformación
        transformador.cargar_datos_para_transformacion(datos_originales)
        
        # Ejecutar transformación completa
        datos_transformados = transformador.ejecutar_transformacion_completa()
        
        # Guardar datos transformados
        transformador.guardar_datos_transformados()
        
        # Cerrar conexión
        extractor.cerrar_conexion()
        
        print("\nProceso de transformación completado exitosamente!")
        
    except Exception as e:
        print(f"ERROR durante la transformación: {e}")
        print("Asegúrate de que MongoDB esté ejecutándose y que los datos estén cargados.")
