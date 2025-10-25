import sys
import os
from datetime import datetime

# Agregar el directorio scr al path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'scr'))

def print_banner():
    print("=" * 50)
    print("ETL PIPELINE - AIRBNB")
    print("=" * 50)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

def check_dependencies():
    """Verifica que las dependencias estén instaladas."""
    try:
        import pandas as pd
        import numpy as np
        import pymongo
        import mysql.connector
        print("Dependencias Listas")
        return True
    except ImportError as e:
        print(f"Error: {e}")
        print("Ejecuta: pip install -r requirements.txt")
        return False

def run_extraction():
    """Ejecuta el paso de extracción."""
    print("\nEXTRAYENDO DATOS...")
    try:
        from scr.extraccion import ExtraccionWindows
        extractor = ExtraccionWindows()
        
        datos = {
            'listings': extractor.obtener_listings(),
            'reviews': extractor.obtener_reviews(),
            'calendar': extractor.obtener_calendar()
        }
        
        print("Extracción completada")
        print(f"   Listings: {len(datos['listings']):,} registros")
        print(f"   Reviews: {len(datos['reviews']):,} registros") 
        print(f"   Calendar: {len(datos['calendar']):,} registros")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def run_transformation():
    """Ejecuta el paso de transformación."""
    print("\nTRANSFORMANDO DATOS...")
    try:
        from scr.transformacion import Transformacion
        
        # Verificar archivos CSV
        csv_files = ['data/listings.csv.gz', 'data/reviews.csv.gz', 'data/calendar.csv.gz']
        missing = [f for f in csv_files if not os.path.exists(f)]
        if missing:
            print(f"Archivos no encontrados: {missing}")
            print("Ejecuta primero: Opción 1 (Extracción)")
            return False
        
        transformador = Transformacion()
        datos = transformador.cargar_datos_desde_csv()
        datos_transformados = transformador.ejecutar_transformacion_completa()
        transformador.guardar_datos_transformados()
        
        print("\nTransformación completada")
        print("   Archivos en: datos_transformados/")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def run_load():
    """Ejecuta el paso de carga."""
    print("\nCARGANDO DATOS...")
    try:
        from scr.carga import CargaMySQL
        
        # Verificar archivos transformados
        csv_files = [
            'datos_transformados/listings_transformado.csv',
            'datos_transformados/reviews_transformado.csv', 
            'datos_transformados/calendar_transformado.csv'
        ]
        missing = [f for f in csv_files if not os.path.exists(f)]
        if missing:
            print(f"❌ Archivos no encontrados: {missing}")
            print("Ejecuta primero: Opción 2 (Transformación)")
            return False
        
        print("Configura MySQL en scr/carga.py (líneas 340-345)")
        
        cargador = CargaMySQL(
            host='localhost', port=3306, database='airbnb',
            user='root', password=''
        )
        
        # Cargar datos transformados desde CSV
        import pandas as pd
        
        datos_transformados = {}
        archivos_csv = {
            'listings': 'datos_transformados/listings_transformado.csv',
            'reviews': 'datos_transformados/reviews_transformado.csv',
            'calendar': 'datos_transformados/calendar_transformado.csv'
        }
        
        print("Cargando datos transformados...")
        for nombre, archivo in archivos_csv.items():
            if os.path.exists(archivo):
                print(f"  Cargando {nombre}...")
                datos_transformados[nombre] = pd.read_csv(archivo, low_memory=False)
                print(f"  {nombre}: {len(datos_transformados[nombre]):,} registros")
        
        # Preparar datos para carga
        cargador.cargar_datos_transformados(datos_transformados)
        
        # Ejecutar carga
        reporte = cargador.ejecutar_carga_completa(datos_transformados)
        
        print("Carga completada")
        print(f"   Total: {reporte['resumen']['total_registros']:,} registros")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def run_eda():
    """Muestra instrucciones para EDA."""
    print("\nANÁLISIS EXPLORATORIO")
    print("-" * 30)
    print("1. Abre: jupyter notebook notebooks/exploracion_airbnb.ipynb")
    print("2. Ejecuta todas las celdas")
    print("3. Revisa gráficos en imagenes/")
    print("4. Continúa con: Opción 2 (Transformación)")

def show_menu():
    """Muestra el menú principal."""
    print("\n" + "="*50)
    print("ETL PIPELINE - AIRBNB")
    print("="*50)
    print("Selecciona una opción:")
    print("1. Extracción (MongoDB → CSV)")
    print("2. Transformación (CSV → CSV limpio)")
    print("3. Carga (CSV → MySQL)")
    print("4. Análisis Exploratorio (EDA)")
    print("5. Pipeline completo (1+2+3)")
    print("0. Salir")
    print("="*50)

def main():
    """Función principal con menú interactivo."""
    print_banner()
    
    if not check_dependencies():
        return
    
    while True:
        show_menu()
        try:
            opcion = input("\nIngresa tu opción (0-5): ").strip()
            
            if opcion == "0":
                print("\n¡Hasta luego!")
                break
            elif opcion == "1":
                run_extraction()
            elif opcion == "2":
                run_transformation()
            elif opcion == "3":
                run_load()
            elif opcion == "4":
                run_eda()
            elif opcion == "5":
                print("\nEJECUTANDO PIPELINE COMPLETO...")
                success = True
                if not run_extraction():
                    success = False
                if not run_transformation():
                    success = False
                if not run_load():
                    success = False
                
                print("\n" + "="*50)
                if success:
                    print("PIPELINE COMPLETADO EXITOSAMENTE")
                else:
                    print("PIPELINE COMPLETADO CON ERRORES")
                print("="*50)
            else:
                print("Opción inválida. Intenta de nuevo.")
                
        except KeyboardInterrupt:
            print("\n\n👋 ¡Hasta luego!")
            break
        except Exception as e:
            print(f"Error inesperado: {e}")

if __name__ == "__main__":
    main()
