"""
Sistema de Logs Unificado para Análisis de Datos Airbnb
====================================================

Esta clase proporciona un sistema de logging unificado para todos los scripts
del proyecto, generando archivos de log por ejecución con timestamps.

Autor: Taller de Análisis de Datos Airbnb
Fecha: 2024
"""

import logging
import os
from datetime import datetime
from typing import Optional

class Logs:
    """
    Clase para manejo unificado de logs en todos los scripts del proyecto.
    
    Genera archivos de log por ejecución con formato: logs/log_YYYYMMDD_HHMM.txt
    Registra mensajes con niveles INFO, WARNING y ERROR.
    """
    
    def __init__(self, nombre_script: str):
        """
        Inicializa el sistema de logs.
        
        Args:
            nombre_script (str): Nombre del script que está ejecutando
        """
        self.nombre_script = nombre_script
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.logger = None
        self._configurar_logging()
    
    def _configurar_logging(self):
        """
        Configura el sistema de logging con archivo por ejecución.
        """
        # Crear directorio de logs si no existe
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # Nombre del archivo de log
        nombre_archivo = f'logs/log_{self.timestamp}.txt'
        
        # Configurar logger
        self.logger = logging.getLogger(f'{self.nombre_script}_{self.timestamp}')
        self.logger.setLevel(logging.INFO)
        
        # Limpiar handlers existentes
        self.logger.handlers.clear()
        
        # Crear handler para archivo
        file_handler = logging.FileHandler(nombre_archivo, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Crear formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        # Agregar handler al logger
        self.logger.addHandler(file_handler)
        
        # Log inicial
        self.info(f"Iniciando {self.nombre_script}")
        print(f"Log iniciado: {nombre_archivo}")
    
    def info(self, mensaje: str):
        """
        Registra un mensaje de nivel INFO.
        
        Args:
            mensaje (str): Mensaje a registrar
        """
        self.logger.info(mensaje)
        print(f"INFO: {mensaje}")
    
    def warning(self, mensaje: str):
        """
        Registra un mensaje de nivel WARNING.
        
        Args:
            mensaje (str): Mensaje a registrar
        """
        self.logger.warning(mensaje)
        print(f"WARNING: {mensaje}")
    
    def error(self, mensaje: str):
        """
        Registra un mensaje de nivel ERROR.
        
        Args:
            mensaje (str): Mensaje a registrar
        """
        self.logger.error(mensaje)
        print(f"ERROR: {mensaje}")
    
    def registrar_inicio_operacion(self, operacion: str):
        """
        Registra el inicio de una operación.
        
        Args:
            operacion (str): Nombre de la operación
        """
        self.info(f"Iniciando operación: {operacion}")
    
    def registrar_fin_operacion(self, operacion: str, resultado: str = "completada"):
        """
        Registra el fin de una operación.
        
        Args:
            operacion (str): Nombre de la operación
            resultado (str): Resultado de la operación
        """
        self.info(f"Operación {operacion} {resultado}")
    
    def registrar_estadisticas(self, estadisticas: dict):
        """
        Registra estadísticas de una operación.
        
        Args:
            estadisticas (dict): Diccionario con estadísticas
        """
        self.info("Estadísticas de la operación:")
        for clave, valor in estadisticas.items():
            self.info(f"  {clave}: {valor}")
    
    def registrar_error_detallado(self, error: Exception, contexto: str = ""):
        """
        Registra un error con detalles completos.
        
        Args:
            error (Exception): Excepción capturada
            contexto (str): Contexto donde ocurrió el error
        """
        mensaje = f"Error en {contexto}: {str(error)}"
        self.error(mensaje)
        self.error(f"Tipo de error: {type(error).__name__}")
    
    def cerrar_log(self):
        """
        Cierra el sistema de logging.
        """
        self.info(f"Finalizando {self.nombre_script}")
        print("Log cerrado correctamente")
