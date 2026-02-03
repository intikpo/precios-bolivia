import logging
import time
import pandas as pd
from pathlib import Path
from typing import List, Dict
from src.utils.shopify import get_all_products, extract_product_data
from src.utils.products import productos_unicos
from src.config import DATA_DIR, REQUEST_DELAY

logger = logging.getLogger(__name__)
    
def scrape_farmacorp(config: dict) -> List[Dict]:
    """
    Ejecuta el scraping completo de Farmacorp
    
    Args:
        config: Diccionario con configuración del scraper
        
    Returns:
        Lista de diccionarios con precios (IdProducto, PrecioVenta, PrecioOriginal)
    """
    logger.info("="*20)
    logger.info("INICIANDO SCRAPER FARMACORP")
    
    base_url = config['base_url']
    delay = REQUEST_DELAY
    
    # 1. Obtener todos los productos desde products.json
    logger.info("PASO 1: Obteniendo productos...")
    productos = get_all_products(base_url, limit=250, delay=delay)
    
    if not productos:
        logger.error("No se obtuvieron productos")
        return []
    
    # 2. Procesar productos
    logger.info("PASO 2: Procesando productos...")
    all_precios = []
    all_productos_maestro = []
    
    for producto in productos:
        try:
            # Extraer datos básicos
            data = extract_product_data(producto)
            
            if not data or not data['IdProducto']:
                continue
            
            sku = data['IdProducto']
            
            # Datos para archivo diario de precios
            precio_data = {
                'IdProducto': sku,
                'PrecioVenta': data['PrecioVenta'],
                'PrecioOriginal': data['PrecioOriginal'],
            }
            all_precios.append(precio_data)
            
            # Datos para maestro de productos
            producto_maestro = {
                'IdProducto': sku,
                'Descripcion': data['Descripcion'],
            }
            all_productos_maestro.append(producto_maestro)
            
        except Exception as e:
            logger.error(f"Error procesando producto {producto.get('id')}: {e}")
    
    # 3. Guardar maestro de productos
    logger.info("PASO 3: Guardando listado de productos...")
    productos_unicos(all_productos_maestro, source='farmacorp')
    
    logger.info("="*20)
    logger.info(f"SCRAPER FARMACORP FINALIZADO")
    logger.info(f"  - Productos procesados: {len(all_precios)}")
    
    return all_precios