"""
Utilidades genéricas para scraping de tiendas Shopify
Funciones reutilizables para cualquier e-commerce basado en Shopify
"""

import requests
import time
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _make_request(url: str, timeout: int = 15) -> requests.Response:
    """
    Realiza request HTTP con reintentos automáticos
    
    Args:
        url: URL a consultar
        timeout: Timeout en segundos
        
    Returns:
        Response object de requests
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response


def get_all_products(base_url: str, limit: int = 250, delay: float = 1.0, timeout: int = 15) -> List[Dict]:
    """
    Obtiene todos los productos desde /products.json con paginación
    
    Args:
        base_url: URL base de la tienda (ej: 'https://farmacorp.com')
        limit: Cantidad de productos por página
        delay: Delay en segundos entre requests
        timeout: Timeout para cada request
        
    Returns:
        Lista de productos (diccionarios)
    """
    all_products = []
    page = 1
    
    while True:
        url = f"{base_url}/products.json?limit={limit}&page={page}"
        
        try:
            response = _make_request(url, timeout=timeout)
            data = response.json()
            
            products = data.get('products', [])
            
            if not products:
                logger.info(f"No hay más productos. Total páginas: {page - 1}")
                break
            
            all_products.extend(products)
            
            page += 1
            time.sleep(delay)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error obteniendo página {page}: {e}")
            break
    
    logger.info(f"Total productos obtenidos: {len(all_products)}")
    return all_products


def extract_product_data(product: Dict) -> Optional[Dict]:
    """
    Extrae datos relevantes de un producto Shopify
    
    Args:
        product: Diccionario con datos del producto
        
    Returns:
        Diccionario con datos extraídos o None si no hay variantes
    """
    try:
        # Obtener primera variante
        variants = product.get('variants', [])
        if not variants:
            return None
        
        variant = variants[0]
        
        # Extraer datos
        sku = variant.get('sku', '')
        price = variant.get('price', '0')
        compare_at_price = variant.get('compare_at_price') or '0'
        
        return {
            'IdProducto': sku,
            'Descripcion': product.get('title', ''),
            'PrecioVenta': price,
            'PrecioOriginal': compare_at_price,
        }
        
    except Exception as e:
        logger.error(f"Error extrayendo datos de producto {product.get('id')}: {e}")
        return None