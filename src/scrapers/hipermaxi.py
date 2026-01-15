import requests
import time
import logging
import pandas as pd
from typing import List, Dict
from pathlib import Path
from ..config import TIMEOUT, REQUEST_DELAY
from ..config import DATA_DIR
from ..utils.auth import get_authenticated_session

logger = logging.getLogger(__name__)

def get_session() -> tuple:
    """Crea sesión con headers autenticados"""
    try:
        session, headers = get_authenticated_session(TIMEOUT)
        session.verify = False
        return session, headers
    except Exception as e:
        logger.error(f"Autenticación falló: {e}")
        raise

def get_sucursales(session: requests.Session, headers: dict, base_url: str, 
                   tipo_servicio_filter: list) -> List[Dict]:
    """Obtiene todas las sucursales activas"""
    try:
        url = f"{base_url}/public/markets/activos?IdMarket=0&IdTipoServicio=0"
        response = session.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get('ConError') or data.get('Estado') != 200:
            logger.error(f"Error en API sucursales: {data.get('Mensaje')}")
            return []
        
        sucursales = []
        for market in data['Dato']:
            for locatario in market.get('Locatarios', []):
                # Filtrar por tipo de servicio
                if locatario.get('IdTipoServicio') in tipo_servicio_filter:
                    sucursales.append({
                        'IdMarket': market['IdMarket'],
                        'IdRegion': market['IdRegion'],
                        'IdSucursal': locatario['IdSucursal'],
                        'Descripcion': locatario['Descripcion'],
                        'Abreviacion': locatario.get('Abreviacion', ''),
                        'IdTipoServicio': locatario.get('IdTipoServicio'),
                        'Direccion': locatario.get('Direccion', '')
                    })
        
        logger.info(f"Sucursales obtenidas: {len(sucursales)}")
        return sucursales
        
    except Exception as e:
        logger.error(f"Error obteniendo sucursales: {e}")
        return []

def get_categorias(session: requests.Session, headers: dict, base_url: str,
                   id_market: int, id_sucursal: int) -> List[Dict]:
    """Obtiene categorías para una sucursal"""
    try:
        url = f"{base_url}/markets/clasificaciones"
        params = {'IdMarket': id_market, 'IdSucursal': id_sucursal}
        response = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get('ConError') or data.get('Estado') != 200:
            return []
        
        categorias_flat = []
        for rubro in data['Dato']:
            for categoria in rubro.get('Categorias', []):
                categorias_flat.append({
                    'IdRubro': rubro['IdRubro'],
                    'IdCategoria': categoria['IdCategoria'],
                    'DescripcionRubro': rubro['Descripcion'],
                    'DescripcionCategoria': categoria['Descripcion'],
                })
        return categorias_flat

        
    except Exception as e:
        logger.error(f"Error obteniendo categorías: {e}")
        return []
    
def get_categorias_subcategorias(session: requests.Session, headers: dict, base_url: str,
                   id_market: int, id_sucursal: int) -> List[Dict]:
    """Obtiene categorías y subcategorías para una sucursal"""
    try:
        url = f"{base_url}/markets/clasificaciones"
        params = {'IdMarket': id_market, 'IdSucursal': id_sucursal}
        response = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get('ConError') or data.get('Estado') != 200:
            return []
        
        categorias_flat = []
        for rubro in data['Dato']:
            for categoria in rubro.get('Categorias', []):
                for subcategoria in categoria.get('SubCategorias', []):
                    categorias_flat.append({
                        'IdRubro': rubro['IdRubro'],
                        'IdCategoria': categoria['IdCategoria'],
                        'IdSubcategoria': subcategoria['IdSubcategoria'],
                        'DescripcionRubro': rubro['Descripcion'],
                        'DescripcionCategoria': categoria['Descripcion'],
                        'DescripcionSubcategoria': subcategoria['Descripcion']
                    })
        return categorias_flat
        
    except Exception as e:
        logger.error(f"Error obteniendo categorías: {e}")
        return []

def get_productos(session: requests.Session, headers: dict, base_url: str,
                 id_market: int, id_locatario: int, id_categoria: int = None,
                 id_subcategoria: int = None) -> List[Dict]:
    """
    Obtiene productos de una categoría específica con paginación
    """
    productos = []
    pagina = 1
    cantidad = 1000
    
    while True:
        try:
            url = f"{base_url}/public/productos"
            params = {
                'IdMarket': id_market,
                'IdLocatario': id_locatario,
                'Pagina': pagina,
                'Cantidad': cantidad,
            }
            
            if id_subcategoria is not None:
                params['IdsSubcategoria[0]'] = id_subcategoria
            
            response = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
            #logger.info("URL real ejecutada: %s", response.url)
            response.raise_for_status()
            data = response.json()
            
            if data.get('ConError') or data.get('Estado') != 200:
                break
            
            datos = data.get('Dato', [])
            if not datos:
                break
            
            productos.extend(datos)
            
            if len(datos) < cantidad:
                break
            
            pagina += 1
            time.sleep(REQUEST_DELAY)
            
        except Exception as e:
            logger.error(f"Error obteniendo productos página {pagina}: {e}")
            break
    
    return productos

def productos_unicos(data):
    """
    Obtenemos listado de productos id y descripción.
    Guardamos en un archivo productos.csv.
    """
    # guardamos los datos como DF y ordenamos
    df = pd.DataFrame(data)
    df_nuevo = df[['IdProducto', 'Descripcion']].copy()
    df_nuevo['IdProducto'] = pd.to_numeric(df_nuevo['IdProducto'], errors='coerce')
    df_nuevo = df_nuevo.drop_duplicates(subset=['IdProducto'], keep='first')
    df_nuevo = df_nuevo.sort_values(by='IdProducto')
    
    # Verificar si el archivo existe
    filename = 'hipermaxi/productos.csv'
    filepath = DATA_DIR / filename
    filepath = Path(filepath)
    
    if filepath.exists():
        # Cargar datos antiguos
        df_antiguo = pd.read_csv(filepath)
        
        # Identificar productos nuevos
        nuevos = df_nuevo[~df_nuevo['IdProducto'].isin(df_antiguo['IdProducto'])]
        
        if len(nuevos) > 0:
            # Hay productos nuevos, combinar y guardar
            df_combinado = pd.concat([df_antiguo, nuevos]).drop_duplicates(
                subset='IdProducto', 
                keep='last'
            )
            df_combinado.to_csv(filepath, index=False)
            logger.info(f"{len(nuevos)} productos nuevos agregados. Archivo actualizado.")
        else:
            logger.info("No hay productos nuevos. Archivo sin cambios.")
    else:
        # Primera vez, creamos archivo
        logger.info("No existe el archivo. Creando archivo nuevo...")
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df_nuevo.to_csv(filepath, index=False)
        logger.info(f"Archivo creado con {len(df_nuevo)} productos en: {filepath}")
    
def scrape_hipermaxi(config: dict) -> List[Dict]:
    """Ejecuta el scraping completo de Hipermaxi"""
    logger.info("="*20)
    logger.info("INICIANDO SCRAPING: HIPERMAXI")
    
    base_url = config['base_url']
    web_url = config['web_url']
    tipo_servicio_filter = config.get('tipo_servicio_filter', [1])
    
    # Crear sesión autenticada
    session, headers = get_session()
    
    #sucursales = get_sucursales(session, headers, base_url, tipo_servicio_filter)
    # definimos las sucursales con las que se trabajará
    sucursales = [
                    {'IdMarket':67, 
                     'IdSucursal':67, 
                     'Descripcion': 'HIPERMAXI ROCA Y CORONADO',
                     'IdRegion': 1},
                    {'IdMarket':85, 
                     'IdSucursal':85, 
                     'Descripcion': 'HIPERMAXI SUPERCENTER',
                     'IdRegion': 1},
                    {'IdMarket':34, 
                     'IdSucursal':34, 
                     'Descripcion': 'HIPERMAXI ACHUMANI',
                     'IdRegion': 2},
                    {'IdMarket':36, 
                     'IdSucursal':36, 
                     'Descripcion': 'HIPERMAXI EL POETA',
                     'IdRegion': 2},
                    {'IdMarket':47, 
                     'IdSucursal':47, 
                     'Descripcion': 'HIPERMAXI JUAN DE LA ROSA',
                     'IdRegion': 3},
                    {'IdMarket':48, 
                     'IdSucursal':48, 
                     'Descripcion': 'HIPERMAXI SACABA',
                     'IdRegion': 3},
                  ]

    if not sucursales:
        logger.error("No se pudieron obtener sucursales")
        return []
    
    all_productos = []
    all_productos_raw = []
    
    for idx, sucursal in enumerate(sucursales, 1):
        logger.info(f"\n[{idx}/{len(sucursales)}] Procesando: {sucursal['Descripcion']} - {sucursal['IdMarket']}-{sucursal['IdSucursal']}")

        # Obtener productos
        productos = get_productos(
            session, headers, base_url,
            sucursal['IdMarket'],
            sucursal['IdSucursal'],
        )    
            
        # Agregar y eliminar variables 
        for producto in productos:
            lista_precios = {
                'IdProducto': producto.get('IdProducto'),
                'PrecioVenta': producto.get('PrecioVenta'),
                'PrecioOriginal': producto.get('PrecioOriginal'),
                'IdMarket': sucursal['IdMarket'],
                'IdRegion': sucursal['IdRegion'],
            }
            all_productos.append(lista_precios)
            
            # Guardamos productos para comparación
            lista_productos = {
                'IdProducto': producto.get('IdProducto'),
                'Descripcion': producto.get('Descripcion'),
            }
            all_productos_raw.append(lista_productos)
        
        logger.info(f"Total Productos Sucursal: {len(productos)}")
        
        time.sleep(REQUEST_DELAY)
    
    # Guardamos lista de productos únicos con id y descripción
    productos_unicos(all_productos_raw)
    
    logger.info(f"\n{'='*20}")
    logger.info(f"RESUMEN HIPERMAXI")
    logger.info(f"Total productos: {len(all_productos)}")
    
    return all_productos
