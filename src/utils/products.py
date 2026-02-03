import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict
from src.config import DATA_DIR

logger = logging.getLogger(__name__)

def productos_unicos(data: List[Dict], source: str = 'farmacorp'):
    """
    Guarda/actualiza archivo maestro de productos únicos
    
    Args:
        data: Lista de productos con IdProducto, Descripcion
        source: Nombre del scraper (carpeta donde se guarda)
    """
    if not data:
        logger.warning("No hay datos de productos para guardar")
        return
    
    # Preparar DataFrame nuevo
    df = pd.DataFrame(data)
    df_nuevo = df[['IdProducto', 'Descripcion']].copy()
    
    # Ordenar por IdProducto
    df_nuevo['IdProducto'] = df_nuevo['IdProducto'].astype(str)
    df_nuevo = df_nuevo.sort_values(by='IdProducto')
    
    # Definir ruta del archivo
    filename = f'{source}/productos.csv'
    filepath = DATA_DIR / filename
    filepath = Path(filepath)
    
    if filepath.exists():
        # Cargar datos antiguos
        df_antiguo = pd.read_csv(filepath)
        
        # Identificar productos nuevos
        nuevos = df_nuevo[~df_nuevo['IdProducto'].isin(df_antiguo['IdProducto'])]
        
        if len(nuevos) > 0:
            # Combinar datos antiguos con nuevos
            df_combinado = pd.concat([df_antiguo, nuevos]).drop_duplicates(
                subset='IdProducto',
                keep='last'  # Mantener la versión más reciente
            )
            
             # eliminar ceros
            df_combinado['IdProducto'] = df_combinado['IdProducto'].str.lstrip('0')
            # Eliminar productos sin ID válido
            df_combinado = df_combinado.dropna(subset=['IdProducto'])  
            # Ordenar
            df_combinado['IdProducto'] = df_combinado['IdProducto'].astype(str)
            df_combinado = df_combinado.sort_values(by='IdProducto')
            # eliminar espacios vacíos
            df_combinado['Descripcion'] = df_combinado['Descripcion'].str.strip()
            # Eliminar duplicados (mantener primer registro)
            df_combinado = df_combinado.drop_duplicates(subset=['IdProducto'], keep='first')
            # guardar
            df_combinado.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"[OK] {len(nuevos)} productos nuevos agregados al maestro")
        else:
            logger.info("No hay productos nuevos. Archivo sin cambios.")
    else:
        # Primera ejecución: crear archivo
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df_nuevo.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"[OK] Archivo maestro de productos creado con {len(df_nuevo)} productos")
