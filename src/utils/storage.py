import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def export_data(data: list, source: str, output_dir: Path, format: str) -> str:
    """
    Exportar datos a un archivo
    format: csv, pkl o parquet
    """
    if not data:
        logger.warning(f"No hay datos para guardar de {source}")
        return None
    
    fecha = datetime.now().strftime("%Y%m%d")
    mes = datetime.now().strftime("%Y%m")
    
    df = pd.DataFrame(data)
    
    # Nombre de archivo
    ext = format if format == 'parquet' else f"{format}.gz"
    filename = f"{fecha}.{ext}"
    filepath = output_dir / source / mes / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Guardar
    if format == 'csv':
        df.to_csv(filepath, index=False, encoding='utf-8-sig', compression='gzip')
    elif format =='pkl':
        df.to_pickle(filepath, compression='gzip')
    elif format == 'parquet':
        df.to_parquet(filepath, index=False)
    else:
        logger.error(f"Formato '{format}' no soportado.")
    
    logger.info(f"[OK] Datos guardados: {filepath}")
    logger.info(f"  - Registros: {len(df)}")
    
    return str(filepath)

def export_info(data: list, source: str, output_dir: Path, filename: str) -> str:
    """
    Exportar información (listas) a un archivo csv
    """
    df = pd.DataFrame(data)
    
    filename = f"{filename}.csv"
    filepath = output_dir / source / filename
    
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    logger.info(f"[OK] Información guardada: {filepath}")
    
    return str(filepath)
