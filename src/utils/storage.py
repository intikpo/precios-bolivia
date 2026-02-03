import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def export_data(data: list, 
                source: str, 
                output_dir: Path, 
                format: str, 
                remove_duplicates: bool = False) -> str:
    """
    Exportar datos a un archivo
    Args:
        data: datos
        source: fuente de datos 
        output_dir: DATA_DIR
        format: csv, pkl o parquet
        remove_duplicates: Indica si se eliminan duplicados

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

    # Eliminamos duplicados considerando solo IdProducto
    if remove_duplicates:
        # Ordenamos, asegurando que "PrecioOriginal <> 0" esté arriba
        df = df.sort_values(by=['IdProducto', 'PrecioOriginal'], ascending=[True, False])
        # eliminamos duplicados, y donde "PrecioOriginal = 0"
        df = df.drop_duplicates(subset=['IdProducto'], keep='first')
    
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
