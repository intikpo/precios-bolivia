from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT = 15
REQUEST_DELAY = 0.5  # segundos entre peticiones

SCRAPERS_CONFIG = {
    'hipermaxi': {
        'enabled': True,
        'base_url': 'https://hipermaxi.com/tienda-api/api/v1',
        'web_url': 'https://hipermaxi.com',
        'tipo_servicio_filter': [1]  # 1=Supermercado, 2=Farmacia
    },
    'farmacorp': {
        'enabled': True,
        'base_url': 'https://farmacorp.com',
    },
    # Agregar otros aquí
    # 'comercio1': {'enabled': True, 'base_url': '...'},
}