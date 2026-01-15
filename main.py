import logging
import warnings
from src.config import SCRAPERS_CONFIG, DATA_DIR
from src.scrapers.hipermaxi import scrape_hipermaxi
from src.utils.storage import export_data

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Suprimir warnings de SSL
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

def main():
    logger = logging.getLogger(__name__)
    logger.info("INICIANDO...")
    logger.info("="*20)
    
    # Scraper Hipermaxi
    if SCRAPERS_CONFIG['hipermaxi']['enabled']:
        try:
            data = scrape_hipermaxi(SCRAPERS_CONFIG['hipermaxi'])
            
            if data:
                export_data(data, 'hipermaxi', DATA_DIR, 'csv')
            else:
                logger.error("No se obtuvieron datos de Hipermaxi")
                
        except Exception as e:
            logger.error(f"Error en scraper Hipermaxi: {e}", exc_info=True)
    
    
    logger.info("\n" + "="*20)
    logger.info("SCRAPING COMPLETADO")

if __name__ == "__main__":
    main()