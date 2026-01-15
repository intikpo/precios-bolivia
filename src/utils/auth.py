import requests
import re
import logging
import time

logger = logging.getLogger(__name__)

def get_bare_headers() -> dict:
    """Retorna headers básicos para las peticiones"""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": "https://www.hipermaxi.com",
        "Referer": "https://www.hipermaxi.com/",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Connection": "keep-alive",
    }

def get_authenticated_session(timeout: int = 10) -> tuple:
    """Obtiene headers con token de autenticación usando una sola sesión"""
    try:
        logger.info("Preparando sesión de autenticación...")
        
        # Crear una sesión persistente
        session = requests.Session()
        session.verify = False
        
        headers = get_bare_headers()
        
        # Paso 1: Visitar la página principal para establecer cookies
        response = session.get(
            "https://www.hipermaxi.com",
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()
        
        # Guardar HTML para debug
        main_html = response.text
        
        # Esperar un poco (comportamiento humano)
        time.sleep(1)
        
        # Paso 2: Obtener token anónimo usando la misma sesión
        logger.info("Obteniendo token anónimo...")
        response = session.put(
            "https://hipermaxi.com/tienda-api/api/v1/CuentasMarket/Anonimo-Por-Token",
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        
        token_data = response.json()
        codigo = token_data["Dato"]["Codigo"]
        token = token_data["Dato"]["Token"]
        logger.info(f"[OK] Token obtenido: {codigo}")
        
        # Paso 3: Extraer credenciales del HTML ya descargado
        logger.info("Extrayendo credenciales de aplicación...")
        
        # Buscar el script main.js en el HTML
        main_match = re.search(r'src="(/static/js/main\.[^"]+\.js)"', main_html)
        if not main_match:
            logger.error("No se encontró referencia a main.js en el HTML")
            raise Exception("No se encontró main.js")
        
        main_url = f"https://www.hipermaxi.com{main_match.group(1)}"
        
        # Descargar main.js usando la misma sesión
        response = session.get(main_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        main_content = response.text
                
        # Extraer variables
        variables = [
            "REACT_APP_CUENTA",
            "REACT_APP_APLICACION", 
            "REACT_APP_PASSWORD",
            "REACT_APP_GRANT_TYPE",
        ]
        
        valores = []
        for variable in variables:
            # Intentar múltiples patrones
            patterns = [
                rf'{variable}:"([^"]*)"',
                rf'{variable}:\\"([^\\"]*)\\"',
                rf'"{variable}":"([^"]*)"',
            ]
            
            match_found = None
            for pattern in patterns:
                matches = re.findall(pattern, main_content)
                if matches:
                    match_found = matches[0]
                    break
            
            if not match_found:
                logger.error(f"No se encontró {variable}")
                # Guardar para debug
                with open('debug_main.js', 'w', encoding='utf-8') as f:
                    f.write(main_content)
                logger.info("main.js guardado en debug_main.js para análisis")
                raise Exception(f"Variable {variable} no encontrada")
            
            valores.append(match_found)
        
        cuenta, aplicacion, password, grant_type = valores
        
        # Paso 4: Autenticar
        logger.info("Autenticando con credenciales...")
        
        response = session.post(
            "https://hipermaxi.com/tienda-api/api/v1/token",
            headers=headers,
            data={
                "grant_type": grant_type,
                "aplicacion": aplicacion,
                "cuenta": cuenta,
                "password": password,
                "CodigoAcceso": codigo,
                "Token": token,
            },
            timeout=timeout,
        )
        response.raise_for_status()
        
        bearer = response.json()["access_token"]
        
        authenticated_headers = {
            **headers,
            "authorization": f"Bearer {bearer}",
            "origin": "https://hipermaxi.com",
            "referer": "https://hipermaxi.com",
        }
        
        logger.info("[OK] Autenticación exitosa")
        return session, authenticated_headers
        
    except Exception as e:
        logger.error(f"Error en autenticación: {e}")
        raise