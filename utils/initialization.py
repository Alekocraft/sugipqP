# utils/initialization.py
import logging
logger = logging.getLogger(__name__)

def inicializar_oficina_principal():
    """
    Inicialización segura.
    Si en el futuro agregas lógica real aquí, la llamará app.py.
    """
    try:
        # Si tienes lógica real en otro módulo, impórtala aquí.
        # from .algo import inicializar_oficina_principal_real
        # return inicializar_oficina_principal_real()
        return None
    except Exception:
        logger.exception("Falló inicializar_oficina_principal()")
        return None
