import os
import logging
import random
import string
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import flash, request, session
from config.config import Config

logger = logging.getLogger(__name__)

def allowed_file(filename):
    """Valida si la extensión del archivo está permitida según configuración"""
    if not filename or '.' not in filename:
        return False
    extension = filename.rsplit('.', 1)[1].lower()
    return extension in Config.ALLOWED_EXTENSIONS

def save_uploaded_file(file, subfolder=''):
    """
    Guarda un archivo subido de forma segura en el sistema de archivos
    """
    if not file or not file.filename:
        return None
    
    if not allowed_file(file.filename):
        allowed_extensions = ', '.join(Config.ALLOWED_EXTENSIONS)
        logger.warning("Intento de subir archivo con extensión no permitida: %s", sanitizar_log_text(file.filename))
        raise ValueError(f"Tipo de archivo no permitido. Extensiones permitidas: {allowed_extensions}")
    
    filename = secure_filename(file.filename)
    upload_dir = os.path.join(Config.UPLOAD_FOLDER, subfolder)
    os.makedirs(upload_dir, exist_ok=True)
    
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)
    
    return f'/{filepath.replace(os.sep, "/")}'

def get_user_permissions():
    """Obtiene los permisos del usuario actual basados en su rol de sesión"""
    role = session.get('rol')
    permissions = Config.ROLES.get(role, [])
    return permissions

def can_access(section):
    """Verifica si el usuario actual tiene acceso a la sección especificada"""
    has_access = section in get_user_permissions()
    return has_access

def format_currency(value):
    """Formatea un valor numérico como moneda colombiana"""
    if value is None:
        return "$0"
    try:
        formatted = f"${value:,.0f}"
        return formatted.replace(",", ".")
    except (ValueError, TypeError) as e:
        logger.warning("Error formateando valor monetario")
        return "$0"

def format_date(date_value, format_str='%d/%m/%Y'):
    """Formatea un objeto datetime o date a string según formato especificado"""
    if not date_value:
        return ""
    
    try:
        if isinstance(date_value, str):
            return date_value
        formatted = date_value.strftime(format_str)
        return formatted
    except (AttributeError, ValueError) as e:
        logger.warning("Error formateando fecha")
        return "{0}".format(date_value)

def get_pagination_params(default_per_page=20):
    """Extrae parámetros de paginación de la solicitud actual"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', default_per_page, type=int)
    
    if page < 1:
        page = 1
    if per_page < 1 or per_page > 100:
        per_page = default_per_page
    
    return page, per_page

def flash_errors(form):
    """Muestra todos los errores de validación de un formulario como mensajes flash"""
    for field, errors in form.errors.items():
        for error in errors:
            field_label = getattr(form, field).label.text if hasattr(form, field) else field
            flash(f"Error en {field_label}: {error}", 'danger')
    
    if form.errors:
        logger.warning("Formulario con %s errores de validación", len(form.errors))

def generate_codigo_unico(prefix, existing_codes):
    """
    Genera un código único con prefijo especificado
    """
    max_attempts = 100
    for attempt in range(max_attempts):
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        codigo = f"{prefix}-{random_part}"
        
        if codigo not in existing_codes:
            return codigo
    
    logger.error("No se pudo generar código único después de %s intentos", max_attempts)
    raise ValueError("No se pudo generar código único")

def calcular_valor_total(cantidad, valor_unitario):
    """Calcula el valor total multiplicando cantidad por valor unitario"""
    try:
        if cantidad is None or valor_unitario is None:
            return 0
        total = cantidad * valor_unitario
        return total
    except (TypeError, ValueError) as e:
        logger.warning("Error calculando valor total")
        return 0

def validar_stock(cantidad_solicitada, stock_disponible):
    """Valida que la cantidad solicitada no exceda el stock disponible"""
    if cantidad_solicitada is None or stock_disponible is None:
        logger.warning("Valores None en validación de stock")
        return False
    
    es_valido = cantidad_solicitada <= stock_disponible
    if not es_valido:
        logger.info("Validación de stock fallida: solicitado=%s, disponible=%s", cantidad_solicitada, stock_disponible)
    
    return es_valido

def obtener_mes_actual():
    """Devuelve el nombre del mes actual en español"""
    meses = [
        'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
        'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
    ]
    mes_actual = meses[datetime.now().month - 1]
    return mes_actual

# ==================== FUNCIONES DE SANITIZACIÓN PARA LOGS ====================

def sanitizar_identificacion(numero):
    """
    Enmascara números de identificación para logs, protegiendo información sensible.
    Muestra solo los primeros 3 y últimos 3 dígitos.
    """
    if not numero:
        return '[identificacion-protegida]'
    
    try:
        num_str = "{0}".format(numero).strip()
        if len(num_str) <= 6:
            return '***' + num_str[-3:] if len(num_str) > 3 else '***'
        return num_str[:3] + '***' + num_str[-3:]
    except Exception as e:
        logger.warning("Error sanitizando identificación")
        return '[identificacion-protegida]'

def sanitizar_email(email):
    """
    Enmascara emails para logs, protegiendo información sensible
    """
    if not email or '@' not in email:
        return '[email-protegido]'
    
    try:
        partes = email.split('@')
        usuario = partes[0]
        dominio = partes[1]
        
        # Mostrar solo los primeros 2 caracteres del usuario
        if len(usuario) <= 2:
            usuario_sanitizado = usuario[0] + '***'
        else:
            usuario_sanitizado = usuario[:2] + '***'
        
        return f"{usuario_sanitizado}@{dominio}"
    except Exception as e:
        logger.warning("Error sanitizando email")
        return '[email-protegido]'

def sanitizar_username(username):
    """
    Enmascara usernames para logs, protegiendo información sensible
    """
    if not username:
        return '[usuario-protegido]'
    
    try:
        # Mostrar solo los primeros 2 caracteres
        if len(username) < 3:
            return username[0] + '***'
        else:
            return username[:2] + '***'
    except Exception as e:
        logger.warning("Error sanitizando username")
        return '[usuario-protegido]'

def sanitizar_ip(ip):
    """
    Enmascara direcciones IP para logs, protegiendo información sensible
    """
    if not ip:
        return '[ip-protegida]'
    
    try:
        # Para IPv4
        if '.' in ip:
            partes = ip.split('.')
            if len(partes) == 4:
                return f"{partes[0]}.{partes[1]}.***.***"
        
        # Para IPv6 u otros formatos, enmascarar completamente
        return '[ip-protegida]'
    except Exception as e:
        logger.warning("Error sanitizando IP")
        return '[ip-protegida]'


def sanitizar_log_text(value, max_len=500):
    """
    Neutraliza caracteres de control para evitar Log Injection (CWE-117).
    - Reemplaza CR/LF/TAB por secuencias visibles.
    - Elimina otros caracteres de control ASCII (< 32), excepto espacio.
    - Trunca a max_len.
    """
    if value is None:
        return ''

    if isinstance(value, BaseException):
        return '[error]'
    try:
        s = "{0}".format(value)
    except Exception:
        return '[texto-protegido]'

    # Normalizar y neutralizar
    s = s.replace('\r', '\\r').replace('\n', '\\n').replace('\t', '\\t')

    # Remover otros controles (0-31) excepto espacio
    s = ''.join(ch for ch in s if (ord(ch) >= 32) or ch == ' ')

    if max_len and len(s) > max_len:
        s = s[:max_len] + '...'
    return s


# Exportar las funciones de sanitización para que estén disponibles
__all__ = [
    'allowed_file', 'save_uploaded_file', 'get_user_permissions', 'can_access',
    'format_currency', 'format_date', 'get_pagination_params', 'flash_errors',
    'generate_codigo_unico', 'calcular_valor_total', 'validar_stock', 
    'obtener_mes_actual', 'sanitizar_identificacion', 'sanitizar_email', 
    'sanitizar_username', 'sanitizar_ip',
    'sanitizar_log_text'
]
