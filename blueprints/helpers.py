import os
import logging
import random
import string
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import flash, request, session
from config.config import Config

logger = logging.getLogger(__name__)

# ==================== FUNCIONES HELPER PARA COBROS POP ====================

def _can_view_cobros_pop() -> bool:
    """Verifica si el usuario tiene permiso para ver el reporte de cobros"""
    return can_access('reportes', 'cobros_view')

def _can_cancel_cobros_pop() -> bool:
    """Verifica si el usuario tiene permiso para cancelar cobros"""
    return can_access('reportes', 'cobros_cancel')

def _can_export_cobros_pop() -> bool:
    """Verifica si el usuario tiene permiso para exportar el reporte de cobros"""
    return can_access('reportes', 'cobros_export')

def _parse_periodo(periodo_raw: str) -> str:
    """
    Parsea un período en formato YYYY-MM, validando que sea correcto.
    Si no es válido, retorna el período actual.
    """
    try:
        periodo_raw = (periodo_raw or '').strip()
        if len(periodo_raw) == 7 and periodo_raw[4] == '-':
            y = int(periodo_raw[:4])
            m = int(periodo_raw[5:7])
            if 1 <= m <= 12 and 2000 <= y <= 2100:
                return f"{y:04d}-{m:02d}"
    except Exception:
        pass
    return datetime.now().strftime('%Y-%m')

def _periodo_to_range(periodo: str):
    """
    Convierte un período YYYY-MM en un rango de fechas (inicio y fin del mes)
    """
    y = int(periodo[:4])
    m = int(periodo[5:7])
    inicio = datetime(y, m, 1)
    fin = datetime(y+1, 1, 1) if m == 12 else datetime(y, m+1, 1)
    return inicio, fin

def _consultar_cobros_pop(periodo: str, oficina_id=None):
    """
    Consulta los cobros por oficina y material para el período especificado.
    Retorna una lista de diccionarios con los datos agregados.
    """
    inicio, fin = _periodo_to_range(periodo)
    conn = get_database_connection()
    if conn is None:
        return []
    cur = conn.cursor()
    try:
        where_oficina = ''
        params = [inicio, fin]
        if oficina_id is not None:
            where_oficina = ' AND sm.OficinaSolicitanteId = ?'
            params.append(oficina_id)

        query = f"""
        SELECT
            o.OficinaId,
            o.NombreOficina,
            m.MaterialId,
            m.NombreElemento,
            CAST(m.ValorUnitario AS DECIMAL(18,2)) AS ValorUnitario,
            CAST(sm.PorcentajeOficina AS DECIMAL(5,2)) AS PorcentajeOficina,
            COUNT(sm.SolicitudId) AS NumSolicitudes,
            SUM(COALESCE(sm.CantidadEntregada, sm.CantidadSolicitada)) AS CantidadTotal,
            SUM(COALESCE(sm.ValorTotalSolicitado, (m.ValorUnitario * COALESCE(sm.CantidadEntregada, sm.CantidadSolicitada)))) AS ValorTotal,
            SUM(COALESCE(sm.ValorOficina, (m.ValorUnitario * COALESCE(sm.CantidadEntregada, sm.CantidadSolicitada)) * (sm.PorcentajeOficina/100.0))) AS ValorCobroOficina
        FROM dbo.SolicitudesMaterial sm
        INNER JOIN dbo.Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
        INNER JOIN dbo.Materiales m ON sm.MaterialId = m.MaterialId
        INNER JOIN dbo.EstadosSolicitud es ON sm.EstadoId = es.EstadoId
        WHERE
            LOWER(es.NombreEstado) LIKE '%aprob%'
            AND sm.FechaAprobacion >= ?
            AND sm.FechaAprobacion < ?
            {where_oficina}
        GROUP BY
            o.OficinaId, o.NombreOficina,
            m.MaterialId, m.NombreElemento, m.ValorUnitario, sm.PorcentajeOficina
        ORDER BY
            o.NombreOficina ASC, m.NombreElemento ASC
        """
        cur.execute(query, params)
        rows = cur.fetchall() or []
        cols = [c[0] for c in cur.description]
        out = []
        for r in rows:
            d = dict(zip(cols, r))
            out.append({
                'oficina_id': int(d['OficinaId']),
                'oficina_nombre': d['NombreOficina'],
                'material_id': int(d['MaterialId']),
                'material_nombre': d['NombreElemento'],
                'valor_unitario': float(d['ValorUnitario'] or 0),
                'porcentaje_oficina': float(d['PorcentajeOficina'] or 0),
                'num_solicitudes': int(d['NumSolicitudes'] or 0),
                'cantidad_total': int(d['CantidadTotal'] or 0),
                'valor_total': float(d['ValorTotal'] or 0),
                'valor_cobro_oficina': float(d['ValorCobroOficina'] or 0),
            })
        return out
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# ==================== FUNCIONES HELPER EXISTENTES ====================

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
    
    logger.debug("Archivo guardado exitosamente: %s", sanitizar_log_text(os.path.basename(filepath)))
    return f'/{filepath.replace(os.sep, "/")}'

def get_user_permissions():
    """Obtiene los permisos del usuario actual basados en su rol de sesión"""
    role = session.get('rol')
    permissions = Config.ROLES.get(role, [])
    logger.debug("Permisos obtenidos para rol %s: %s permisos", sanitizar_log_text(role), sanitizar_log_text(len(permissions)))
    return permissions

def can_access(section):
    """Verifica si el usuario actual tiene acceso a la sección especificada"""
    has_access = section in get_user_permissions()
    logger.debug("Acceso a sección %s: %s", sanitizar_log_text(section), sanitizar_log_text(has_access))
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
    
    logger.debug("Parámetros de paginación: página=%s, por_página=%s", sanitizar_log_text(page), sanitizar_log_text(per_page))
    return page, per_page

def flash_errors(form):
    """Muestra todos los errores de validación de un formulario como mensajes flash"""
    for field, errors in form.errors.items():
        for error in errors:
            field_label = getattr(form, field).label.text if hasattr(form, field) else field
            flash(f"Error en {field_label}: {error}", 'danger')
    
    if form.errors:
        logger.warning("Formulario con %s errores de validación", sanitizar_log_text(len(form.errors)))

def generate_codigo_unico(prefix, existing_codes):
    """
    Genera un código único con prefijo especificado
    """
    max_attempts = 100
    for attempt in range(max_attempts):
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        codigo = f"{prefix}-{random_part}"
        
        if codigo not in existing_codes:
            logger.debug("Código único generado: %s (intento %s)", sanitizar_log_text(codigo), sanitizar_log_text(attempt + 1))
            return codigo
    
    logger.error("No se pudo generar código único después de %s intentos", sanitizar_log_text(max_attempts))
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
        logger.info("Validación de stock fallida: solicitado=%s, disponible=%s", sanitizar_log_text(cantidad_solicitada), sanitizar_log_text(stock_disponible))
    
    return es_valido

def obtener_mes_actual():
    """Devuelve el nombre del mes actual en español"""
    meses = [
        'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
        'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
    ]
    mes_actual = meses[datetime.now().month - 1]
    logger.debug("Mes actual obtenido: %s", sanitizar_log_text(mes_actual))
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
    '_can_view_cobros_pop', '_can_cancel_cobros_pop', '_can_export_cobros_pop',
    '_parse_periodo', '_periodo_to_range', '_consultar_cobros_pop',
    'allowed_file', 'save_uploaded_file', 'get_user_permissions', 'can_access',
    'format_currency', 'format_date', 'get_pagination_params', 'flash_errors',
    'generate_codigo_unico', 'calcular_valor_total', 'validar_stock', 
    'obtener_mes_actual', 'sanitizar_identificacion', 'sanitizar_email', 
    'sanitizar_username', 'sanitizar_ip', 'sanitizar_log_text'
]