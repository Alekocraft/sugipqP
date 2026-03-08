# app.py
import sys
import io
import os
import logging
from datetime import datetime, timedelta
from flask import (
    Flask, render_template, request, redirect, session, flash,
    jsonify, url_for, send_file, g, make_response
)
from utils.helpers import sanitizar_log_text
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import uuid
import json
# ============================================================================
# 0. CORRECCIÓN ENCODING PARA WINDOWS
# ============================================================================
# Configurar encoding UTF-8 para Windows
if sys.platform == "win32":
    # Forzar UTF-8 en stdout/stderr
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    
    # Configurar logging para Windows (reemplazar emojis)
    class SafeFilter(logging.Filter):
        def filter(self, record):
            if hasattr(record, 'msg'):
                # Reemplazar emojis con texto seguro para Windows
                replacements = {
                    'âœ…': '[OK]',
                    'âš ï¸\x8f': '[WARN]',
                    'âŒ': '[ERROR]',
                    'â„¹ï¸\x8f': '[INFO]',
                    'ðŸ"¦': '[INVENTARIO]',
                    'ðŸ"‹': '[SOLICITUD]',
                    'ðŸ”': '[LDAP]',
                    'ðŸ"§': '[EMAIL]',
                    'ðŸš€': '[INICIO]',
                    "ðŸ'¥": '[ROLES]',  # Usar comillas dobles para el interior
                    'ðŸ"§': '[CONFIG]',
                    'ðŸ"': '[DIRECTORIO]'
                }
                for emoji, text in replacements.items():
                    if emoji in record.msg:
                        record.msg = record.msg.replace(emoji, text)
            return True

# ============================================================================
# 1. CARGAR VARIABLES DE ENTORNO
# ============================================================================
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

if os.path.exists(env_path):
    print(f"[OK] Archivo .env encontrado en: {env_path}")
else:
    print(f"[WARN] Archivo .env NO encontrado en: {env_path}")

# ============================================================================
# 2. CONFIGURAR LOGGING INICIAL (CORREGIDO PARA WINDOWS)
# ============================================================================
# Configurar logging primero
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Aplicar filtro para Windows si es necesario
if sys.platform == "win32":
    for handler in logging.root.handlers:
        handler.addFilter(SafeFilter())

# ============================================================================
# 3. IMPRESIÓN DE VARIABLES DE ENTORNO (CORREGIDO)
# ============================================================================
print("\n=== VARIABLES DE ENTORNO CARGADAS ===")
print(f"SMTP_SERVER: {os.getenv('SMTP_SERVER', 'NO CONFIGURADO')}")
print(f"SMTP_PORT: {os.getenv('SMTP_PORT', 'NO CONFIGURADO')}")
print(f"SMTP_FROM_EMAIL: {os.getenv('SMTP_FROM_EMAIL', 'NO CONFIGURADO')}")
print(f"FLASK_ENV: {os.getenv('FLASK_ENV', 'NO CONFIGURADO')}")
print(f"DATABASE_URL: {os.getenv('DATABASE_URL', 'NO CONFIGURADO')}")
print(f"SECRET_KEY: {'CONFIGURADO' if os.getenv('SECRET_KEY') else 'NO CONFIGURADO'}")
print("====================================\n")

# ============================================================================
# 4. IMPORTACIÓN DEL SERVICIO DE NOTIFICACIONES
# ============================================================================
try:
    from services.notification_service import NotificationService, servicio_notificaciones_disponible
    
    if not servicio_notificaciones_disponible():
        logger.warning("[WARN] Servicio de notificaciones no disponible (configuración faltante)")
    else:
        logger.info("[OK] Servicio de notificaciones disponible")
except ImportError as e:
    logger.warning("No se pudo importar el servicio de notificaciones")
    
    def servicio_notificaciones_disponible():
        return False

# ============================================================================
# 5. CONFIGURACIÓN DE LOGGING COMPLETA (CORREGIDA)
# ============================================================================
# Re-configurar logging con encoding correcto
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log', encoding='utf-8'),
        logging.StreamHandler(stream=sys.stdout)
    ]
)

# Aplicar filtro para Windows
if sys.platform == "win32":
    for handler in logging.root.handlers:
        handler.addFilter(SafeFilter())

logger = logging.getLogger(__name__)

# Configuración de logging para LDAP
ldap_logger = logging.getLogger('ldap3')
ldap_logger.setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

ldap_log_file = os.path.join(log_dir, 'ldap.log')
file_handler = logging.FileHandler(ldap_log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
ldap_logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)
ldap_logger.addHandler(console_handler)

# Configurar logger de la aplicación para LDAP (utils/ldap_auth.py usa el logger 'ldap')
app_ldap_logger = logging.getLogger('ldap')
app_ldap_logger.setLevel(logging.INFO)
# Evitar duplicar handlers si ya existen
if not any(getattr(h, 'baseFilename', None) == ldap_log_file for h in app_ldap_logger.handlers):
    app_ldap_logger.addHandler(file_handler)
app_ldap_logger.propagate = False


logger.info("Logging de LDAP configurado. Archivo: %s", sanitizar_log_text(ldap_log_file))

# ============================================================================
# 6. CREAR MÓDULO HELPERS SI NO EXISTE
# ============================================================================
# Crear un módulo helpers temporal si no existe en utils
import types

# Verificar si utils.helpers existe
try:
    import utils.helpers as helpers_module
    logger.info("Módulo utils.helpers cargado correctamente")
except ImportError:
    logger.warning("Creando módulo helpers temporal...")
    
    # Crear módulo helpers temporal
    helpers_module = types.ModuleType('utils.helpers')
    sys.modules['utils.helpers'] = helpers_module
    
    # Definir funciones mínimas requeridas
    def sanitizar_email(email):
        """Enmascara emails para logs"""
        if not email or '@' not in email:
            return '[email-protegido]'
        try:
            partes = email.split('@')
            usuario = partes[0]
            dominio = partes[1]
            if len(usuario) <= 2:
                return f"{usuario[0]}***@{dominio}"
            return f"{usuario[:2]}***@{dominio}"
        except Exception:
            return '[email-protegido]'
    
    def sanitizar_username(username):
        """Enmascara usernames para logs"""
        if not username:
            return '[usuario-protegido]'
        try:
            if len(username) < 3:
                return username[0] + '***'
            return username[:2] + '***'
        except Exception:
            return '[usuario-protegido]'
    
    def sanitizar_ip(ip):
        """Enmascara direcciones IP para logs"""
        if not ip:
            return '[ip-protegida]'
        try:
            if '.' in ip:
                partes = ip.split('.')
                if len(partes) == 4:
                    return f"{partes[0]}.{partes[1]}.***.***"
            return '[ip-protegida]'
        except Exception:
            return '[ip-protegida]'
    
    # Agregar funciones al módulo
    helpers_module.sanitizar_email = sanitizar_email
    helpers_module.sanitizar_username = sanitizar_username
    helpers_module.sanitizar_ip = sanitizar_ip
    
    logger.info("Módulo helpers temporal creado")

# ============================================================================
# 7. CONFIGURACIÓN DE LA APLICACIÓN FLASK
# ============================================================================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, 'templates'),
    static_folder=os.path.join(BASE_DIR, 'static')
)

app.secret_key = os.environ.get('SECRET_KEY', os.urandom(32))
app.config['JSON_AS_ASCII'] = False
app.config['TEMPLATES_AUTO_RELOAD'] = True

UPLOAD_FOLDER = 'static/uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
logger.info("Directorio de uploads configurado en: %s", sanitizar_log_text(os.path.abspath(UPLOAD_FOLDER)))

app.config['SESSION_COOKIE_SECURE'] = os.environ.get('FLASK_ENV') == 'production'
app.config['SESSION_COOKIE_HTTPONLY'] = False # ajuste a true cuando este el link 
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=8)

SESSION_TIMEOUT_MINUTES = 30

# ============================================================================
# 7. CONEXIÓN A BASE DE DATOS Y MODELOS
# ============================================================================

# Importación de modelos
try:
    from models.materiales_model import MaterialModel
    from models.oficinas_model import OficinaModel
    from models.solicitudes_model import SolicitudModel
    from models.usuarios_model import UsuarioModel
    from models.inventario_corporativo_model import InventarioCorporativoModel
    logger.info("Modelos cargados correctamente")
except ImportError as e:
    logger.error("Error cargando modelos")
    # Definir clases dummy para evitar errores
    class MaterialModel: 
        @staticmethod
        def obtener_todos(oficina_id=None): return []
        @staticmethod
        def crear(data): pass
    class OficinaModel: 
        @staticmethod
        def obtener_todas(): return []
    class SolicitudModel: 
        @staticmethod
        def obtener_todas(oficina_id=None): return []
        @staticmethod
        def crear(data): pass
    class UsuarioModel: 
        @staticmethod
        def obtener_todos(): return []
        @staticmethod
        def obtener_aprobadores(): return []
    class InventarioCorporativoModel: pass

# Importación de utilidades (importación separada para evitar modo "allow-all")
# 1) Filtros / oficina
try:
    from utils.filters import filtrar_por_oficina_usuario, verificar_acceso_oficina
except Exception as e:
    logger.exception("Error cargando utils.filters (se aplicará fallback seguro)")
    # Fallback seguro: no amplía acceso
    def filtrar_por_oficina_usuario(data, *args, **kwargs):
        # Si no hay filtro disponible, no devolvemos más datos; se mantiene lo que ya traiga el blueprint/modelo.
        return data
    def verificar_acceso_oficina(*args, **kwargs):
        return False

# 2) Inicialización
try:
    from utils.initialization import inicializar_oficina_principal
except Exception as e:
    logger.exception("Error cargando utils.initialization (se omite inicialización)")
    def inicializar_oficina_principal():
        return None

# 3) Permisos (FUENTE para UI/plantillas)
try:
    from utils.permissions import (
        can_access, can_view_actions,
        get_accessible_modules,
        can_create_novedad, can_manage_novedad,
        can_approve_solicitud, can_approve_partial_solicitud,
        can_reject_solicitud, can_return_solicitud,
        can_view_novedades, user_can_view_all
    )
    logger.info("Permisos y utilidades cargadas correctamente")
except Exception as e:
    logger.exception("Error cargando utils.permissions (fallback seguro: deny-by-default)")
    # Fallback seguro: deny-by-default (NO mostrar opciones ni conceder acceso)
    def can_access(*args, **kwargs): return False
    def can_view_actions(*args, **kwargs): return []
    def get_accessible_modules(*args, **kwargs): return []
    def can_create_novedad(*args, **kwargs): return False
    def can_manage_novedad(*args, **kwargs): return False
    def can_approve_solicitud(*args, **kwargs): return False
    def can_approve_partial_solicitud(*args, **kwargs): return False
    def can_reject_solicitud(*args, **kwargs): return False
    def can_return_solicitud(*args, **kwargs): return False
    def can_view_novedades(*args, **kwargs): return False
    def user_can_view_all(*args, **kwargs): return False


# Importar funciones de permisos para templates
try:
    from utils.permissions_functions import PERMISSION_FUNCTIONS
    logger.info("Funciones de permisos para templates cargadas correctamente")
except ImportError as e:
    logger.warning("No se encontró permissions_functions.py, usando funciones por defecto")
    PERMISSION_FUNCTIONS = {}

# ============================================================================
# 8. IMPORTACIÓN CONDICIONAL DE BLUEPRINTS
# ============================================================================

# Importación de blueprints principales (siempre disponibles)
try:
    from blueprints.auth import auth_bp
    from blueprints.materiales import materiales_bp
    from blueprints.solicitudes import solicitudes_bp
    from blueprints.oficinas import oficinas_bp
    from blueprints.aprobadores import aprobadores_bp
    from blueprints.reportes import reportes_bp
    from blueprints.api import api_bp
    from blueprints.usuarios import usuarios_bp
    from certificado_route import certificado_bp  # ← LÍNEA AGREGADA
    logger.info("Blueprints principales cargados correctamente")
except ImportError as e:
    logger.error("Error cargando blueprints principales")
    # Crear blueprints dummy
    from flask import Blueprint
    auth_bp = Blueprint('auth', __name__)
    materiales_bp = Blueprint('materiales', __name__)
    solicitudes_bp = Blueprint('solicitudes', __name__)
    oficinas_bp = Blueprint('oficinas', __name__)
    aprobadores_bp = Blueprint('aprobadores', __name__)
    reportes_bp = Blueprint('reportes', __name__)
    api_bp = Blueprint('api', __name__)
    usuarios_bp = Blueprint('usuarios', __name__)
    certificado_bp = Blueprint('certificado', __name__)  # ← LÍNEA AGREGADA

# Importación condicional de blueprint de préstamos
try:
    from blueprints.prestamos import prestamos_bp
    logger.info("Blueprint de préstamos cargado exitosamente")
except ImportError as e:
    logger.warning("Blueprint de préstamos no disponible")
    from flask import Blueprint
    prestamos_bp = Blueprint('prestamos', __name__)
    
    @prestamos_bp.route('/')
    def prestamos_vacio():
        flash('Módulo de préstamos no disponible', 'warning')
        return redirect('/dashboard')

# Importación condicional de blueprint de inventario corporativo
try:
    from blueprints.inventario_corporativo import inventario_corporativo_bp
    logger.info("Blueprint de inventario corporativo cargado desde blueprints")
except ImportError as e:
    logger.warning("Blueprint de inventario corporativo no encontrado en blueprints")
    try:
        from routes_inventario_corporativo import bp_inv as inventario_corporativo_bp
        logger.info("Blueprint de inventario corporativo cargado desde routes_inventario_corporativo")
    except ImportError as e2:
        logger.warning("Blueprint de inventario corporativo no disponible")
        from flask import Blueprint
        inventario_corporativo_bp = Blueprint('inventario_corporativo', __name__)
        
    @inventario_corporativo_bp.route('/')
    def inventario_vacio():
        flash('Módulo de inventario corporativo no disponible', 'warning')
        return redirect('/dashboard')

# Importación condicional de blueprint de confirmaciones (NUEVO)
try:
    from blueprints.confirmacion_asignaciones import confirmacion_asignaciones_bp as confirmacion_bp
    logger.info("Blueprint de confirmaciones cargado exitosamente")
except ImportError as e:
    logger.warning("Blueprint de confirmaciones no disponible")
    from flask import Blueprint
    confirmacion_bp = Blueprint('confirmacion', __name__)
    
    @confirmacion_bp.route('/')
    def confirmacion_vacio():
        flash('Módulo de confirmaciones no disponible', 'warning')
        return redirect('/dashboard')

# ============================================================================
# 9. MIDDLEWARE DE SESIÓN
# ============================================================================

@app.before_request
def check_session_timeout():
    """Verifica timeout de sesión antes de cada request"""
    # Rutas públicas que no requieren verificación
    public_routes = ['/login', '/logout', '/static', '/api/session-check', 
                     '/auth/login', '/auth/logout', '/auth/test-ldap',
                     '/certificado', '/certificado/generar',
                     '/confirmacion']  # ← AÑADIDO
    
    if any(request.path.startswith(route) for route in public_routes):
        return
    
    if 'usuario_id' in session:
        last_activity = session.get('last_activity')
        if last_activity:
            try:
                if isinstance(last_activity, str):
                    last_activity = datetime.fromisoformat(last_activity)
                
                inactive_time = datetime.now() - last_activity
                if inactive_time > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                    logger.info("Sesión expirada por inactividad: %s", sanitizar_log_text(session.get("usuario")))
                    session.clear()
                    flash('Su sesión ha expirado por inactividad. Por favor, inicie sesión nuevamente.', 'warning')
                    return redirect('/auth/login')
            except Exception as e:
                logger.warning("Error verificando timeout de sesión")

@app.after_request
def update_session_activity(response):
    """Actualiza timestamp de actividad después de cada request"""
    if 'usuario_id' in session and response.status_code < 400:
        session['last_activity'] = datetime.now().isoformat()
        session.modified = True
    return response

# ============================================================================
# 10. FUNCIONES DE PERMISOS PARA TEMPLATES (DEFINIDAS LOCALMENTE)
# ============================================================================

# Roles con permisos completos
ROLES_GESTION_COMPLETA = ['administrador', 'lider_inventario', 'aprobador']

# Roles de oficina
ROLES_OFICINA = [
    'oficina_coq', 'oficina_cali', 'oficina_pereira', 'oficina_neiva',
    'oficina_kennedy', 'oficina_bucaramanga', 'oficina_polo_club',
    'oficina_nogal', 'oficina_tunja', 'oficina_cartagena', 'oficina_morato',
    'oficina_medellin', 'oficina_cedritos', 'oficina_lourdes', 'oficina_regular'
]

def get_user_role():
    """Obtiene el rol del usuario actual"""
    return session.get('rol', '').lower()

def has_gestion_completa():
    """Verifica si el usuario tiene permisos de gestión completa"""
    return get_user_role() in ROLES_GESTION_COMPLETA

def is_oficina_role():
    """Verifica si el usuario tiene rol de oficina (incluye roles office-like)."""
    rol = get_user_role()
    if not rol:
        return False

    # Roles de oficina explícitos (compatibilidad)
    if rol in ROLES_OFICINA:
        return True

    # Cualquier rol que empiece por 'oficina' se considera oficina
    # (ej: oficina_polo_club, oficina_coq, oficina_regular, etc.)
    if rol.startswith('oficina'):
        return True

    # Roles corporativos que deben comportarse como oficina COQ (UI y filtros)
    if rol in {'gerencia_talento_humano', 'gerencia_comercial', 'comunicaciones', 'presidencia'}:
        return True

    return False

def can_create_or_view():
    """Verifica si puede crear novedades o ver detalles"""
    rol = get_user_role()
    return rol in ROLES_GESTION_COMPLETA or is_oficina_role()

def should_show_devolucion_button(solicitud):
    """Determina si mostrar botón de devolución"""
    if not solicitud:
        return False
    if not can_create_or_view():
        return False
    
    estado_id = solicitud.get('estado_id') or 1
    estados_permitidos = [2, 4, 5]  # Aprobada, Entregada Parcial, Completada
    
    if estado_id not in estados_permitidos:
        return False
    
    cantidad_entregada = solicitud.get('cantidad_entregada', 0) or 0
    cantidad_devuelta = solicitud.get('cantidad_devuelta', 0) or 0
    
    return cantidad_entregada > cantidad_devuelta

def should_show_gestion_devolucion_button(solicitud):
    """Determina si mostrar el botón de gestionar devolución

    Solo roles con gestión completa. Requiere que exista devolución pendiente.
    """
    if not solicitud:
        return False
    if not has_gestion_completa():
        return False
    solicitud_id = solicitud.get('id') or solicitud.get('solicitud_id')
    if not solicitud_id:
        return False
    try:
        from models.solicitudes_model import SolicitudModel
        return bool(SolicitudModel.tiene_devolucion_pendiente(int(solicitud_id)))
    except Exception:
        # Fallback: si el backend ya marcó el flag
        return bool(solicitud.get('devolucion_pendiente'))

def should_show_novedad_button(solicitud):
    """Determina si mostrar botón de crear novedad"""
    if not solicitud:
        return False
    if not can_create_or_view():
        return False
    
    estado_id = solicitud.get('estado_id') or 1
    estados_permitidos = [2, 4, 5]  # Aprobada, Entregada Parcial, Completada
    estados_con_novedad = [7, 8, 9]
    
    if estado_id in estados_con_novedad:
        return False
    
    return estado_id in estados_permitidos

def should_show_gestion_novedad_button(solicitud):
    """Determina si mostrar botón de gestionar novedad (aprobar/rechazar)"""
    if not solicitud:
        return False
    if not has_gestion_completa():
        return False
    
    estado_id = solicitud.get('estado_id') or 1
    return estado_id == 7  # Novedad Registrada

def should_show_aprobacion_buttons(solicitud):
    """Determina si mostrar botones de aprobación/rechazo de solicitudes"""
    if not solicitud:
        return False
    if not has_gestion_completa():
        return False
    
    estado_id = solicitud.get('estado_id') or 1
    return estado_id == 1  # Pendiente

def should_show_detalle_button(solicitud):
    """Determina si mostrar botón de ver detalles"""
    return solicitud is not None and can_create_or_view()

# ============================================================================
# 11. CONTEXT PROCESSOR
# ============================================================================

@app.context_processor
def utility_processor():
    """Inyecta funciones de permisos en todos los templates"""
    all_functions = {}
    
    # Agregar funciones principales de utils.permissions
    try:
        all_functions.update({
            'can_access': can_access,
            'can_create_novedad': can_create_novedad,
            'can_manage_novedad': can_manage_novedad,
            'can_view_novedades': can_view_novedades,
            'can_approve_solicitud': can_approve_solicitud,
            'can_reject_solicitud': can_reject_solicitud,
            'can_return_solicitud': can_return_solicitud,
            'can_approve_partial_solicitud': can_approve_partial_solicitud,
            'can_view_actions': can_view_actions,
            'get_accessible_modules': get_accessible_modules,
            'user_can_view_all': user_can_view_all
        })
    except Exception as e:
        logger.error("No se pudieron importar funciones de permisos")
    
    # Agregar funciones de PERMISSION_FUNCTIONS si existen
    if PERMISSION_FUNCTIONS:
        all_functions.update(PERMISSION_FUNCTIONS)
    
    # AGREGAR FUNCIONES should_show_* LOCALES (SIEMPRE)
    all_functions.update({
        'should_show_devolucion_button': should_show_devolucion_button,
        'should_show_gestion_devolucion_button': should_show_gestion_devolucion_button,
        'should_show_novedad_button': should_show_novedad_button,
        'should_show_gestion_novedad_button': should_show_gestion_novedad_button,
        'should_show_aprobacion_buttons': should_show_aprobacion_buttons,
        'should_show_detalle_button': should_show_detalle_button,
        'has_gestion_completa': has_gestion_completa,
        'is_oficina_role': is_oficina_role,
        'can_create_or_view': can_create_or_view,
        'get_user_role': get_user_role,
        'filtrar_por_oficina_usuario': filtrar_por_oficina_usuario,
        'verificar_acceso_oficina': verificar_acceso_oficina,
    })
    
    # Funciones adicionales
    def can_view_solicitud_detalle():
        """Todos los roles pueden ver detalles de solicitudes"""
        return True
    
    def get_estados_novedad():
        """Obtiene los estados de novedad"""
        return {
            'registrada': 'registrada',
            'aceptada': 'aceptada', 
            'rechazada': 'rechazada'
        }
    
    all_functions.update({
        'can_view_solicitud_detalle': can_view_solicitud_detalle,
        'get_estados_novedad': get_estados_novedad,
        'session_timeout_minutes': SESSION_TIMEOUT_MINUTES
    })
    
    return all_functions

# ============================================================================
# 12. REGISTRO DE BLUEPRINTS
# ============================================================================

 
app.register_blueprint(auth_bp)
logger.info("Blueprint de autenticación registrado (con url_prefix='/auth' desde auth.py)")

app.register_blueprint(materiales_bp)
app.register_blueprint(solicitudes_bp, url_prefix='/solicitudes')
app.register_blueprint(oficinas_bp)
app.register_blueprint(aprobadores_bp)
app.register_blueprint(reportes_bp)
app.register_blueprint(api_bp)
app.register_blueprint(usuarios_bp)
app.register_blueprint(certificado_bp)
logger.info("Blueprint de certificados registrado")

# Registrar blueprints opcionales
app.register_blueprint(prestamos_bp, url_prefix='/prestamos')
logger.info("Blueprint de préstamos registrado")

app.register_blueprint(inventario_corporativo_bp, url_prefix='/inventario-corporativo')
logger.info("Blueprint de inventario corporativo registrado")

app.register_blueprint(confirmacion_bp, url_prefix='/confirmacion')
logger.info("Blueprint de confirmaciones registrado")

# ============================================================================
# 13. RUTAS PRINCIPALES (UNIFICADAS)
# ============================================================================

@app.route('/')
def index():
    """Redirige usuarios autenticados al dashboard, otros al login"""
    if 'usuario_id' in session:
        return redirect('/dashboard')
    return redirect('/auth/login')

@app.route('/dashboard')
def dashboard():
    """Página principal del dashboard de la aplicación"""
    if 'usuario_id' not in session:
        logger.warning("Intento de acceso al dashboard sin autenticación")
        return redirect('/auth/login')
    
    try:
        oficina_id = None if user_can_view_all() else session.get('oficina_id')
        
        materiales = MaterialModel.obtener_todos(oficina_id) or []
        oficinas = OficinaModel.obtener_todas() or []
        solicitudes = SolicitudModel.obtener_todas(oficina_id) or []
        aprobadores = UsuarioModel.obtener_aprobadores() or []
        
        return render_template('dashboard.html',
            materiales=materiales,
            oficinas=oficinas,
            solicitudes=solicitudes,
            aprobadores=aprobadores
        )
    except Exception as e:
        logger.error("Error cargando dashboard")
        return render_template('dashboard.html',
            materiales=[],
            oficinas=[],
            solicitudes=[],
            aprobadores=[]
        )

@app.route('/logout', methods=['GET', 'POST'])
def logout():
    """Redirige logout al blueprint de auth"""
    return redirect('/auth/logout')

@app.route('/test-ldap', methods=['GET', 'POST'])
def test_ldap():
    """Redirige a la ruta de test-ldap en auth"""
    return redirect('/auth/test-ldap')  

# ============================================================================
# 15. API DE ESTADO DE SESIÓN
# ============================================================================

@app.route('/api/session-check')
def api_session_check():
    """API para verificar estado de sesión (útil para JavaScript)"""
    if 'usuario_id' not in session:
        return jsonify({'authenticated': False, 'reason': 'no_session'})
    
    last_activity = session.get('last_activity')
    if last_activity:
        try:
            if isinstance(last_activity, str):
                last_activity = datetime.fromisoformat(last_activity)
            
            inactive_time = datetime.now() - last_activity
            remaining_seconds = (timedelta(minutes=SESSION_TIMEOUT_MINUTES) - inactive_time).total_seconds()
            
            if remaining_seconds <= 0:
                return jsonify({'authenticated': False, 'reason': 'timeout'})
            
            return jsonify({
                'authenticated': True,
                'user': session.get('usuario_nombre'),
                'remaining_seconds': max(0, int(remaining_seconds)),
                'timeout_minutes': SESSION_TIMEOUT_MINUTES
            })
        except Exception:
            pass
    
    return jsonify({'authenticated': True, 'user': session.get('usuario_nombre')})

# ============================================================================
# 16. RUTAS DE MATERIALES (BACKUP)
# ============================================================================

@app.route('/materiales/crear', methods=['GET', 'POST'])
def crear_material_backup():
    """Ruta de respaldo para crear material"""
    if 'usuario_id' not in session:
        return redirect('/auth/login')
    
    if not has_gestion_completa():
        flash('No tiene permisos para crear materiales', 'danger')
        return redirect('/materiales')
    
    if request.method == 'POST':
        try:
            nombre = request.form.get('nombre')
            descripcion = request.form.get('descripcion')
            stock = int(request.form.get('stock', 0))
            stock_minimo = int(request.form.get('stock_minimo', 0))
            categoria = request.form.get('categoria')
            
            if not nombre:
                flash('El nombre es requerido', 'danger')
                return render_template('materiales/crear.html')
            
            nuevo_material = {
                'nombre': nombre,
                'descripcion': descripcion,
                'stock': stock,
                'stock_minimo': stock_minimo,
                'categoria': categoria
            }
            
            MaterialModel.crear(nuevo_material)
            flash('Material creado exitosamente', 'success')
            return redirect('/materiales')
            
        except Exception as e:
            logger.error("Error creando material")
            flash('Error creando material', 'danger')
    
    return render_template('materiales/crear.html')

# ============================================================================
# 17. RUTAS DE SOLICITUDES (BACKUP)
# ============================================================================

@app.route('/solicitudes/listar')
def listar_solicitudes_backup():
    """Ruta de respaldo para listar solicitudes"""
    if 'usuario_id' not in session:
        return redirect('/auth/login')
    
    try:
        oficina_id = None if user_can_view_all() else session.get('oficina_id')
        solicitudes = SolicitudModel.obtener_todas(oficina_id) or []
        
        return render_template('solicitudes/listar.html',
            solicitudes=solicitudes
        )
    except Exception as e:
        logger.error("Error listando solicitudes")
        flash('Error cargando solicitudes', 'danger')
        return redirect('/dashboard')

@app.route('/solicitudes/crear', methods=['GET', 'POST'])
def crear_solicitud_backup():
    """Ruta de respaldo para crear solicitud"""
    if 'usuario_id' not in session:
        return redirect('/auth/login')
    
    if request.method == 'POST':
        try:
            material_id = request.form.get('material_id')
            cantidad = int(request.form.get('cantidad', 1))
            comentario = request.form.get('comentario', '')
            
            if not material_id or cantidad <= 0:
                flash('Datos inválidos', 'danger')
                return redirect('/solicitudes/crear')
            
            nueva_solicitud = {
                'material_id': material_id,
                'usuario_id': session.get('usuario_id'),
                'cantidad': cantidad,
                'comentario': comentario,
                'estado': 'pendiente'
            }
            
            SolicitudModel.crear(nueva_solicitud)
            flash('Solicitud creada exitosamente', 'success')
            return redirect('/solicitudes')
            
        except Exception as e:
            logger.error("Error creando solicitud")
            flash('Error creando solicitud', 'danger')
    
    # Obtener materiales disponibles
    try:
        oficina_id = None if user_can_view_all() else session.get('oficina_id')
        materiales = MaterialModel.obtener_todos(oficina_id) or []
    except:
        materiales = []
    
    return render_template('solicitudes/crear.html',
        materiales=materiales
    )

# ============================================================================
# 18. RUTAS DE USUARIOS (BACKUP)
# ============================================================================

@app.route('/usuarios')
def listar_usuarios_backup():
    """Ruta de respaldo para listar usuarios"""
    if 'usuario_id' not in session:
        return redirect('/auth/login')
    
    if not has_gestion_completa():
        flash('No tiene permisos para ver usuarios', 'danger')
        return redirect('/dashboard')
    
    try:
        usuarios = UsuarioModel.obtener_todos() or []
        return render_template('usuarios/listar.html',
                               usuarios=usuarios,
                               total_usuarios=len(usuarios),
                               total_activos=len([u for u in usuarios if u.get('activo', True)]),
                               total_inactivos=len([u for u in usuarios if not u.get('activo', True)]))
    except Exception as e:
        logger.error("Error listando usuarios")
        flash('Error cargando usuarios', 'danger')
        return redirect('/dashboard')

# ============================================================================
# 19. RUTAS DE REPORTES (BACKUP)
# ============================================================================

@app.route('/reportes')
def reportes_redirect():
    """Compatibilidad: redirige /reportes -> /reportes/ (blueprint real)."""
    if 'usuario_id' not in session:
        return redirect('/auth/login')
    return redirect('/reportes/')
# ============================================================================
# 20. MANEJADORES DE ERRORES
# ============================================================================

@app.errorhandler(404)
def pagina_no_encontrada(error):
    """Maneja errores 404 - Página no encontrada"""
    logger.warning("Página no encontrada: %s", sanitizar_log_text(request.path))
    return render_template('error/404.html'), 404

@app.errorhandler(500)
def error_interno(error):
    """Maneja errores 500 - Error interno del servidor"""
    logger.exception("Error interno del servidor")
    return render_template('error/500.html'), 500

@app.errorhandler(413)
def archivo_demasiado_grande(error):
    """Maneja errores 413 - Archivo demasiado grande"""
    logger.warning("Intento de subir archivo demasiado grande: %s", sanitizar_log_text(request.url))
    flash('El archivo es demasiado grande. Tamaño máximo: 16MB', 'danger')
    return redirect(request.referrer or '/')

@app.errorhandler(401)
def no_autorizado(error):
    """Maneja errores 401 - No autorizado"""
    logger.warning("Acceso no autorizado: %s", sanitizar_log_text(request.path))
    flash('No está autorizado para acceder a esta página', 'danger')
    return redirect('/auth/login')

# ============================================================================
# 21. RUTAS DE SISTEMA
# ============================================================================

@app.route('/system/health')
def system_health():
    """Endpoint de salud del sistema"""
    try:
        # Verificar conexión a base de datos
        from database import get_database_connection
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.close()
        conn.close()
        
        # Verificar servicio de notificaciones
        notification_status = 'available' if servicio_notificaciones_disponible() else 'unavailable'
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected',
            'notifications': notification_status,
            'session': 'active' if 'usuario_id' in session else 'inactive',
            'blueprints': {
                'auth': 'registered',
                'materiales': 'registered',
                'solicitudes': 'registered',
                'oficinas': 'registered',
                'usuarios': 'registered'
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': 'internal_error'
        }), 500

@app.route('/system/info')
def system_info():
    """Información del sistema"""
    info = {
        'app_name': 'Sistema de Gestión de Inventarios',
        'version': '1.0.0',
        'environment': os.environ.get('FLASK_ENV', 'development'),
        'python_version': os.sys.version,
        'debug': app.debug,
        'session_timeout_minutes': SESSION_TIMEOUT_MINUTES,
        'upload_folder': app.config['UPLOAD_FOLDER'],
        'registered_blueprints': list(app.blueprints.keys()),
        'notifications_available': servicio_notificaciones_disponible()
    }
    return jsonify(info)

# ============================================================================
# 22. API ESTADÍSTICAS INVENTARIO CORPORATIVO (BACKUP)
# ============================================================================

@app.route('/inventario-corporativo/api/estadisticas-dashboard')
def api_estadisticas_inventario_dashboard():
    """API para obtener estadísticas del inventario corporativo para el dashboard"""
    if 'usuario_id' not in session:
        return jsonify({'error': 'No autorizado'}), 401
    
    try:
        from models.inventario_corporativo_model import InventarioCorporativoModel
        
        productos_todos = InventarioCorporativoModel.obtener_todos() or []
        productos_sede = InventarioCorporativoModel.obtener_por_sede_principal() or []
        productos_oficinas = InventarioCorporativoModel.obtener_por_oficinas_servicio() or []
        
        # Calcular estadísticas
        total_productos = len(productos_todos)
        valor_total = sum(float(p.get('valor_unitario', 0) or 0) * int(p.get('cantidad', 0) or 0) for p in productos_todos)
        productos_bajo_stock = sum(1 for p in productos_todos if int(p.get('cantidad', 0) or 0) <= int(p.get('stock_minimo', 5) or 5))
        
        return jsonify({
            "total_productos": total_productos,
            "valor_total": valor_total,
            "stock_bajo": productos_bajo_stock,
            "productos_sede": len(productos_sede),
            "productos_oficinas": len(productos_oficinas)
        })
        
    except Exception as e:
        logger.error("Error en API estadisticas inventario dashboard")
        return jsonify({
            "total_productos": 0,
            "valor_total": 0,
            "stock_bajo": 0,
            "productos_sede": 0,
            "productos_oficinas": 0,
            "error": "Error interno"
        })

if __name__ == '__main__':
    logger.info("Iniciando servidor Flask de Sistema de Gestión de Inventarios")
    logger.info("Logging de LDAP activo en: %s", sanitizar_log_text(ldap_log_file))
    
    # Verificar disponibilidad del servicio de notificaciones
    if not servicio_notificaciones_disponible():
        logger.warning("⚠️ El servicio de notificaciones no está disponible")
    else:
        logger.info("✅ Servicio de notificaciones configurado correctamente")
    
    try:
        # Inicialización del sistema - ahora devuelve True/False
        if inicializar_oficina_principal():
            logger.info("Inicialización de oficina completada correctamente")
        else:
            logger.warning("Inicialización de oficina tuvo problemas, pero el sistema continúa")
        logger.info("Sistema listo para operar")
    except Exception as e:
        logger.error("Error en inicialización")
        _dbg = os.environ.get('FLASK_DEBUG', 'false')
        if str(_dbg).strip().lower() in ('1', 'true', 'yes', 'y', 'si'):
            logger.exception("Detalle de excepción en inicialización")
        logger.warning("Continuando con la ejecución a pesar del error de inicialización")
    
    # Configuración del puerto
    port = int(os.environ.get('PORT', 5010))
    logger.info("Servidor iniciado en puerto: %s", sanitizar_log_text(port))

    # =======================
    # Seguridad de transporte
    # =======================
    # En producción, usa HTTPS (TLS) a través de un proxy (Nginx/Apache/Ingress)
    # o habilita ssl_context para el servidor de desarrollo.
    env_name = os.environ.get('FLASK_ENV', os.environ.get('ENV', 'development')).strip().lower()
    debug_env = os.environ.get('FLASK_DEBUG', 'false')
    debug_mode = (env_name != 'production') and (str(debug_env).strip().lower() in ('1', 'true', 'yes', 'y', 'si'))
    is_production = (env_name == 'production')

    # Permite forzar TLS por variable de entorno. En producción se recomienda activarlo.
    force_ssl = os.environ.get('FLASK_USE_SSL', '').lower() in ('1', 'true', 'yes', 'y') or is_production

    ssl_context = None
    if force_ssl:
        cert_path = os.environ.get('SSL_CERT_PATH')
        key_path = os.environ.get('SSL_KEY_PATH')

        if cert_path and key_path and os.path.exists(cert_path) and os.path.exists(key_path):
            ssl_context = (cert_path, key_path)
        else:
            # Certificado auto-firmado para pruebas (no usar para internet público).
            ssl_context = 'adhoc'

    
    # Verificar estructura de carpetas
    required_folders = ['templates', 'static', 'static/uploads', 'logs']
    for folder in required_folders:
        folder_path = os.path.join(BASE_DIR, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
            logger.info("Carpeta creada: %s", sanitizar_log_text(folder_path))
    
    app.run(
        debug=debug_mode,
        host='0.0.0.0',
        port=port,
        ssl_context=ssl_context
    )