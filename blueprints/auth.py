# blueprints/auth.py
import logging
import os
import uuid
import ipaddress
from datetime import datetime, timedelta
from functools import wraps

from flask import Blueprint, render_template, request, redirect, session, flash, current_app

from models.usuarios_model import UsuarioModel
from utils.helpers import sanitizar_log_text, sanitizar_username, sanitizar_ip

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

SESSION_TIMEOUT_MINUTES = 10
SESSION_ABSOLUTE_TIMEOUT_HOURS = 2


def init_session_config(app):
    """Configura cookies de sesión de forma segura según el entorno."""
    is_production = (
        os.getenv('FLASK_ENV') == 'production'
        or 'sugipq.qualitascolombia.com.co' in (os.getenv('SERVER_NAME', '') or '')
    )

    app.config['SESSION_COOKIE_SECURE'] = bool(is_production)
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=SESSION_ABSOLUTE_TIMEOUT_HOURS)

    # Solo en producción (dominio real)
    if is_production:
        app.config['SESSION_COOKIE_DOMAIN'] = '.qualitascolombia.com.co'

    logger.info(
        "[SESIÓN] Configuración: SECURE=%s, HTTPONLY=True, SAMESITE=Lax",
        app.config.get('SESSION_COOKIE_SECURE'),
    )
    logger.info(
        "[SESIÓN] Entorno: %s",
        'PRODUCCIÓN (HTTPS)' if is_production else 'DESARROLLO (HTTP)',
    )


def check_session_timeout() -> bool:
    if 'usuario_id' not in session:
        return False

    last_activity = session.get('last_activity')
    if not last_activity:
        return False

    try:
        if isinstance(last_activity, str):
            last_activity = datetime.fromisoformat(last_activity)

        inactive_time = datetime.now() - last_activity
        return inactive_time > timedelta(minutes=SESSION_TIMEOUT_MINUTES)
    except Exception:
        # No exponer detalles por logs
        return False


def update_session_activity():
    if 'usuario_id' in session:
        session['last_activity'] = datetime.now().isoformat()
        session.modified = True


def clear_session_safely():
    try:
        session.clear()
    except Exception:
        # No exponer detalles por logs
        pass


def require_login(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Por favor, inicie sesión para continuar', 'warning')
            return redirect('/auth/login')

        if check_session_timeout():
            clear_session_safely()
            flash(
                f'Su sesión ha expirado por inactividad ({SESSION_TIMEOUT_MINUTES} minutos). '
                'Por favor, inicie sesión nuevamente.',
                'warning',
            )
            return redirect('/auth/login')

        update_session_activity()
        return f(*args, **kwargs)

    return decorated_function


def assign_role_by_office(office_name: str) -> str:
    office = (office_name or '').lower().strip()

    if 'gerencia' in office:
        return 'admin'
    if 'almacén' in office or 'almacen' in office or 'logística' in office or 'logistica' in office:
        return 'almacen'
    if 'finanzas' in office or 'contabilidad' in office:
        return 'finanzas'
    if 'rrhh' in office or 'recursos humanos' in office:
        return 'rrhh'
    return 'usuario'


def get_client_info():
    # request.remote_addr puede ser None; no lo logueamos sin enmascarar.
    return {
        'ip': request.remote_addr,
        'user_agent': request.headers.get('User-Agent', 'Unknown'),
        'timestamp': datetime.now().isoformat(),
    }



def _is_private_ip(ip: str) -> bool:
    """Permite habilitar /auth/test-ldap en producción solo desde IPs privadas.

    Evita exponer la página públicamente en internet, pero no rompe el acceso
    interno (red 10.x/172.16-31.x/192.168.x).
    """
    try:
        if not ip:
            return False
        return ipaddress.ip_address(ip).is_private
    except Exception:
        return False

def _as_dict(raw):
    """Convierte salida de UsuarioModel a dict (sin asumir formato)."""
    if raw is None:
        return None

    if isinstance(raw, dict):
        return raw

    # Algunos modelos devuelven {'user': {...}} o {'data': {...}}
    if isinstance(raw, (list, tuple)) and raw and isinstance(raw[0], dict):
        return raw[0]

    # Objetos con atributos
    try:
        d = vars(raw)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _pick(d: dict, keys, default=None):
    for k in keys:
        if k in d and d.get(k) not in (None, ''):
            return d.get(k)
    return default


def _normalize_usuario_info(raw_info, fallback_username: str = ''):
    """Normaliza llaves para evitar KeyError y permitir login.

    Esto corrige el caso típico donde UsuarioModel devuelve columnas con
    nombres distintos (ej: 'ID', 'Nombre', 'Usuario', 'Rol', etc.).
    """
    d = _as_dict(raw_info)
    if not d:
        return None

    # Unwrap típico: {'user': {...}} / {'usuario': {...}} / {'data': {...}}
    for container_key in ('user', 'usuario', 'data', 'result', 'usuario_info'):
        if isinstance(d.get(container_key), dict):
            d = d[container_key]
            break

    usuario_id = _pick(d, ['id', 'ID', 'Id', 'usuario_id', 'UsuarioId', 'user_id', 'UserId'])
    if usuario_id in (None, ''):
        return None

    usuario_login = _pick(
        d,
        ['usuario', 'Usuario', 'username', 'Username', 'user', 'login', 'LOGIN'],
        default=fallback_username,
    )
    nombre = _pick(
        d,
        ['nombre', 'Nombre', 'full_name', 'FullName', 'name', 'Name'],
        default=usuario_login or fallback_username or 'Usuario',
    )

    oficina_id = _pick(d, ['oficina_id', 'OficinaId', 'office_id', 'id_oficina'], default=1)
    try:
        oficina_id = int(oficina_id) if oficina_id is not None else 1
    except Exception:
        oficina_id = 1

    oficina_nombre = _pick(
        d,
        ['oficina_nombre', 'OficinaNombre', 'office_name', 'OfficeName', 'oficina'],
        default='',
    )

    rol = _pick(d, ['rol', 'Rol', 'role', 'Role'], default='')
    rol = (rol or '').strip() or assign_role_by_office(oficina_nombre)

    return {
        'id': usuario_id,
        'usuario': usuario_login,
        'nombre': nombre,
        'rol': rol,
        'oficina_id': oficina_id,
        'oficina_nombre': oficina_nombre,
    }


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if 'usuario_id' in session:
        if check_session_timeout():
            clear_session_safely()
            return redirect('/auth/login')
        return redirect('/dashboard')

    if request.method == 'POST':
        usuario = (request.form.get('usuario') or '').strip()

        # Compatibilidad con distintos nombres del campo
        contraseña = (
            request.form.get('contraseña')
            or request.form.get('contrasena')
            or request.form.get('password')
            or ''
        )

        if not usuario or not contraseña:
            flash('Por favor, ingrese usuario y contraseña', 'warning')
            return render_template('auth/login.html')

        client_info = get_client_info()

        try:
            raw_info = UsuarioModel.verificar_credenciales(usuario, contraseña)
            usuario_info = _normalize_usuario_info(raw_info, fallback_username=usuario)

            if not usuario_info:
                flash('Usuario o contraseña incorrectos', 'danger')
                return render_template('auth/login.html')

            # ✅ Evitar session fixation: limpiar antes de setear.
            session.clear()
            session.permanent = True

            session['usuario_id'] = usuario_info['id']
            session['usuario_nombre'] = usuario_info['nombre']
            session['usuario'] = usuario_info['usuario']
            session['rol'] = usuario_info['rol']
            session['oficina_id'] = usuario_info.get('oficina_id', 1)
            session['oficina_nombre'] = usuario_info.get('oficina_nombre', '')

            now_iso = datetime.now().isoformat()
            session['login_time'] = now_iso
            session['last_activity'] = now_iso

            # Guardar IP en sesión (no en logs). Si no hay IP, dejar vacío.
            session['client_ip'] = client_info.get('ip')
            session.modified = True

            logger.info(
                "[SESIÓN] Creada para usuario=%s ip=%s",
                sanitizar_username(usuario_info.get('usuario')),
                sanitizar_ip(client_info.get('ip')),
            )

            flash(f'¡Bienvenido {usuario_info["nombre"]}!', 'success')
            return redirect('/dashboard')

        except Exception as e:
            error_id = uuid.uuid4().hex[:8]
            logger.error(
                "Error inesperado en login (error_id=%s, exc=%s)",
                sanitizar_log_text(error_id),
                sanitizar_log_text(type(e).__name__),
            )
            # Solo en desarrollo: stacktrace (evita exponerlo en prod)
            try:
                if current_app and current_app.debug:
                    logger.exception("Stacktrace login (error_id=%s)", sanitizar_log_text(error_id))
            except Exception:
                pass

            flash('Error inesperado, contacte a soporte', 'danger')
            return render_template('auth/login.html')

    return render_template('auth/login.html')


@auth_bp.route('/logout')
def logout():
    usuario = session.get('usuario', 'Desconocido')
    client_info = get_client_info()

    logger.info(
        "[SESIÓN] Logout usuario=%s ip=%s",
        sanitizar_username(usuario),
        sanitizar_ip(client_info.get('ip')),
    )

    clear_session_safely()
    flash('Sesión cerrada correctamente', 'info')
    return redirect('/auth/login')


@auth_bp.route('/test-ldap', methods=['GET', 'POST'])
def test_ldap():
    """Prueba de autenticación LDAP/AD y verificación de sincronización.

    Importante: por seguridad, este endpoint se deshabilita en producción.
    """

    # ✅ Evita exponer info LDAP en producción
    if (os.getenv('FLASK_ENV') or '').lower() == 'production':
        # En producción, permitir SOLO desde redes privadas (intranet).
        # Si necesitas habilitarlo fuera de intranet, hazlo explícito vía VPN.
        if not _is_private_ip(request.remote_addr):
            return ("Not Found", 404)
    result = None

    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''

        if not username or not password:
            flash('Debe ingresar usuario y contraseña', 'danger')
            return render_template('auth/test_ldap.html', result=None)

        try:
            # Preferir variables de entorno para no hardcodear valores
            def _env_bool(name: str, default: bool = False) -> bool:
                v = os.getenv(name)
                if v is None:
                    return default
                return str(v).strip().lower() in ("1", "true", "yes", "y", "si", "on")

            ldap_server = (os.getenv("LDAP_SERVER") or os.getenv("AD_SERVER") or '').strip()
            ldap_domain = (os.getenv("LDAP_DOMAIN") or '').strip()
            ldap_enabled = _env_bool("LDAP_ENABLED", bool(ldap_server))

            from utils.ldap_auth import ADAuth

            ad_auth = ADAuth()

            conn_res = ad_auth.test_connection()
            if isinstance(conn_res, dict):
                connection_ok = bool(conn_res.get('success'))
                connection_message = (conn_res.get('message') or '').strip()
                conn_meta = {
                    'server': conn_res.get('server'),
                    'port': conn_res.get('port'),
                    'use_ssl': conn_res.get('use_ssl'),
                }
            else:
                connection_ok = bool(conn_res)
                connection_message = 'Conexión al servidor LDAP exitosa' if connection_ok else 'Error de conexión'
                conn_meta = {}

            ad_user = ad_auth.authenticate_user(username, password)

            if ad_user:
                full_name = ad_user.get('full_name') or ad_user.get('nombre') or ad_user.get('name') or ''
                department = ad_user.get('department') or ad_user.get('departamento') or ''
                role_from_ad = ad_user.get('role') or ad_user.get('rol') or ''

                user_info = {
                    'username': ad_user.get('username') or username,
                    'full_name': full_name,
                    'email': ad_user.get('email') or '',
                    'department': department,
                    'role_from_ad': role_from_ad,
                    'groups_count': ad_user.get('groups_count') or '',
                }

                sync_info = None
                sync_error = None

                try:
                    db_user = UsuarioModel.get_by_username(username)

                    if db_user:
                        sync_info = {
                            'user_id': db_user.get('id'),
                            'system_role': db_user.get('rol'),
                            'office_id': db_user.get('oficina_id'),
                            'sync_status': 'Usuario existe en BD',
                        }
                    else:
                        sync_info = {
                            'user_id': None,
                            'system_role': role_from_ad or 'usuario',
                            'office_id': 1,
                            'sync_status': 'Usuario NO existe en BD (se creará en primer login)',
                        }

                except Exception:
                    sync_error = 'Error interno'

                # Nota: se deja esta info en DEV para diagnóstico
                result = {
                    'success': True,
                    'message': 'Autenticación exitosa',
                    'ldap_config': {
                        'enabled': ldap_enabled,
                        'server': ldap_server,
                        'domain': ldap_domain,
                    },
                    'connection': {
                        'status': 'OK' if connection_ok else 'Error',
                        'message': connection_message,
                        **{k: v for k, v in conn_meta.items() if v is not None},
                    },
                    'user_data': {
                        'username': user_info['username'],
                        'full_name': user_info['full_name'],
                        'email': user_info['email'],
                        'department': user_info['department'],
                        'role': user_info['role_from_ad'],
                    },
                    # compat template
                    'ldap_enabled': ldap_enabled,
                    'ldap_server': ldap_server,
                    'ldap_domain': ldap_domain,
                    'username': username,
                    'user_info': user_info,
                    'sync_info': sync_info,
                    'sync_error': sync_error,
                }

                flash('Autenticación LDAP exitosa', 'success')

            else:
                result = {
                    'success': False,
                    'message': 'Autenticación fallida',
                    'ldap_config': {
                        'enabled': ldap_enabled,
                        'server': ldap_server,
                        'domain': ldap_domain,
                    },
                    'connection': {
                        'status': 'OK' if connection_ok else 'Error',
                        'message': connection_message,
                        **{k: v for k, v in conn_meta.items() if v is not None},
                    },
                    'ldap_enabled': ldap_enabled,
                    'ldap_server': ldap_server,
                    'ldap_domain': ldap_domain,
                    'username': username,
                }

                flash('Usuario o contraseña incorrectos', 'danger')

        except Exception as e:
            error_id = uuid.uuid4().hex[:8]
            logger.error(
                "[LDAP] Error en prueba LDAP (error_id=%s, exc=%s)",
                sanitizar_log_text(error_id),
                sanitizar_log_text(type(e).__name__),
            )
            flash('Error interno al realizar la prueba LDAP', 'danger')
            result = {'success': False, 'message': 'Error interno'}

    return render_template('auth/test_ldap.html', result=result)
