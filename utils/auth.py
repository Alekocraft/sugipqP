# -*- coding: utf-8 -*-
# utils/auth.py
"""Helpers de autenticación y autorización para proteger rutas.

Este módulo provee:
- require_login(): valida sesión activa.
- has_role(*roles): valida rol en sesión.
- login_required / roles_required: decoradores para rutas.
"""

import logging
from functools import wraps
from flask import session, flash, redirect, url_for, request

from utils.helpers import sanitizar_log_text, sanitizar_username

logger = logging.getLogger(__name__)


def require_login() -> bool:
    """Retorna True si existe sesión de usuario."""
    is_authenticated = ('user_id' in session) or ('usuario_id' in session)
    return bool(is_authenticated)


def has_role(*roles: str) -> bool:
    """Verifica si el rol en sesión coincide con alguno de los roles indicados."""
    user_role = (session.get('rol', '') or '').strip().lower()
    target_roles = [str(r).strip().lower() for r in roles if r is not None]

    has_valid_role = (user_role in target_roles) if target_roles else False
    return bool(has_valid_role)


def login_required(f):
    """Decorador para requerir autenticación."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not require_login():
            logger.warning(
                "Intento de acceso no autenticado a %s",
                sanitizar_log_text(getattr(request, 'endpoint', '') or ''),
            )
            flash('Por favor inicie sesión para acceder a esta página.', 'warning')
            return redirect(url_for('auth_bp.login', next=request.url))

        return f(*args, **kwargs)
    return decorated_function


def roles_required(*roles: str):
    """Decorador para requerir autenticación + uno de los roles indicados."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not require_login():
                logger.warning(
                    "Intento de acceso no autenticado a ruta con roles %s",
                    sanitizar_log_text(roles),
                )
                flash('Por favor inicie sesión para acceder a esta página.', 'warning')
                return redirect(url_for('auth_bp.login', next=request.url))

            user_role = (session.get('rol', '') or '').strip().lower()
            if roles and user_role not in [str(r).strip().lower() for r in roles]:
                logger.warning(
                    "Usuario rol '%s' intentó acceder a ruta que requiere roles %s",
                    sanitizar_log_text(user_role),
                    sanitizar_log_text(roles),
                )
                flash('No tiene permisos para acceder a esta página.', 'danger')
                return redirect(url_for('dashboard'))

            return f(*args, **kwargs)
        return decorated_function
    return decorator


def get_user_data() -> dict:
    """Devuelve datos básicos de usuario desde sesión (sin exponer info sensible en logs)."""
    user_data = {
        'id': session.get('usuario_id') or session.get('user_id'),
        'nombre': session.get('usuario_nombre') or session.get('nombre') or '',
        'rol': session.get('rol') or '',
    }
    return user_data


def can_access_module(module_name: str) -> bool:
    """Chequeo simple de acceso a módulo (si la app usa listas en sesión)."""
    module_norm = (module_name or '').strip().lower()
    permissions = session.get('permisos_modulos') or session.get('permissions') or []
    try:
        allowed = module_norm in [str(x).strip().lower() for x in permissions]
    except Exception:
        allowed = False

    return bool(allowed)
