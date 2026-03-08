# -*- coding: utf-8 -*-
# blueprints/confirmacion_asignaciones.py 

from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, render_template, request, session, flash, redirect, jsonify

from models.confirmacion_asignaciones_model import ConfirmacionAsignacionesModel
from utils.helpers import sanitizar_username, sanitizar_ip, sanitizar_log_text

logger = logging.getLogger(__name__)

# Crear el blueprint
confirmacion_bp = Blueprint('confirmacion', __name__, url_prefix='/confirmacion')

# -----------------------------------------------------------------------------
# Helpers: reparación automática de encoding para plantillas HTML (solo confirmación)
# -----------------------------------------------------------------------------

def _project_root() -> str:
    """
    Devuelve la raíz del proyecto (carpeta padre de /blueprints).
    Ej: .../sugipq
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _templates_dir() -> str:
    """
    Devuelve la ruta absoluta a /templates.
    """
    return os.path.join(_project_root(), 'templates')


def _ensure_template_utf8(template_name: str) -> Tuple[bool, str]:
    """
    Intenta convertir la plantilla indicada (ej: 'confirmacion/confirmado_exitoso.html')
    a UTF-8 si actualmente está en cp1252/latin1 u otra codificación incompatible.

    Retorna: (ok, mensaje)
    - ok True: si ya estaba en UTF-8 o si se convirtió exitosamente.
    - ok False: si no se pudo convertir.
    """
    # Normaliza separadores
    rel_path = template_name.replace('/', os.sep).replace('\\', os.sep)
    file_path = os.path.join(_templates_dir(), rel_path)

    if not os.path.isfile(file_path):
        return False, f"No se encontró la plantilla en disco: {file_path}"

    try:
        with open(file_path, 'rb') as f:
            raw = f.read()

         
        try:
            raw.decode('utf-8')
            return True, "Plantilla ya está en UTF-8"
        except UnicodeDecodeError:
            pass

         
        decoded: Optional[str] = None
        last_err: Optional[Exception] = None
        for enc in ('cp1252', 'latin-1'):
            try:
                decoded = raw.decode(enc)
                break
            except Exception as e:
                last_err = e

        if decoded is None:
            return False, f"No fue posible decodificar la plantilla con cp1252/latin-1. Error: {last_err}"

        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{file_path}.bak_{timestamp}"

        try:
            with open(backup_path, 'wb') as f:
                f.write(raw)
        except Exception as e:
            # Si no podemos respaldar, mejor no tocar nada
            return False, f"No se pudo crear backup de la plantilla. Error: {e}"

        # Escribir en UTF-8
        try:
            # newline='\n' para estabilidad entre entornos
            with open(file_path, 'w', encoding='utf-8', newline='\n') as f:
                f.write(decoded)
        except Exception as e:
            return False, f"No se pudo reescribir la plantilla en UTF-8. Error: {e}"

        return True, f"Plantilla convertida a UTF-8 correctamente. Backup: {backup_path}"

    except Exception as e:
        return False, f"Error inesperado al asegurar UTF-8 para plantilla: {e}"


def safe_render_template(template_name: str, **context: Any):
    """
    Renderiza una plantilla y, si falla por UnicodeDecodeError, intenta convertirla a UTF-8
    y reintenta una vez. Esto evita que el usuario vea 'error' cuando la confirmación
    ya se realizó en BD.
    """
    try:
        return render_template(template_name, **context)

    except UnicodeDecodeError as e:
        logger.error("❌ UnicodeDecodeError al renderizar plantilla '%s': %s. Intentando convertir a UTF-8 y reintentar...", sanitizar_log_text(template_name), 'Error interno')

        ok, msg = _ensure_template_utf8(template_name)
        if ok:
            logger.info("✅ Reparación de encoding aplicada para '%s': %s", sanitizar_log_text(template_name), sanitizar_log_text(msg))
            try:
                return render_template(template_name, **context)
            except Exception as e2:
                logger.error("❌ Falló el reintento de render_template('%s') después de convertir a UTF-8: %s", sanitizar_log_text(template_name), sanitizar_log_text(str(e2)))
        else:
            logger.error("❌ No se pudo reparar encoding para '%s': %s", sanitizar_log_text(template_name), sanitizar_log_text(msg))

        # Fallback: si era página de éxito, mostrar HTML mínimo en vez de error genérico
        resultado = context.get('resultado')
        if isinstance(resultado, dict) and resultado.get('success'):
            asignacion_id = resultado.get('asignacion_id', '')
            producto = resultado.get('producto_nombre', 'Producto')
            oficina = resultado.get('oficina_nombre', 'Oficina')
            usuario = resultado.get('usuario_nombre', '')
            fecha = resultado.get('fecha_confirmacion', '')

            return (
                f"<!doctype html>"
                f"<html lang='es'>"
                f"<head><meta charset='utf-8'><title>Confirmación exitosa</title></head>"
                f"<body style='font-family: Arial, sans-serif; padding: 24px;'>"
                f"<h2>✅ Asignación confirmada exitosamente</h2>"
                f"<p><b>Asignación:</b> {asignacion_id}</p>"
                f"<p><b>Producto:</b> {producto}</p>"
                f"<p><b>Oficina:</b> {oficina}</p>"
                f"<p><b>Usuario:</b> {usuario}</p>"
                f"<p><b>Fecha:</b> {fecha}</p>"
                f"<hr>"
                f"<p style='color:#666'>Nota: Se detectó un problema de codificación en la plantilla HTML y se intentó reparar automáticamente.</p>"
                f"</body></html>"
            )

        # Si no era éxito, caer al template de error (también con safe_render_template para evitar bucles)
        # Aquí usamos render_template directo para no entrar en recursión si error.html también está corrupta.
        try:
            return render_template(
                'confirmacion/error.html',
                error="Ocurrió un error al renderizar la página. Por favor, contacte al administrador."
            )
        except Exception:
            return (
                "<!doctype html><html lang='es'><head><meta charset='utf-8'>"
                "<title>Error</title></head><body style='font-family: Arial, sans-serif; padding: 24px;'>"
                "<h2>❌ Error</h2>"
                "<p>Ocurrió un error inesperado. Por favor, contacte al administrador.</p>"
                "</body></html>"
            )


# -----------------------------------------------------------------------------
# Rutas
# -----------------------------------------------------------------------------

@confirmacion_bp.route('/verificar/<string:token>', methods=['GET', 'POST'])
def verificar_credencial(token):
    """
    Verifica un token de confirmación y permite confirmar la asignación.
    Requiere autenticación contra Active Directory y número de cédula.
    """
    try:
        logger.info("Solicitud para verificar código de confirmación")
# Validar el token
        validacion = ConfirmacionAsignacionesModel.validar_token(token)

        if not validacion.get('es_valido'):
            mensaje_error = validacion.get('mensaje_error', 'Código inválido')
            logger.warning("Código inválido: %s", sanitizar_log_text(mensaje_error, max_len=160))
            return safe_render_template(
                'confirmacion/error.html',
                error=mensaje_error,
                ya_confirmado=validacion.get('ya_confirmado', False),
                expirado=validacion.get('expirado', False),
            )

        # Si es GET, mostrar formulario de confirmación con autenticación
        if request.method == 'GET':
            return safe_render_template(
                'confirmacion/confirmar.html',
                token=token,
                asignacion=validacion,
                ldap_disponible=True
            )

        # Si es POST, procesar la confirmación
        if request.method == 'POST':
            usuario_ad_username = request.form.get('usuario_ad_username', '').strip()
            usuario_ad_password = request.form.get('usuario_ad_password', '')
            numero_identificacion = request.form.get('numero_identificacion', '').strip()

            direccion_ip = request.remote_addr
            user_agent = request.headers.get('User-Agent', 'Unknown')

            logger.info("Intentando confirmar asignación %s para usuario: %s", sanitizar_log_text(validacion.get("asignacion_id")), sanitizar_username(usuario_ad_username))

            # Validar campos requeridos
            if not usuario_ad_username or not usuario_ad_password:
                flash('Usuario y contraseña son requeridos', 'danger')
                return safe_render_template(
                    'confirmacion/confirmar.html',
                    token=token,
                    asignacion=validacion,
                    ldap_disponible=True
                )

            if not numero_identificacion:
                flash('El número de cédula es requerido', 'danger')
                return safe_render_template(
                    'confirmacion/confirmar.html',
                    token=token,
                    asignacion=validacion,
                    ldap_disponible=True
                )

            resultado = ConfirmacionAsignacionesModel.confirmar_asignacion(
                token=token,
                username=usuario_ad_username,
                password=usuario_ad_password,
                numero_identificacion=numero_identificacion,
                direccion_ip=direccion_ip,
                user_agent=user_agent
            )

            logger.info("Resultado confirmación [success=%s asignacion=%s msg=%s usuario=%s ip=%s]", sanitizar_log_text(bool((resultado or {}).get("success"))), sanitizar_log_text((resultado or {}).get("asignacion_id", validacion.get("asignacion_id"))), sanitizar_log_text((resultado or {}).get("message", ""), max_len=160), sanitizar_username(usuario_ad_username), sanitizar_ip(direccion_ip))

            if resultado.get('success'):
                logger.info("✅ Asignación confirmada exitosamente [asignacion=%s usuario=%s]", sanitizar_log_text(validacion.get("asignacion_id")), sanitizar_username(usuario_ad_username))

                # Asegurar datos completos para plantilla
                datos_confirmacion: Dict[str, Any] = {
                    'success': True,
                    'message': resultado.get('message', 'Asignación confirmada exitosamente'),
                    'asignacion_id': resultado.get('asignacion_id', validacion.get('asignacion_id')),
                    'producto_nombre': resultado.get('producto_nombre', validacion.get('producto_nombre', 'Producto')),
                    'oficina_nombre': resultado.get('oficina_nombre', validacion.get('oficina_nombre', 'Oficina')),
                    'usuario_nombre': resultado.get('usuario_nombre', usuario_ad_username),
                    'cedula': resultado.get('cedula', numero_identificacion),
                    # Mantengo tu formato actual si viene en resultado; si no, formato humano
                    'fecha_confirmacion': resultado.get(
                        'fecha_confirmacion',
                        datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                    )
                }


                return safe_render_template(
                    'confirmacion/confirmado_exitoso.html',
                    resultado=datos_confirmacion,
                    asignacion=validacion
                )

            mensaje_error = resultado.get('message', 'Error al confirmar la asignación')
            logger.error("❌ Error confirmando asignación")
            flash(mensaje_error, 'danger')
            return safe_render_template(
                'confirmacion/confirmar.html',
                token=token,
                asignacion=validacion,
                ldap_disponible=True
            )

        # Fallback (no debería llegar)
        return safe_render_template('confirmacion/error.html', error="Método no soportado.")

    except Exception as e:
        error_msg = 'Error inesperado: Error interno'
        logger.error("%s", sanitizar_log_text(f"❌ Error en verificar_credencial: {error_msg}"))
        return safe_render_template(
            'confirmacion/error.html',
            error="Ocurrió un error inesperado. Por favor, contacte al administrador del sistema."
        )


@confirmacion_bp.route('/api/validar-cedula', methods=['POST'])
def api_validar_cedula():
    """
    API para validar número de cédula.
    """
    try:
        data = request.get_json() or {}
        cedula = (data.get('cedula') or '').strip()

        es_valida = ConfirmacionAsignacionesModel.validar_cedula_colombiana(cedula)

        return jsonify({
            'es_valida': es_valida,
            'mensaje': 'Cédula válida' if es_valida else 'Cédula inválida'
        })

    except Exception as e:
        logger.error("Error validando cédula: [error](%s)", 'Error')
        return jsonify({
            'es_valida': False,
            'mensaje': 'Error al validar cédula'
        }), 500


@confirmacion_bp.route('/mis-pendientes', methods=['GET'])
def mis_pendientes():
    """
    Muestra las confirmaciones pendientes del usuario actual.
    Requiere autenticación.
    """
    if 'usuario_id' not in session:
        flash('Por favor, inicie sesión para ver sus confirmaciones pendientes', 'warning')
        return redirect('/auth/login')

    try:
        usuario_email = session.get('usuario')

        pendientes = ConfirmacionAsignacionesModel.obtener_confirmaciones_pendientes(usuario_email)

        return safe_render_template(
            'confirmacion/mis_pendientes.html',
            pendientes=pendientes,
            total_pendientes=len(pendientes)
        )

    except Exception as e:
        error_msg = 'Error obteniendo confirmaciones pendientes: Error interno'
        logger.error("%s", sanitizar_log_text(f"❌ Error en mis_pendientes: {error_msg}"))
        flash('Error al cargar las confirmaciones pendientes', 'danger')
        return redirect('/dashboard')


@confirmacion_bp.route('/api/validar-token/<string:token>', methods=['GET'])
def api_validar_credencial(token):
    """
    API para validar un token (útil para integraciones o AJAX).
    """
    try:
        validacion = ConfirmacionAsignacionesModel.validar_token(token)

        respuesta = {
            'es_valido': validacion.get('es_valido', False),
            'mensaje': validacion.get('mensaje_error', 'Código válido') if not validacion.get('es_valido') else 'Código válido',
            'producto_nombre': validacion.get('producto_nombre', ''),
            'oficina_nombre': validacion.get('oficina_nombre', ''),
            'usuario_nombre': validacion.get('usuario_ad_nombre', ''),
            'fecha_asignacion': validacion.get('fecha_asignacion').isoformat() if validacion.get('fecha_asignacion') else None,
            'dias_restantes': validacion.get('dias_restantes', 0)
        }

        return jsonify(respuesta)

    except Exception as e:
        logger.error("❌ Error en api_validar_credencial: [error](%s)", 'Error')
        return jsonify({
            'es_valido': False,
            'mensaje': 'Error al validar el token',
            'error': 'Error interno'
        }), 500


@confirmacion_bp.route('/estadisticas', methods=['GET'])
def estadisticas():
    """
    Muestra estadísticas de confirmaciones.
    Solo para administradores.
    """
    if 'usuario_id' not in session:
        flash('Por favor, inicie sesión', 'warning')
        return redirect('/auth/login')

    if session.get('rol') not in ['administrador', 'admin']:
        flash('No tiene permisos para ver estadísticas', 'danger')
        return redirect('/dashboard')

    try:
        stats = ConfirmacionAsignacionesModel.obtener_estadisticas_confirmaciones()

        eliminados = ConfirmacionAsignacionesModel.limpiar_tokens_expirados()
        if eliminados > 0:
            flash(f'Se eliminaron {eliminados} tokens expirados', 'info')

        return safe_render_template('confirmacion/estadisticas.html', estadisticas=stats)

    except Exception as e:
        error_msg = 'Error obteniendo estadísticas: Error interno'
        logger.error("%s", sanitizar_log_text(f"❌ Error en estadisticas: {error_msg}"))
        flash('Error al cargar estadísticas', 'danger')
        return redirect('/dashboard')