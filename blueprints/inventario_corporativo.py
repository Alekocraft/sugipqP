# -*- coding: utf-8 -*-
# blueprints/inventario_corporativo.py

from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify, send_file
from werkzeug.utils import secure_filename
from models.inventario_corporativo_model import InventarioCorporativoModel
from models.oficinas_model import OficinaModel
from utils.permissions import can_access, can_manage_inventario_corporativo, can_view_inventario_actions, user_can_view_all
from database import get_database_connection
import os
import pandas as pd
from io import BytesIO
from datetime import datetime
from functools import wraps
import logging
import re
import unicodedata
from utils.helpers import sanitizar_log_text

logger = logging.getLogger(__name__)

_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _safe_sql_identifier(name: str, what: str = "identificador") -> str:
    """Valida identificadores SQL (tabla/columna) para evitar inyección en SQL dinámico."""
    if not name or not _SQL_IDENT_RE.match(name):
        raise ValueError(f"{what} SQL inválido")
    return name


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

def login_required(f):
    """Decorator to require an authenticated session."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Debe iniciar sesion para acceder a esta pagina', 'warning')
            return redirect('/auth/login')
        return f(*args, **kwargs)
    return decorated_function


try:
    from utils.ldap_auth import ad_auth
    LDAP_AVAILABLE = True
except ImportError:
    LDAP_AVAILABLE = False
    logger.warning("Servicio de directorio no disponible - búsqueda de usuarios deshabilitada")

try:
    from services.notification_service import NotificationService
    NOTIFICATIONS_AVAILABLE = True
except ImportError:
    NOTIFICATIONS_AVAILABLE = False
    logger.warning("Servicio de notificaciones no disponible")

try:
    from models.inventario_corporativo_model_extended import InventarioCorporativoModelExtended
    from models.confirmacion_asignaciones_model import ConfirmacionAsignacionesModel
    EXTENDED_MODEL_AVAILABLE = True
except ImportError:
    EXTENDED_MODEL_AVAILABLE = False
    logger.warning("Modelo extendido o de confirmaciones no disponible")

inventario_corporativo_bp = Blueprint(
    'inventario_corporativo',
    __name__,
    template_folder='templates'
)

def _require_login():
    return 'usuario_id' in session

def _can_approve_inv_requests() -> bool:
    """Determina si el usuario puede aprobar/rechazar solicitudes de devolución/traspaso."""
    rol = (session.get('rol') or '').strip().lower()
    # Roles típicos con capacidad de aprobación
    if rol in {'administrador', 'aprobador', 'lider_inventario'}:
        return True
    # Fallback: quien gestiona inventario corporativo suele poder aprobar
    return can_manage_inventario_corporativo()

def _handle_unauthorized():
    flash('No autorizado', 'danger')
    return redirect(url_for('inventario_corporativo.listar_inventario_corporativo'))

def _handle_not_found():
    flash('Producto no encontrado', 'danger')
    return redirect(url_for('inventario_corporativo.listar_inventario_corporativo'))

def _calculate_inventory_stats(productos):
    if not productos:
        return {
            'valor_total': 0,
            'productos_bajo_stock': 0,
            'productos_asignables': 0,
            'total_productos': 0
        }
    
    valor_total = sum(float(p.get('valor_unitario', 0)) * int(p.get('cantidad', 0)) for p in productos)
    productos_bajo_stock = len([p for p in productos if int(p.get('cantidad', 0)) <= int(p.get('cantidad_minima', 5))])
    productos_asignables = len([p for p in productos if p.get('es_asignable')])
    
    return {
        'valor_total': valor_total,
        'productos_bajo_stock': productos_bajo_stock,
        'productos_asignables': productos_asignables,
        'total_productos': len(productos)
    }



def _normalizar_texto_comparacion(valor: str) -> str:
    valor = unicodedata.normalize('NFKD', valor or '')
    valor = ''.join(ch for ch in valor if not unicodedata.combining(ch))
    return valor.strip().lower()


def _obtener_nombre_categoria(categorias, categoria_id) -> str:
    categoria_id_txt = str(categoria_id or '').strip()
    for categoria in categorias or []:
        cat_id = categoria.get('id') if isinstance(categoria, dict) else None
        if str(cat_id) == categoria_id_txt:
            return (categoria.get('nombre') or '').strip()
    return ''


def _es_categoria_tecnologia(nombre_categoria: str) -> bool:
    return _normalizar_texto_comparacion(nombre_categoria) == 'tecnologia'


def _categoria_requiere_serial_modelo(categorias, categoria_id) -> bool:
    nombre_categoria = _obtener_nombre_categoria(categorias, categoria_id)
    return _es_categoria_tecnologia(nombre_categoria)

def _handle_image_upload(archivo, producto_actual=None):
    if not archivo or not archivo.filename:
        return producto_actual.get('ruta_imagen') if producto_actual else None
    
    filename = secure_filename(archivo.filename)
    upload_dir = os.path.join('static', 'uploads', 'productos')
    os.makedirs(upload_dir, exist_ok=True)
    filepath = os.path.join(upload_dir, filename)
    archivo.save(filepath)
    return 'static/uploads/productos/' + filename

def _validate_product_form(categorias, proveedores):
    nombre = request.form.get('nombre', '').strip()
    categoria_id = request.form.get('categoria_id')
    proveedor_id = request.form.get('proveedor_id')
    modelo = request.form.get('modelo', '').strip()

    errors = []

    if not nombre:
        errors.append('El nombre es requerido')

    if not categoria_id:
        errors.append('La categoria es requerida')

    if not proveedor_id:
        errors.append('El proveedor es requerido')

    if categoria_id and _categoria_requiere_serial_modelo(categorias, categoria_id):
        if not modelo:
            errors.append('El modelo es obligatorio cuando la categoria es Tecnologia')

    return errors

@inventario_corporativo_bp.route('/')
def listar_inventario_corporativo():
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'view'):
        return _handle_unauthorized()

    productos = InventarioCorporativoModel.obtener_todos_con_oficina() or []
    categorias = InventarioCorporativoModel.obtener_categorias() or []
    proveedores = InventarioCorporativoModel.obtener_proveedores() or []

    stats = _calculate_inventory_stats(productos)

    return render_template('inventario_corporativo/listar.html',
        productos=productos,
        categorias=categorias,
        proveedores=proveedores,
        total_productos=stats['total_productos'],
        valor_total_inventario=stats['valor_total'],
        productos_bajo_stock=stats['productos_bajo_stock'],
        productos_asignables=stats['productos_asignables'],
        puede_gestionar_inventario=can_manage_inventario_corporativo(),
        puede_ver_acciones_inventario=can_view_inventario_actions()
    )

@inventario_corporativo_bp.route('/sede-principal')
def listar_sede_principal():
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'view'):
        return _handle_unauthorized()

    productos = InventarioCorporativoModel.obtener_por_sede_principal() or []
    categorias = InventarioCorporativoModel.obtener_categorias() or []
    proveedores = InventarioCorporativoModel.obtener_proveedores() or []

    stats = _calculate_inventory_stats(productos)

    return render_template('inventario_corporativo/listar_con_filtros.html',
        productos=productos,
        categorias=categorias,
        proveedores=proveedores,
        total_productos=stats['total_productos'],
        valor_total_inventario=stats['valor_total'],
        productos_bajo_stock=stats['productos_bajo_stock'],
        productos_asignables=stats['productos_asignables'],
        filtro_tipo='sede_principal',
        titulo='Inventario - Sede Principal (COQ)',
        puede_gestionar_inventario=can_manage_inventario_corporativo(),
        puede_ver_acciones_inventario=can_view_inventario_actions()
    )

@inventario_corporativo_bp.route('/oficinas-servicio')
def listar_oficinas_servicio():
    if not _require_login():
        return redirect('/login')

    
    if not (can_access('inventario_corporativo', 'view') or can_access('inventario_corporativo', 'view_oficinas_servicio')):
        return _handle_unauthorized()

    # Si el usuario NO puede ver todas las oficinas, solo debe ver lo asignado a su oficina
    puede_ver_todas = user_can_view_all()
    oficina_id = session.get('oficina_id')

    # Evitar variables no definidas para la vista global
    oficinas = []
    asignaciones = None
    if not puede_ver_todas and oficina_id:
        # Vista "Mi oficina": lo asignado (tipo reporte de oficinas) + botones de devolución/traspaso
        asignaciones = InventarioCorporativoModel.obtener_asignaciones_por_oficina(oficina_id) or []
        # Lista de oficinas para selector de traspaso
        oficinas = OficinaModel.obtener_todas() or []
        if not user_can_view_all():
            oficina_actual = session.get("oficina_id")
            oficinas = [o for o in oficinas if (o.get("id") or o.get("oficina_id")) != oficina_actual]

        productos = InventarioCorporativoModel.obtener_por_oficina(oficina_id) or []
        titulo = 'Inventario - Mi Oficina'
        mostrar_tabla_productos = False  # evitamos mostrar el inventario de todas las oficinas
    else:
        # Vista global (admin / roles con office_filter=all)
        productos = InventarioCorporativoModel.obtener_por_oficinas_servicio() or []
        titulo = 'Inventario - Oficinas de Servicio'
        mostrar_tabla_productos = True

    categorias = InventarioCorporativoModel.obtener_categorias() or []
    proveedores = InventarioCorporativoModel.obtener_proveedores() or []

    stats = _calculate_inventory_stats(productos)

    return render_template('inventario_corporativo/listar_con_filtros.html',
        productos=productos,
        asignaciones=asignaciones,
        oficinas=oficinas,
        categorias=categorias,
        proveedores=proveedores,
        total_productos=stats['total_productos'],
        valor_total_inventario=stats['valor_total'],
        productos_bajo_stock=stats['productos_bajo_stock'],
        productos_asignables=stats['productos_asignables'],
        filtro_tipo='oficinas_servicio',
        titulo=titulo,
        puede_gestionar_inventario=can_manage_inventario_corporativo(),
        # Acciones de gestión (ver/editar/asignar) SOLO para quienes gestionan inventario
        puede_ver_acciones_inventario=can_manage_inventario_corporativo(),
        # Acciones para oficinas (solicitudes)
        puede_solicitar_devolucion=can_access('inventario_corporativo', 'request_return') or can_access('inventario_corporativo', 'return'),
        puede_solicitar_traslado=can_access('inventario_corporativo', 'request_transfer') or can_access('inventario_corporativo', 'transfer'),
        puede_aprobar_solicitudes=_can_approve_inv_requests(),
        es_vista_oficinas_servicio=True,
        mostrar_tabla_productos=mostrar_tabla_productos,
        vista_mi_oficina=(not puede_ver_todas)
    )

@inventario_corporativo_bp.route('/<int:producto_id>')
def ver_inventario_corporativo(producto_id):
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'view'):
        return _handle_unauthorized()

    producto = InventarioCorporativoModel.obtener_por_id(producto_id)
    if not producto:
        return _handle_not_found()

    try:
        historial = InventarioCorporativoModel.historial_asignaciones(producto_id) or []
    except AttributeError:
        historial = []
        logger.warning("Metodo historial_movimientos no disponible")

    return render_template('inventario_corporativo/detalle.html',
        producto=producto,
        historial=historial
    )

@inventario_corporativo_bp.route('/crear', methods=['GET', 'POST'])
def crear_inventario_corporativo():
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'create'):
        return _handle_unauthorized()

    categorias = InventarioCorporativoModel.obtener_categorias() or []
    proveedores = InventarioCorporativoModel.obtener_proveedores() or []

    if request.method == 'POST':
        errors = _validate_product_form(categorias, proveedores)
        if errors:
            for error in errors:
                flash(error, 'danger')
            return redirect(request.path)

        try:
            ruta_imagen = _handle_image_upload(request.files.get('imagen'))

            codigo_unico = request.form.get('codigo_unico')
            if not codigo_unico:
                codigo_unico = InventarioCorporativoModel.generar_codigo_unico()

            nuevo_id = InventarioCorporativoModel.crear(
                codigo_unico=codigo_unico,
                nombre=request.form.get('nombre'),
                descripcion=request.form.get('descripcion'),
                categoria_id=int(request.form.get('categoria_id')),
                proveedor_id=int(request.form.get('proveedor_id')),
                valor_unitario=float(request.form.get('valor_unitario', 0)),
                cantidad=int(request.form.get('cantidad', 0)),
                cantidad_minima=int(request.form.get('cantidad_minima', 0)),
                ubicacion=request.form.get('ubicacion', ''),
                es_asignable=1 if 'es_asignable' in request.form else 0,
                usuario_creador=session.get('usuario', 'Sistema'),
                ruta_imagen=ruta_imagen,
                serial=None,
                modelo=request.form.get('modelo', '').strip() or None
            )

            if nuevo_id:
                flash('Producto creado correctamente.', 'success')
                return redirect(url_for('inventario_corporativo.listar_inventario_corporativo'))
            else:
                flash('Error al crear producto.', 'danger')

        except Exception as e:
            logger.error("[ERROR CREAR] [error](%s)", "error")
            flash('Error al crear producto.', 'danger')

    return render_template('inventario_corporativo/crear.html',
        categorias=categorias,
        proveedores=proveedores
    )

@inventario_corporativo_bp.route('/<int:producto_id>/editar', methods=['GET', 'POST'])
def editar_inventario_corporativo(producto_id):
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'edit'):
        return _handle_unauthorized()

    producto = InventarioCorporativoModel.obtener_por_id(producto_id)
    if not producto:
        return _handle_not_found()

    categorias = InventarioCorporativoModel.obtener_categorias() or []
    proveedores = InventarioCorporativoModel.obtener_proveedores() or []

    if request.method == 'POST':
        errors = _validate_product_form(categorias, proveedores)
        if errors:
            for error in errors:
                flash(error, 'danger')
            return redirect(request.path)

        try:
            ruta_imagen = _handle_image_upload(request.files.get('imagen'), producto)

            actualizado = InventarioCorporativoModel.actualizar(
                producto_id=producto_id,
                codigo_unico=request.form.get('codigo_unico'),
                nombre=request.form.get('nombre'),
                descripcion=request.form.get('descripcion'),
                categoria_id=int(request.form.get('categoria_id')),
                proveedor_id=int(request.form.get('proveedor_id')),
                valor_unitario=float(request.form.get('valor_unitario', 0)),
                cantidad=int(request.form.get('cantidad', 0)),
                cantidad_minima=int(request.form.get('cantidad_minima', 0)),
                ubicacion=request.form.get('ubicacion', producto.get('ubicacion', '')),
                es_asignable=1 if 'es_asignable' in request.form else 0,
                ruta_imagen=ruta_imagen,
                serial=producto.get('serial'),
                modelo=request.form.get('modelo', '').strip() or None
            )

            if actualizado:
                flash('Producto actualizado correctamente.', 'success')
                return redirect(url_for('inventario_corporativo.ver_inventario_corporativo', producto_id=producto_id))
            else:
                flash('Error al actualizar producto.', 'danger')

        except Exception as e:
            logger.error("[ERROR EDITAR] [error](%s)", "error")
            flash('Error al actualizar producto.', 'danger')

    return render_template('inventario_corporativo/editar.html',
        producto=producto,
        categorias=categorias,
        proveedores=proveedores
    )

@inventario_corporativo_bp.route('/<int:producto_id>/eliminar', methods=['POST'])
def eliminar_inventario_corporativo(producto_id):
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'delete'):
        return _handle_unauthorized()

    producto = InventarioCorporativoModel.obtener_por_id(producto_id)
    if not producto:
        return _handle_not_found()

    try:
        InventarioCorporativoModel.eliminar(producto_id, session.get('usuario', 'Sistema'))
        flash('Producto eliminado correctamente.', 'success')
    except Exception as e:
        logger.error("[ERROR ELIMINAR] [error](%s)", "error")
        flash('Error al eliminar producto.', 'danger')

    return redirect(url_for('inventario_corporativo.listar_inventario_corporativo'))

@inventario_corporativo_bp.route('/<int:producto_id>/asignar', methods=['GET', 'POST'])
def asignar_inventario_corporativo(producto_id):
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'assign'):
        return _handle_unauthorized()

    producto = InventarioCorporativoModel.obtener_por_id(producto_id)
    if not producto:
        return _handle_not_found()

    if not producto.get('es_asignable'):
        flash('Este producto no es asignable.', 'warning')
        return redirect(url_for('inventario_corporativo.ver_inventario_corporativo', producto_id=producto_id))

    oficinas = InventarioCorporativoModel.obtener_oficinas() or []
    
    try:
        historial = InventarioCorporativoModel.historial_asignaciones(producto_id) or []
    except AttributeError:
        historial = []
        logger.warning("Metodo historial_asignaciones no disponible")

    if request.method == 'POST':
        try:
            oficina_id = int(request.form.get('oficina_id') or 0)
            cantidad_asignar = int(request.form.get('cantidad') or 0)
            
            usuario_ad_username = request.form.get('usuario_ad_username', '').strip()
            usuario_ad_nombre = request.form.get('usuario_ad_nombre', '').strip()
            usuario_ad_email = request.form.get('usuario_ad_email', '').strip()
            raw_notif = (request.form.get('enviar_notificacion') or '').strip().lower()
            enviar_notificacion = raw_notif in ('1', 'true', 'on', 'yes', 'y', 'si')
            serial_asignacion = (request.form.get('serial_asignacion') or '').strip()
            requiere_serial_asignacion = _es_categoria_tecnologia(producto.get('categoria', ''))

            if requiere_serial_asignacion and not serial_asignacion:
                flash('El serial es obligatorio al asignar productos de la categoria Tecnologia.', 'danger')
                return redirect(request.path)

            if cantidad_asignar > producto.get('cantidad', 0):
                flash('No hay suficiente stock.', 'danger')
                return redirect(request.path)
            
            if not oficina_id:
                flash('Debe seleccionar una oficina.', 'danger')
                return redirect(request.path)

            oficina_nombre = next(
                (o['nombre'] for o in oficinas if o['id'] == oficina_id), 
                'Oficina'
            )

            usuario_ad_info = None
            if usuario_ad_username:
                usuario_ad_info = {
                    'username': usuario_ad_username,
                    'full_name': usuario_ad_nombre or usuario_ad_username,
                    'email': usuario_ad_email,
                    'department': ''
                }

            if usuario_ad_info and EXTENDED_MODEL_AVAILABLE:
                resultado = InventarioCorporativoModelExtended.asignar_a_usuario_ad_con_confirmacion(
                    producto_id=producto_id,
                    oficina_id=oficina_id,
                    cantidad=cantidad_asignar,
                    usuario_ad_info=usuario_ad_info,
                    usuario_accion=session.get('usuario', 'Sistema'),
                    serial_asignacion=serial_asignacion or None
                )
                
                if resultado.get('success'):
                    flash('Producto asignado correctamente.', 'success')
                    
                    producto_info = {
                        'nombre': producto.get('nombre', 'Producto'),
                        'codigo_unico': producto.get('codigo_unico', 'N/A'),
                        'categoria': producto.get('categoria', 'N/A'),
                        'serial': serial_asignacion or producto.get('serial', ''),
                        'modelo': producto.get('modelo', '')
                    }
                    
                    if enviar_notificacion and usuario_ad_email and NOTIFICATIONS_AVAILABLE:
                        try:
                            base_url = os.getenv('APP_BASE_URL', request.url_root.rstrip('/'))
                            
                            exito_email = NotificationService.enviar_notificacion_asignacion_con_confirmacion(
                                destinatario_email=usuario_ad_email,
                                destinatario_nombre=usuario_ad_nombre or usuario_ad_username,
                                producto_info=producto_info,
                                cantidad=cantidad_asignar,
                                oficina_nombre=oficina_nombre,
                                asignador_nombre=session.get('usuario_nombre', session.get('usuario', 'Sistema')),
                                token_confirmacion=resultado.get('token'),
                                base_url=base_url
                            )
                            
                            if exito_email:
                                flash(f'Notificacion enviada a {usuario_ad_email}', 'info')
                                if resultado.get('token'):
                                    flash(f'Link de confirmacion generado (valido 8 dias)', 'info')
                            else:
                                flash('No se pudo enviar el email de notificacion', 'warning')
                                
                        except Exception as e:
                            logger.error("Error enviando notificacion: [error](%s)", "error")
                            flash('Producto asignado pero no se pudo enviar la notificacion.', 'warning')
                    else:
                        if not usuario_ad_email:
                            flash('No se envio notificacion: usuario sin email', 'info')
                    
                    return redirect(url_for('inventario_corporativo.ver_inventario_corporativo', producto_id=producto_id))
                else:
                    flash(resultado.get('message', 'No se pudo asignar el producto.'), 'danger')
            else:
                asignado = InventarioCorporativoModel.asignar_a_oficina(
                    producto_id=producto_id,
                    oficina_id=oficina_id,
                    cantidad=cantidad_asignar,
                    usuario_accion=session.get('usuario', 'Sistema'),
                    serial_asignacion=serial_asignacion or None
                )

                if asignado:
                    flash('Producto asignado correctamente.', 'success')
                    return redirect(url_for('inventario_corporativo.ver_inventario_corporativo', producto_id=producto_id))
                else:
                    flash('No se pudo asignar el producto.', 'danger')

        except Exception as e:
            logger.error("[ERROR ASIGNAR] [error](%s)", "error")
            flash('Error al asignar producto.', 'danger')

    return render_template(
        'inventario_corporativo/asignar.html',
        producto=producto,
        oficinas=oficinas,
        historial=historial,
        ldap_disponible=LDAP_AVAILABLE,
        requiere_serial_asignacion=_es_categoria_tecnologia(producto.get('categoria', ''))
    )

@inventario_corporativo_bp.route('/api/buscar-usuarios-ad')
def api_buscar_usuarios_ad():
    if not _require_login():
        return jsonify({'error': 'No autorizado'}), 401
    
    if not LDAP_AVAILABLE:
        return jsonify({
            'error': 'Busqueda de usuarios AD no disponible',
            'usuarios': []
        }), 503

    termino = request.args.get('q', '').strip()
    
    if len(termino) < 3:
        return jsonify({
            'error': 'Ingrese al menos 3 caracteres para buscar',
            'usuarios': []
        })
    
    try:
        usuarios = ad_auth.search_user_by_name(termino)
        
        return jsonify({
            'success': True,
            'usuarios': usuarios,
            'total': len(usuarios)
        })
        
    except Exception as e:
        logger.error("Error buscando usuarios AD: [error](%s)", "error")
        return jsonify({
            'error': 'Error al buscar usuarios',
            'usuarios': []
        }), 500

@inventario_corporativo_bp.route('/api/obtener-usuario-ad/<username>')
def api_obtener_usuario_ad(username):
    if not _require_login():
        return jsonify({'error': 'No autorizado'}), 401
    
    if not LDAP_AVAILABLE:
        return jsonify({'error': 'Servicio de directorio no disponible'}), 503

    try:
        usuarios = ad_auth.search_user_by_name(username)
        
        usuario = next(
            (u for u in usuarios if u.get('usuario', '').lower() == username.lower()),
            None
        )
        
        if usuario:
            return jsonify({
                'success': True,
                'usuario': usuario
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Usuario no encontrado'
            }), 404
            
    except Exception as e:
        logger.error("Error obteniendo usuario AD: [error](%s)", "error")
        return jsonify({'error': 'Error al obtener usuario'}), 500

@inventario_corporativo_bp.route('/api/estadisticas-dashboard')
def api_estadisticas_dashboard():
    """Estadísticas rápidas para el modal del dashboard.

    - Para roles con vista global (user_can_view_all=True): devuelve totales globales + sede + oficinas.
    - Para roles de oficina (vista restringida): devuelve solo el inventario de su oficina en `mi_inventario`
      (y en `total_productos` por compatibilidad del frontend).
    """
    if not _require_login():
        return jsonify({'error': 'No autorizado'}), 401

    if not can_access('inventario_corporativo', 'view'):
        return jsonify({'error': 'Sin permisos'}), 403

    try:
        puede_ver_todas = user_can_view_all()
        oficina_id = session.get('oficina_id')

        # -------------------------
        # Vista restringida (oficina)
        # -------------------------
        if not puede_ver_todas and oficina_id:
            productos_mi = InventarioCorporativoModel.obtener_por_oficina(int(oficina_id)) or []
            stats_mi = _calculate_inventory_stats(productos_mi)

            return jsonify({
                'total_productos': stats_mi.get('total_productos', 0),
                'mi_inventario': stats_mi.get('total_productos', 0),
                'productos_sede': 0,
                'productos_oficinas': 0,
                'valor_total': stats_mi.get('valor_total', 0),
                'bajo_stock': stats_mi.get('productos_bajo_stock', 0),
                'asignables': stats_mi.get('productos_asignables', 0)
            })

        # -------------------------
        # Vista global (admin / roles con office_filter=all)
        # -------------------------
        productos = InventarioCorporativoModel.obtener_todos() or []
        stats = _calculate_inventory_stats(productos)

        productos_sede = InventarioCorporativoModel.obtener_por_sede_principal() or []
        productos_oficinas = InventarioCorporativoModel.obtener_por_oficinas_servicio() or []

        stats_sede = _calculate_inventory_stats(productos_sede)
        stats_oficinas = _calculate_inventory_stats(productos_oficinas)

        mi_inventario = 0
        if oficina_id:
            productos_mi = InventarioCorporativoModel.obtener_por_oficina(int(oficina_id)) or []
            stats_mi = _calculate_inventory_stats(productos_mi)
            mi_inventario = stats_mi.get('total_productos', 0)

        return jsonify({
            'total_productos': stats.get('total_productos', 0),
            'mi_inventario': mi_inventario,
            'productos_sede': stats_sede.get('total_productos', 0),
            'productos_oficinas': stats_oficinas.get('total_productos', 0),
            'valor_total': stats.get('valor_total', 0),
            'bajo_stock': stats.get('productos_bajo_stock', 0),
            'asignables': stats.get('productos_asignables', 0)
        })

    except Exception as e:
        logger.error("Error en API estadísticas dashboard: [error](%s)", "error")
        return jsonify({'error': 'Error interno'}), 500
@inventario_corporativo_bp.route('/api/estadisticas')
def api_estadisticas_inventario():
    if not _require_login():
        return jsonify({'error': 'No autorizado'}), 401

    try:
        productos = InventarioCorporativoModel.obtener_todos_con_oficina() or []
        stats = _calculate_inventory_stats(productos)

        productos_sede = [p for p in productos if not p.get('oficina') or p.get('oficina') == 'Sede Principal']
        productos_oficinas = [p for p in productos if p.get('oficina') and p.get('oficina') != 'Sede Principal']
        
        return jsonify({
            "total_productos": stats['total_productos'],
            "valor_total": stats['valor_total'],
            "stock_bajo": stats['productos_bajo_stock'],
            "productos_sede": len(productos_sede),
            "productos_oficinas": len(productos_oficinas)
        })
        
    except Exception as e:
        logger.error("Error en API estadisticas: [error](%s)", "error")
        return jsonify({
            "total_productos": 0,
            "valor_total": 0,
            "stock_bajo": 0,
            "productos_sede": 0,
            "productos_oficinas": 0
        })

@inventario_corporativo_bp.route('/exportar/excel/<tipo>')
def exportar_inventario_corporativo_excel(tipo):
    if not _require_login():
        return redirect('/login')

    if not can_access('inventario_corporativo', 'view'):
        return _handle_unauthorized()

    productos = InventarioCorporativoModel.obtener_todos_con_oficina() or []
    df = pd.DataFrame(productos)

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, download_name='inventario_corporativo.xlsx', as_attachment=True)
# ============================================================================
# API: SOLICITUDES (DEVOLUCION / TRASPASO) DESDE "MI OFICINA"
# ============================================================================

@inventario_corporativo_bp.route('/api/solicitar-devolucion', methods=['POST'])
@login_required
def api_solicitar_devolucion():
    """Crea una solicitud de devolución a COQ para un ítem asignado."""
    try:
        data = request.get_json(silent=True) or request.form or {}

        asignacion_id = _safe_int(data.get('asignacion_id') or 0)
        cantidad = _safe_int(data.get('cantidad') or 0)
        motivo = (data.get('motivo') or data.get('observacion') or data.get('observaciones') or '').strip()

        if asignacion_id <= 0:
            return jsonify(success=False, message='Asignación inválida.'), 400
        if cantidad <= 0:
            return jsonify(success=False, message='Cantidad inválida.'), 400
        if not motivo:
            return jsonify(success=False, message='El motivo es obligatorio.'), 400

        # Bloqueo de duplicados activos (devolución/traspaso) por asignación
        conn_check = get_database_connection()
        if conn_check:
            try:
                if _has_active_request_for_asignacion(conn_check, asignacion_id):
                    return jsonify(
                        success=False,
                        message='Ya existe una solicitud activa (devolución o traspaso) para esta asignación.'
                    ), 409
            finally:
                try:
                    conn_check.close()
                except Exception:
                    pass

        usuario_id = _session_user_id()
        username = _session_username() or 'sistema'

        # Compatibilidad con diferentes firmas del Model (por si cambia nombre de parámetro)
        try:
            ok, msg = InventarioCorporativoModel.crear_solicitud_devolucion(
                asignacion_id=asignacion_id,
                cantidad=cantidad,
                motivo=motivo,
                usuario_solicita=(usuario_id if usuario_id is not None else ((username if isinstance(username, str) else (None if username is None else '{0}'.format(username)))))
            )
        except TypeError:
            # Fallback: algunos modelos esperan usuario_solicita_id
            ok, msg = InventarioCorporativoModel.crear_solicitud_devolucion(
                asignacion_id,
                cantidad,
                motivo,
                (usuario_id if usuario_id is not None else ((username if isinstance(username, str) else (None if username is None else '{0}'.format(username)))))
            )

        return jsonify(success=ok, message=msg), (200 if ok else 400)

    except Exception as e:
        logger.error("Error creando solicitud de devolucion (api): [error](%s)", "error")
        return jsonify(success=False, message='Error interno del servidor'), 500

@inventario_corporativo_bp.route('/api/solicitar-traspaso', methods=['POST'])
@login_required
def api_solicitar_traspaso():
    """Crea una solicitud de traspaso entre oficinas para un ítem asignado."""
    try:
        data = request.get_json(silent=True) or request.form or {}

        asignacion_id = _safe_int(data.get('asignacion_id') or 0)
        cantidad = _safe_int(data.get('cantidad') or 0)
        destino_oficina_id = _safe_int(data.get('oficina_destino_id') or 0)

        destino_usuario = (
            (data.get('destino_usuario') or data.get('usuario_destino') or data.get('destinoUsuario') or '')
        ).strip()

        motivo = (data.get('motivo') or data.get('observaciones') or data.get('observacion') or '').strip()

        if asignacion_id <= 0:
            return jsonify(success=False, message='Asignación inválida.'), 400
        if cantidad <= 0:
            return jsonify(success=False, message='Cantidad inválida.'), 400
        if destino_oficina_id <= 0:
            return jsonify(success=False, message='Debe seleccionar la oficina destino.'), 400
        if not destino_usuario:
            return jsonify(success=False, message='Debe seleccionar el usuario destino.'), 400
        if not motivo:
            return jsonify(success=False, message='El motivo es obligatorio.'), 400

        # Bloqueo de duplicados activos (devolución/traspaso) por asignación
        conn_check = get_database_connection()
        if conn_check:
            try:
                if _has_active_request_for_asignacion(conn_check, asignacion_id):
                    return jsonify(
                        success=False,
                        message='Ya existe una solicitud activa (devolución o traspaso) para esta asignación.'
                    ), 409
            finally:
                try:
                    conn_check.close()
                except Exception:
                    pass

        usuario_id = _session_user_id()
        username = _session_username() or 'sistema'

        # Compatibilidad de firmas (por si el modelo usa nombres distintos)
        try:
            ok, msg = InventarioCorporativoModel.crear_solicitud_traspaso(
                asignacion_id=asignacion_id,
                cantidad=cantidad,
                destino_oficina_id=destino_oficina_id,
                destino_usuario=destino_usuario,
                motivo=motivo,
                usuario_solicita=(usuario_id if usuario_id is not None else ((username if isinstance(username, str) else (None if username is None else '{0}'.format(username)))))
            )
        except TypeError:
            # Alternativas comunes de nombre de parámetro
            try:
                ok, msg = InventarioCorporativoModel.crear_solicitud_traspaso(
                    asignacion_id=asignacion_id,
                    cantidad=cantidad,
                    oficina_destino_id=destino_oficina_id,
                    usuario_destino=destino_usuario,
                    motivo=motivo,
                    usuario_solicita=(usuario_id if usuario_id is not None else ((username if isinstance(username, str) else (None if username is None else '{0}'.format(username)))))
                )
            except TypeError:
                # Fallback posicional
                ok, msg = InventarioCorporativoModel.crear_solicitud_traspaso(
                    asignacion_id, cantidad, destino_oficina_id, destino_usuario, motivo,
                    (usuario_id if usuario_id is not None else ((username if isinstance(username, str) else (None if username is None else '{0}'.format(username)))))
                )

        return jsonify(success=ok, message=msg), (200 if ok else 400)

    except Exception as e:
        logger.error("Error creando solicitud de traspaso (api): [error](%s)", "error")
        return jsonify(success=False, message='Error interno del servidor'), 500



# ============================================================================
# API: LDAP - BÚSQUEDA DE USUARIOS (AUTOCOMPLETE)
# ============================================================================
@inventario_corporativo_bp.route('/api/ldap/buscar-usuarios', methods=['GET'])
@login_required
def api_ldap_buscar_usuarios():
    """Busca usuarios en Active Directory por nombre/usuario/email (para traspasos)."""
    try:
        term = (request.args.get('term') or request.args.get('q') or '').strip()
        if len(term) < 2:
            return jsonify({'success': True, 'users': []})

        # limitar para no cargar el AD
        term = term[:80]

        from utils.ldap_auth import ADAuth
        ad = ADAuth()
        users = ad.search_user_by_name(term) or []

        # Normalizar y limitar
        out = []
        for u in users[:10]:
            out.append({
                'nombre': u.get('nombre') or '',
                'usuario': u.get('usuario') or '',
                'email': u.get('email') or '',
                'departamento': u.get('departamento') or ''
            })
        return jsonify({'success': True, 'users': out})
    except Exception as e:
        logger.error("Error LDAP buscar usuarios: [error](%s)", "error")
        return jsonify({'success': False, 'users': [], 'message': 'Error consultando directorio activo'}), 500

# ============================================================================
# API: APROBACIÓN (DEVOLUCIÓN / TRASPASO) PARA ROLES APROBADORES / ADMIN
# ============================================================================

def _fetchall_dict(cursor):
    """Convierte un cursor pyodbc en lista de dict."""
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default

def _session_user_id():
    # En el proyecto se usa 'usuario_id' para el ID numérico
    for k in ('usuario_id', 'user_id', 'UsuarioId', 'id_usuario'):
        v = session.get(k)
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    return None

def _session_username():
    for k in ('usuario', 'username', 'usuario_nombre', 'Usuario', 'UserName'):
        v = session.get(k)
        if v:
            return v if isinstance(v, str) else '{0}'.format(v)
    return None

def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """SELECT 1
           FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?""",
        (table_name,)
    )
    return cur.fetchone() is not None

def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """SELECT 1
           FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=? AND COLUMN_NAME=?""",
        (table_name, column_name)
    )
    return cur.fetchone() is not None

def _first_existing_column(cur, table_name: str, candidates):
    for c in candidates:
        if _column_exists(cur, table_name, c):
            return c
    return None

def _has_active_request_for_asignacion(conn, asignacion_id: int):
    """Evita duplicados: si existe devolucion/traspaso activo no finalizado para la misma Asignación."""
    try:
        cur = conn.cursor()

        # Tablas (si existen en esta BD)
        devol_table = 'DevolucionesInventarioCorporativo'
        tras_table  = 'TraspasosInventarioCorporativo'

        has_dev = False
        has_tra = False

        if _table_exists(cur, devol_table):
            estado_col = _first_existing_column(cur, devol_table, ['EstadoDevolucion', 'Estado', 'estado'])
            activo_col = _first_existing_column(cur, devol_table, ['Activo', 'activo'])
            asig_col   = _first_existing_column(cur, devol_table, ['AsignacionId', 'asignacion_id'])
            if asig_col:
                where = f"[{_safe_sql_identifier(asig_col,'columna')}] = ?"
                params = [asignacion_id]
                if activo_col:
                    where += f" AND [{_safe_sql_identifier(activo_col,'columna')}] = 1"
                if estado_col:
                    # 'APROBADO/RECHAZADO' son los estados usados en el blueprint; si la BD usa otros, sigue funcionando por NOT IN
                    estado_col_safe = _safe_sql_identifier(estado_col, 'columna')
                    where += f" AND ( [{estado_col_safe}] IS NULL OR UPPER([{estado_col_safe}]) NOT IN ('APROBADO','RECHAZADO') )"
                devol_table_safe = _safe_sql_identifier(devol_table, 'tabla')
                cur.execute(f"SELECT TOP 1 1 FROM dbo.[{devol_table_safe}] WHERE {where}", params)
                has_dev = cur.fetchone() is not None

        if _table_exists(cur, tras_table):
            estado_col = _first_existing_column(cur, tras_table, ['EstadoTraspaso', 'Estado', 'estado'])
            activo_col = _first_existing_column(cur, tras_table, ['Activo', 'activo'])
            asig_col   = _first_existing_column(cur, tras_table, ['AsignacionId', 'asignacion_id'])
            if asig_col:
                where = f"[{_safe_sql_identifier(asig_col,'columna')}] = ?"
                params = [asignacion_id]
                if activo_col:
                    where += f" AND [{_safe_sql_identifier(activo_col,'columna')}] = 1"
                if estado_col:
                    estado_col_safe = _safe_sql_identifier(estado_col, 'columna')
                    where += f" AND ( [{estado_col_safe}] IS NULL OR UPPER([{estado_col_safe}]) NOT IN ('APROBADO','RECHAZADO') )"
                tras_table_safe = _safe_sql_identifier(tras_table, 'tabla')
                cur.execute(f"SELECT TOP 1 1 FROM dbo.[{tras_table_safe}] WHERE {where}", params)
                has_tra = cur.fetchone() is not None

        return has_dev or has_tra

    except Exception:
        # Si no se puede validar (por permisos/esquema), no bloqueamos el flujo
        return False


@inventario_corporativo_bp.route('/api/solicitudes-pendientes', methods=['GET'])
@login_required
def api_solicitudes_pendientes_inventario():
    """Lista solicitudes pendientes (devoluciones/traspasos) para aprobación."""
    if not _can_approve_inv_requests():
        return jsonify({'success': False, 'message': 'No autorizado'}), 403

    conn = get_database_connection()
    if not conn:
        return jsonify({'success': False, 'message': 'No hay conexión a base de datos'}), 500

    try:
        cur = conn.cursor()

        # Devoluciones (pendientes = cualquier estado distinto de Aprobado/Rechazado, y activo=1)
        q_devol = """
            SELECT
                'DEVOLUCION' AS tipo,
                d.DevolucionId AS solicitud_id,
                d.AsignacionId AS asignacion_id,
                d.ProductoId AS producto_id,
                p.NombreProducto AS producto_nombre,
                d.OficinaId AS oficina_origen_id,
                o.NombreOficina AS oficina_origen_nombre,
                NULL AS oficina_destino_id,
                NULL AS oficina_destino_nombre,
                d.Cantidad AS cantidad,
                d.Motivo AS motivo,
                d.EstadoDevolucion AS estado,
                d.UsuarioSolicita AS usuario_solicita,
                d.FechaSolicitud AS fecha_solicitud,
                a.UsuarioADNombre AS usuario_origen_nombre,
                a.UsuarioADEmail AS usuario_origen_email
            FROM dbo.DevolucionesInventarioCorporativo d
            INNER JOIN dbo.ProductosCorporativos p ON p.ProductoId = d.ProductoId
            INNER JOIN dbo.Oficinas o ON o.OficinaId = d.OficinaId
            LEFT JOIN dbo.Asignaciones a ON a.AsignacionId = d.AsignacionId
            WHERE d.Activo = 1
              AND UPPER(ISNULL(d.EstadoDevolucion,'')) NOT IN ('APROBADO','RECHAZADO')
        """

        # Traspasos
        q_tras = """
            SELECT
                'TRASPASO' AS tipo,
                t.TraspasoId AS solicitud_id,
                t.AsignacionOrigenId AS asignacion_id,
                t.ProductoId AS producto_id,
                p.NombreProducto AS producto_nombre,
                t.OficinaOrigenId AS oficina_origen_id,
                o1.NombreOficina AS oficina_origen_nombre,
                t.OficinaDestinoId AS oficina_destino_id,
                o2.NombreOficina AS oficina_destino_nombre,
                t.Cantidad AS cantidad,
                t.Motivo AS motivo,
                t.EstadoTraspaso AS estado,
                t.UsuarioSolicita AS usuario_solicita,
                t.FechaSolicitud AS fecha_solicitud,
                a.UsuarioADNombre AS usuario_origen_nombre,
                a.UsuarioADEmail AS usuario_origen_email
            FROM dbo.TraspasosInventarioCorporativo t
            INNER JOIN dbo.ProductosCorporativos p ON p.ProductoId = t.ProductoId
            INNER JOIN dbo.Oficinas o1 ON o1.OficinaId = t.OficinaOrigenId
            INNER JOIN dbo.Oficinas o2 ON o2.OficinaId = t.OficinaDestinoId
            LEFT JOIN dbo.Asignaciones a ON a.AsignacionId = t.AsignacionOrigenId
            WHERE t.Activo = 1
              AND UPPER(ISNULL(t.EstadoTraspaso,'')) NOT IN ('APROBADO','RECHAZADO')
        """

        cur.execute(q_devol)
        devol = _fetchall_dict(cur)

        cur.execute(q_tras)
        tras = _fetchall_dict(cur)

        # Ordenar por fecha desc
        data = devol + tras
        data.sort(key=lambda x: x.get('fecha_solicitud') or datetime.min, reverse=True)

        return jsonify({'success': True, 'data': data})

    except Exception as e:
        logger.error("Error listando solicitudes pendientes inventario: [error](%s)", "error")
        return jsonify({'success': False, 'message': 'Error interno del servidor'}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@inventario_corporativo_bp.route('/api/solicitudes/aprobar', methods=['POST'])
@login_required
def api_aprobar_solicitud_inventario():
    """Aprueba una solicitud (devolución/traspaso) sin romper esquemas distintos."""
    if not _can_approve_inv_requests():
        return jsonify({'success': False, 'message': 'No autorizado'}), 403

    data = request.get_json(silent=True) or request.form or {}
    tipo = (data.get('tipo') or '').strip().upper()
    solicitud_id = _safe_int(data.get('solicitud_id') or 0)
    observaciones = (data.get('observaciones') or '').strip()

    if solicitud_id <= 0 or tipo not in {'DEVOLUCION', 'TRASPASO'}:
        return jsonify({'success': False, 'message': 'Solicitud inválida'}), 400

    conn = get_database_connection()
    if not conn:
        return jsonify({'success': False, 'message': 'No hay conexión a base de datos'}), 500

    try:
        cur = conn.cursor()

        if tipo == 'DEVOLUCION':
            table = 'DevolucionesInventarioCorporativo'
            id_col = _first_existing_column(cur, table, ['DevolucionId', 'Id', 'devolucion_id'])
            estado_col = _first_existing_column(cur, table, ['EstadoDevolucion', 'Estado', 'estado'])
        else:
            table = 'TraspasosInventarioCorporativo'
            id_col = _first_existing_column(cur, table, ['TraspasoId', 'Id', 'traspaso_id'])
            estado_col = _first_existing_column(cur, table, ['EstadoTraspaso', 'Estado', 'estado'])

        if not _table_exists(cur, table) or not id_col or not estado_col:
            return jsonify({'success': False, 'message': 'Estructura de BD no compatible para aprobar'}), 500

        usuario_id = _session_user_id()
        username = _session_username() or 'sistema'

        usuario_col = _first_existing_column(cur, table, ['UsuarioApruebaId', 'UsuarioAprueba', 'AprobadoPor', 'AprobadoPorId'])
        fecha_col   = _first_existing_column(cur, table, ['FechaAprobacion', 'FechaAprobado', 'FechaGestion', 'FechaGestionAprobacion'])
        obs_col     = _first_existing_column(cur, table, ['ObservacionesAprobacion', 'ObservacionAprobacion', 'Observaciones', 'Observacion'])
        activo_col  = _first_existing_column(cur, table, ['Activo', 'activo'])

        sets = [f"{estado_col} = ?"]
        params = ['APROBADO']

        if usuario_col:
            if usuario_col.lower().endswith('id') and usuario_id is not None:
                sets.append(f"{usuario_col} = ?")
                params.append(int(usuario_id))
            else:
                sets.append(f"{usuario_col} = ?")
                params.append(('{0}'.format(username) if username is not None else None))

        if fecha_col:
            sets.append(f"{fecha_col} = GETDATE()")

        if obs_col:
            sets.append(f"{obs_col} = ?")
            params.append(observaciones or '')

        where = f"{id_col} = ?"
        params.append(solicitud_id)

        if activo_col:
            where += f" AND [{_safe_sql_identifier(activo_col,'columna')}] = 1"

        sql = f"UPDATE dbo.{table} SET " + ", ".join(sets) + " WHERE " + where
        cur.execute(sql, tuple(params))
        conn.commit()

        if cur.rowcount <= 0:
            return jsonify({'success': False, 'message': 'No se encontró la solicitud o ya fue gestionada'}), 404

        return jsonify({'success': True, 'message': 'Solicitud aprobada'})

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.error("Error aprobando solicitud inventario: [error](%s)", "error")
        return jsonify({'success': False, 'message': 'Error interno del servidor'}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

@inventario_corporativo_bp.route('/api/solicitudes/rechazar', methods=['POST'])
@login_required
def api_rechazar_solicitud_inventario():
    """Rechaza una solicitud (devolución/traspaso) sin romper esquemas distintos."""
    if not _can_approve_inv_requests():
        return jsonify({'success': False, 'message': 'No autorizado'}), 403

    data = request.get_json(silent=True) or request.form or {}
    tipo = (data.get('tipo') or '').strip().upper()
    solicitud_id = _safe_int(data.get('solicitud_id') or 0)
    observaciones = (data.get('observaciones') or '').strip()

    if solicitud_id <= 0 or tipo not in {'DEVOLUCION', 'TRASPASO'}:
        return jsonify({'success': False, 'message': 'Solicitud inválida'}), 400

    conn = get_database_connection()
    if not conn:
        return jsonify({'success': False, 'message': 'No hay conexión a base de datos'}), 500

    try:
        cur = conn.cursor()

        if tipo == 'DEVOLUCION':
            table = 'DevolucionesInventarioCorporativo'
            id_col = _first_existing_column(cur, table, ['DevolucionId', 'Id', 'devolucion_id'])
            estado_col = _first_existing_column(cur, table, ['EstadoDevolucion', 'Estado', 'estado'])
        else:
            table = 'TraspasosInventarioCorporativo'
            id_col = _first_existing_column(cur, table, ['TraspasoId', 'Id', 'traspaso_id'])
            estado_col = _first_existing_column(cur, table, ['EstadoTraspaso', 'Estado', 'estado'])

        if not _table_exists(cur, table) or not id_col or not estado_col:
            return jsonify({'success': False, 'message': 'Estructura de BD no compatible para rechazar'}), 500

        usuario_id = _session_user_id()
        username = _session_username() or 'sistema'

        usuario_col = _first_existing_column(cur, table, ['UsuarioApruebaId', 'UsuarioAprueba', 'AprobadoPor', 'AprobadoPorId'])
        fecha_col   = _first_existing_column(cur, table, ['FechaAprobacion', 'FechaAprobado', 'FechaGestion', 'FechaGestionAprobacion'])
        obs_col     = _first_existing_column(cur, table, ['ObservacionesAprobacion', 'ObservacionAprobacion', 'Observaciones', 'Observacion'])
        activo_col  = _first_existing_column(cur, table, ['Activo', 'activo'])

        sets = [f"{estado_col} = ?"]
        params = ['RECHAZADO']

        if usuario_col:
            if usuario_col.lower().endswith('id') and usuario_id is not None:
                sets.append(f"{usuario_col} = ?")
                params.append(int(usuario_id))
            else:
                sets.append(f"{usuario_col} = ?")
                params.append(('{0}'.format(username) if username is not None else None))

        if fecha_col:
            sets.append(f"{fecha_col} = GETDATE()")

        if obs_col:
            sets.append(f"{obs_col} = ?")
            params.append(observaciones or '')

        where = f"{id_col} = ?"
        params.append(solicitud_id)

        if activo_col:
            where += f" AND [{_safe_sql_identifier(activo_col,'columna')}] = 1"

        sql = f"UPDATE dbo.{table} SET " + ", ".join(sets) + " WHERE " + where
        cur.execute(sql, tuple(params))
        conn.commit()

        if cur.rowcount <= 0:
            return jsonify({'success': False, 'message': 'No se encontró la solicitud o ya fue gestionada'}), 404

        return jsonify({'success': True, 'message': 'Solicitud rechazada'})

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.error("Error rechazando solicitud inventario: [error](%s)", "error")
        return jsonify({'success': False, 'message': 'Error interno del servidor'}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

