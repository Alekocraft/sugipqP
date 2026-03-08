# blueprints/materiales.py
 

from __future__ import annotations

# Standard library
import os
from datetime import datetime

# Third-party
import uuid
from flask import Blueprint, render_template, request, redirect, session, flash, url_for, current_app
from werkzeug.utils import secure_filename

# Local
from models.materiales_model import MaterialModel
from models.oficinas_model import OficinaModel
from utils.permissions import can_access
from utils.filters import verificar_acceso_oficina, filtrar_por_oficina_usuario
import logging
from utils.helpers import sanitizar_log_text, sanitizar_username

logger = logging.getLogger(__name__)

# ✅ Importación segura de SolicitudModel
try:
    from models.solicitudes_model import SolicitudModel
    SOLICITUD_MODEL_DISPONIBLE = True
except ImportError:
    SOLICITUD_MODEL_DISPONIBLE = False
    logger.warning(sanitizar_log_text("⚠️ SolicitudModel no disponible - estadísticas deshabilitadas"))

materiales_bp = Blueprint('materiales', __name__, url_prefix='/materiales')


def _require_login() -> bool:
    return 'usuario_id' in session


def _obtener_estadisticas_material(material_id):
    """
    Obtiene estadísticas de un material de forma segura.
    Retorna None si hay error o el modelo no está disponible.
    """
    if not SOLICITUD_MODEL_DISPONIBLE:
        return None
    
    try:
        return SolicitudModel.obtener_estadisticas_por_material(material_id)
    except Exception as e:
        logger.error(sanitizar_log_text(f"⚠️ Error obteniendo estadísticas para material {material_id}: {e}"))
        return None


@materiales_bp.route('/', methods=['GET'])
def listar_materiales():
    """Listar todos los materiales (protegido por permisos)."""
    if not can_access('materiales', 'view'):
        flash('❌ No tienes permisos para acceder a materiales', 'danger')
        logger.warning(sanitizar_log_text(f"🚫 Acceso denegado a /materiales - Usuario: {session.get('usuario_nombre')}"))
        return redirect('/dashboard')

    # Inicializar variables con valores por defecto
    materiales = []
    stats_dict = {}
    valor_total_inventario = 0
    total_solicitudes = 0
    total_entregado = 0

    try:
        logger.info(sanitizar_log_text("📦 Cargando lista de materiales..."))
        
        # ✅ Obtener todos los materiales
        todos_materiales = MaterialModel.obtener_todos() or []
        
        # ✅ Aplicar filtro por oficina según permisos del usuario
        materiales = filtrar_por_oficina_usuario(todos_materiales, 'oficina_id')
        
        logger.info(sanitizar_log_text(f"📦 Se cargaron {len(materiales)} materiales para mostrar (filtrados por oficina)"))
        
    except Exception as e:
        logger.error(sanitizar_log_text(f"❌ Error obteniendo materiales: {e}"))
        
        flash('Error al cargar los materiales', 'danger')
        # Continuar con lista vacía en lugar de fallar completamente
        materiales = []

    # ✅ OBTENER ESTADÍSTICAS DE FORMA SEGURA (separado del bloque principal)
    try:
        for material in materiales:
            try:
                material_id = material.get('id')
                if not material_id:
                    continue
                    
                # Obtener estadísticas de solicitudes para este material (de forma segura)
                stats = _obtener_estadisticas_material(material_id)
                if stats:
                    stats_dict[material_id] = stats
                
                # Calcular valores totales
                valor_total = float(material.get('valor_total', 0) or 0)
                valor_total_inventario += valor_total
                
                if stats and isinstance(stats, (list, tuple)) and len(stats) >= 4:
                    total_solicitudes += int(stats[0] or 0)
                    total_entregado += int(stats[3] or 0)
                    
            except Exception as e:
                logger.error(sanitizar_log_text(f"⚠️ Error procesando estadísticas del material {material.get('id', 'desconocido')}: {e}"))
                # Continuar con el siguiente material
                continue
                
    except Exception as e:
        logger.error(sanitizar_log_text(f"⚠️ Error general en cálculo de estadísticas: {e}"))
        # Las estadísticas fallan pero los materiales se muestran igual

    return render_template('materials/listar.html', 
                         materiales=materiales,
                         stats_dict=stats_dict,
                         valor_total_inventario=valor_total_inventario,
                         total_solicitudes=total_solicitudes,
                         total_entregado=total_entregado)


# RUTA GET PARA MOSTRAR EL FORMULARIO DE CREACIÓN
@materiales_bp.route('/crear', methods=['GET'])
def mostrar_formulario_creacion():
    """Mostrar el formulario para crear materiales"""
    if not _require_login():
        return redirect('/login')

    if not can_access('materiales', 'create'):
        flash('❌ No tienes permisos para crear materiales', 'danger')
        return redirect('/materiales')

    return render_template('materials/crear.html')


# RUTA POST PARA PROCESAR LA CREACIÓN DE MATERIALES
@materiales_bp.route('/crear', methods=['POST'])
def crear_materiales():
    """Procesar la creación de materiales"""
    if not _require_login():
        return redirect('/login')

    if not can_access('materiales', 'create'):
        flash('❌ No tienes permisos para crear materiales', 'danger')
        return redirect('/materiales')

    try:
        # Obtener datos del formulario
        materiales_data = []
        
        # Iterar sobre los materiales (hasta 10)
        for i in range(10):
            nombre = request.form.get(f'nombre_{i}')
            if not nombre:  # Si no hay nombre, asumir que no hay más materiales
                continue
                
            valor_unitario = request.form.get(f'valor_unitario_{i}')
            cantidad = request.form.get(f'cantidad_{i}')
            cantidad_minima = request.form.get(f'cantidad_minima_{i}')
            imagen = request.files.get(f'imagen_{i}')
            
            # Validar que todos los campos requeridos estén presentes
            if not all([nombre, valor_unitario, cantidad, cantidad_minima]):
                flash(f'Faltan campos requeridos en el material {i+1}', 'danger')
                continue
            
            # Procesar imagen y guardar ruta
            ruta_imagen = None
            if imagen and imagen.filename:
                filename = secure_filename(imagen.filename)
                # Crear nombre único para evitar colisiones
                unique_filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
                
                # Asegurar que el directorio de uploads existe
                upload_folder = current_app.config.get('UPLOAD_FOLDER', 'static/uploads')
                os.makedirs(upload_folder, exist_ok=True)
                
                filepath = os.path.join(upload_folder, unique_filename)
                imagen.save(filepath)
                ruta_imagen = unique_filename
                logger.info(sanitizar_log_text(f"✅ Imagen guardada en: {filepath}"))
            
            materiales_data.append({
                'nombre': nombre,
                'valor_unitario': float(valor_unitario),
                'cantidad': int(cantidad),
                'cantidad_minima': int(cantidad_minima),
                'ruta_imagen': ruta_imagen
            })
        
        # Crear materiales en la base de datos
        oficina_id = session.get('oficina_id')
        usuario_creador = session.get('username', 'Sistema')
        
        materiales_creados = 0
        for material in materiales_data:
            material_id = MaterialModel.crear(
                nombre=material['nombre'],
                valor_unitario=material['valor_unitario'],
                cantidad=material['cantidad'],
                oficina_id=oficina_id,
                usuario_creador=usuario_creador,
                ruta_imagen=material['ruta_imagen'],
                cantidad_minima=material['cantidad_minima']
            )
            
            if material_id:
                materiales_creados += 1
                logger.info(sanitizar_log_text(f"✅ Material creado: {material['nombre']} (ID: {material_id})"))
            else:
                flash(f'❌ Error al crear el material: {material["nombre"]}', 'danger')
        
        if materiales_creados > 0:
            flash(f'✅ {materiales_creados} materiales creados exitosamente', 'success')
        else:
            flash('❌ No se pudo crear ningún material', 'danger')
        
        return redirect('/materiales')
        
    except Exception as e:
        logger.error(sanitizar_log_text(f"❌ Error al crear materiales: {e}"))
        
        flash('Error al crear los materiales', 'danger')
        return redirect('/materiales/crear')


# RUTAS PARA EDITAR Y ELIMINAR
@materiales_bp.route('/editar/<int:material_id>', methods=['GET', 'POST'])
def editar_material(material_id):
    """Editar un material existente"""
    if not _require_login():
        return redirect('/login')

    if not can_access('materiales', 'edit'):
        flash('No tiene permisos para editar materiales', 'danger')
        return redirect('/materiales')

    if request.method == 'GET':
        # Obtener el material por ID
        material = MaterialModel.obtener_por_id(material_id)
        if not material:
            flash('Material no encontrado', 'danger')
            return redirect('/materiales')
        
        # Verificar que el usuario tiene acceso a la oficina del material
        if not verificar_acceso_oficina(material.get('oficina_id')):
            flash('No tiene permisos para editar este material', 'danger')
            return redirect('/materiales')

        return render_template('materials/editar.html', material=material)

    # Procesar POST para actualizar
    try:
        # Validar que el material existe y el usuario tiene permisos
        material_existente = MaterialModel.obtener_por_id(material_id)
        if not material_existente:
            flash('Material no encontrado', 'danger')
            return redirect('/materiales')

        if not verificar_acceso_oficina(material_existente.get('oficina_id')):
            flash('No tiene permisos para editar este material', 'danger')
            return redirect('/materiales')

        # Obtener datos del formulario
        nombre = request.form.get('nombre')
        valor_unitario = request.form.get('valor_unitario')
        cantidad = request.form.get('cantidad')
        cantidad_minima = request.form.get('cantidad_minima')
        imagen = request.files.get('imagen')

        # Validaciones
        if not nombre or not valor_unitario or not cantidad or not cantidad_minima:
            flash('Todos los campos son obligatorios', 'danger')
            return render_template('materials/editar.html', material=material_existente)

        try:
            valor_unitario_float = float(valor_unitario)
            cantidad_int = int(cantidad)
            cantidad_minima_int = int(cantidad_minima)
        except ValueError:
            flash('Valor unitario o cantidad no válidos', 'danger')
            return render_template('materials/editar.html', material=material_existente)

        # Procesar imagen si se subió una nueva
        ruta_imagen = material_existente.get('ruta_imagen')
        if imagen and imagen.filename:
            filename = secure_filename(imagen.filename)
            unique_filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
            
            # Asegurar que el directorio de uploads existe
            upload_folder = current_app.config.get('UPLOAD_FOLDER', 'static/uploads')
            os.makedirs(upload_folder, exist_ok=True)
            
            filepath = os.path.join(upload_folder, unique_filename)
            imagen.save(filepath)
            ruta_imagen = unique_filename
            logger.info(sanitizar_log_text(f"✅ Nueva imagen guardada en: {filepath}"))

        # Actualizar el material
        actualizado = MaterialModel.actualizar(
            material_id=material_id,
            nombre=nombre,
            valor_unitario=valor_unitario_float,
            cantidad=cantidad_int,
            oficina_id=material_existente.get('oficina_id'),
            ruta_imagen=ruta_imagen,
            cantidad_minima=cantidad_minima_int
        )

        if actualizado:
            flash('✅ Material actualizado correctamente', 'success')
        else:
            flash('❌ Error al actualizar el material', 'danger')

        return redirect('/materiales')

    except Exception as e:
        logger.error(sanitizar_log_text(f"❌ Error editando material: {e}"))
        
        flash('Error interno al actualizar el material', 'danger')
        return redirect('/materiales')


@materiales_bp.route('/eliminar/<int:material_id>', methods=['POST'])
def eliminar_material(material_id):
    """Eliminar (desactivar) un material"""
    if not _require_login():
        return redirect('/login')

    if not can_access('materiales', 'delete'):
        flash('No tiene permisos para eliminar materiales', 'danger')
        return redirect('/materiales')

    try:
        # Validar que el material existe y el usuario tiene permisos
        material = MaterialModel.obtener_por_id(material_id)
        if not material:
            flash('Material no encontrado', 'danger')
            return redirect('/materiales')

        if not verificar_acceso_oficina(material.get('oficina_id')):
            flash('No tiene permisos para eliminar este material', 'danger')
            return redirect('/materiales')

        # Eliminar (desactivar) el material
        eliminado = MaterialModel.eliminar(material_id)
        if eliminado:
            flash('✅ Material eliminado correctamente', 'success')
        else:
            flash('❌ Error al eliminar el material', 'danger')

        return redirect('/materiales')

    except Exception as e:
        logger.error(sanitizar_log_text(f"❌ Error eliminando material: {e}"))
        flash('Error interno al eliminar el material', 'danger')
        return redirect('/materiales')