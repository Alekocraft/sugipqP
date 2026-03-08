#blueprints/reportes.py

"""
BLUEPRINT DE REPORTES - Versión mejorada con filtros avanzados
Integra la funcionalidad original con la estructura actual del sistema
"""

from flask import Blueprint, render_template, request, redirect, session, flash, url_for, jsonify, send_file
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
from models.solicitudes_model import SolicitudModel
from models.materiales_model import MaterialModel
from models.oficinas_model import OficinaModel
from models.prestamos_model import PrestamosModel
from models.novedades_model import NovedadModel
<<<<<<< HEAD
from models.cobros_pop_model import CobroPOPMensualModel, CobroPOPDiferidoSolicitudModel, _add_months
=======
from models.cobros_pop_model import CobroPOPMensualModel
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
from utils.permissions import can_access, get_office_filter
from utils.filters import filtrar_por_oficina_usuario
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
import logging
from database import get_database_connection
logger = logging.getLogger(__name__)

# Crear blueprint de reportes
reportes_bp = Blueprint('reportes', __name__, url_prefix='/reportes')

# Helpers de autenticación locales
def _require_login():
    return 'usuario_id' in session

def _can_view_reportes() -> bool:
    """Permiso general de visualización del módulo de reportes."""
    return can_access('reportes', 'view')


# Permisos específicos: reporte de cobros POP (tesorería)
def _can_view_cobros_pop() -> bool:
    return can_access('reportes', 'cobros_view')

def _can_cancel_cobros_pop() -> bool:
    return can_access('reportes', 'cobros_cancel')

def _can_export_cobros_pop() -> bool:
    return can_access('reportes', 'cobros_export')


def _parse_periodo(periodo_raw: str) -> str:
    """Normaliza periodo en formato YYYY-MM. Si viene inválido, usa mes actual."""
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
    """Retorna (inicio, fin) datetime para el mes [inicio, fin)."""
    y = int(periodo[:4]); m = int(periodo[5:7])
    inicio = datetime(y, m, 1)
    if m == 12:
        fin = datetime(y+1, 1, 1)
    else:
        fin = datetime(y, m+1, 1)
    return inicio, fin


def _consultar_cobros_pop(periodo: str, oficina_id=None):
    """Consulta detalle de cobros POP aprobados agrupado por oficina y producto."""
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


<<<<<<< HEAD


def _consultar_cobros_pop_solicitudes(periodo: str, oficina_id=None):
    """Detalle por solicitud del cobro POP del periodo."""
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
            params.append(int(oficina_id))
        query = f"""
        SELECT
            sm.SolicitudId,
            o.OficinaId,
            o.NombreOficina,
            MIN(sm.FechaAprobacion) AS FechaAprobacion,
            MAX(CAST(sm.PorcentajeOficina AS DECIMAL(5,2))) AS PorcentajeOficina,
            COUNT(*) AS NumeroRegistros,
            SUM(COALESCE(sm.CantidadEntregada, sm.CantidadSolicitada)) AS CantidadTotal,
            SUM(COALESCE(sm.ValorTotalSolicitado, (m.ValorUnitario * COALESCE(sm.CantidadEntregada, sm.CantidadSolicitada)))) AS ValorTotal,
            SUM(COALESCE(sm.ValorOficina, (m.ValorUnitario * COALESCE(sm.CantidadEntregada, sm.CantidadSolicitada)) * (sm.PorcentajeOficina/100.0))) AS ValorCobroOficina
        FROM dbo.SolicitudesMaterial sm
        INNER JOIN dbo.Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
        INNER JOIN dbo.Materiales m ON sm.MaterialId = m.MaterialId
        INNER JOIN dbo.EstadosSolicitud es ON sm.EstadoId = es.EstadoId
        WHERE LOWER(es.NombreEstado) LIKE '%aprob%'
          AND sm.FechaAprobacion >= ?
          AND sm.FechaAprobacion < ?
          {where_oficina}
        GROUP BY sm.SolicitudId, o.OficinaId, o.NombreOficina
        ORDER BY o.NombreOficina ASC, sm.SolicitudId ASC
        """
        cur.execute(query, params)
        rows = cur.fetchall() or []
        cols = [c[0] for c in cur.description]
        out = []
        for r in rows:
            d = dict(zip(cols, r))
            out.append({
                'solicitud_id': int(d['SolicitudId']),
                'oficina_id': int(d['OficinaId']),
                'oficina_nombre': d['NombreOficina'],
                'fecha_aprobacion': d['FechaAprobacion'],
                'porcentaje_oficina': float(d['PorcentajeOficina'] or 0),
                'numero_registros': int(d['NumeroRegistros'] or 0),
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

=======
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
# Helper para aplicar filtros según permisos
def aplicar_filtro_permisos(datos, campo_oficina='oficina_id'):
    """
    Aplica filtro de oficina según permisos del usuario
    """
    if not datos:
        return []
    
    # Si por configuración puede ver todas las oficinas (p.ej. administrador/tesorería), no filtra
    if get_office_filter() is None:
        return datos
    
    # Para otros roles, filtrar por su oficina
    oficina_usuario = session.get('oficina_id')
    if not oficina_usuario:
        return []
    
    # Filtrar datos por oficina
    datos_filtrados = []
    for item in datos:
        if isinstance(item, dict):
            if item.get(campo_oficina) == oficina_usuario:
                datos_filtrados.append(item)
        else:
            # Si es objeto, verificar atributo
            if hasattr(item, campo_oficina):
                if getattr(item, campo_oficina) == oficina_usuario:
                    datos_filtrados.append(item)
    
    return datos_filtrados

# ============================================================================
# RUTAS DE REPORTES
# ============================================================================

# Página principal de reportes
@reportes_bp.route('/')
def reportes_index():
    """Página principal de reportes"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    return render_template('reportes/index.html')

# ================================
# REPORTE DE SOLICITUDES  
# ===============================

@reportes_bp.route('/solicitudes')
def reporte_solicitudes():
    """Reporte de solicitudes con filtros avanzados - VERSIÓN CORREGIDA"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    # Verificar permisos
    if not _can_view_reportes():
        flash('No tiene permisos para ver reportes de solicitudes', 'warning')
        return redirect('/reportes')
    
    try:
        # Obtener parámetros de filtro
        filtro_estado = request.args.get('estado', 'todos')
        filtro_oficina = request.args.get('oficina', 'todas')
        filtro_material = request.args.get('material', '').strip()
        filtro_solicitante = request.args.get('solicitante', '').strip()
        filtro_fecha_inicio = request.args.get('fecha_inicio', '')
        filtro_fecha_fin = request.args.get('fecha_fin', '')
        
        # Obtener todas las solicitudes con detalle
        solicitudes = SolicitudModel.obtener_todas_con_detalle() or []
        
        # Aplicar filtro según permisos del usuario
        if get_office_filter() == 'own':
            solicitudes = filtrar_por_oficina_usuario(solicitudes, 'oficina_id')
        
        # Aplicar filtros adicionales
        solicitudes_filtradas = []
        for solicitud in solicitudes:
            # Filtro por estado
            if filtro_estado != 'todos':
                estado_solicitud = solicitud.get('estado', '').lower()
                estado_filtro = filtro_estado.lower()
                # CORRECCIÓN: Mejor comparación de estados
                if estado_filtro == 'pendiente' and 'pendiente' not in estado_solicitud:
                    continue
                elif estado_filtro == 'aprobada' and 'aprobada' not in estado_solicitud:
                    continue
                elif estado_filtro == 'rechazada' and 'rechazada' not in estado_solicitud:
                    continue
                elif estado_filtro == 'parcial' and 'parcial' not in estado_solicitud:
                    continue
                elif estado_filtro == 'completada' and 'completada' not in estado_solicitud:
                    continue
                elif estado_filtro == 'devuelta' and 'devuelta' not in estado_solicitud:
                    continue
            
            # Filtro por oficina - CORRECCIÓN: Usar ID en lugar de nombre
            if filtro_oficina != 'todas':
                oficina_solicitud_id = solicitud.get('oficina_id')
                # Intentar convertir ambos a string para comparación
                if str(oficina_solicitud_id) != str(filtro_oficina):
                    continue
            
            # Filtro por material (búsqueda por parte del nombre) - CORRECCIÓN
            if filtro_material:
                material_nombre = str(solicitud.get('material_nombre', '')).lower()
                material_filtro = filtro_material.lower().strip()
                if material_filtro not in material_nombre:
                    continue
            
            # Filtro por solicitante (búsqueda por parte del nombre) - CORRECCIÓN
            if filtro_solicitante:
                solicitante = str(solicitud.get('usuario_solicitante', '')).lower()
                solicitante_filtro = filtro_solicitante.lower().strip()
                if solicitante_filtro not in solicitante:
                    continue
            
            # Filtro por fecha
            if filtro_fecha_inicio:
                try:
                    fecha_solicitud_str = solicitud.get('fecha_solicitud', '')
                    if fecha_solicitud_str:
                        # Manejar diferentes formatos de fecha
                        if isinstance(fecha_solicitud_str, str):
                            fecha_solicitud = datetime.strptime(str(fecha_solicitud_str).split()[0], '%Y-%m-%d').date()
                        else:
                            fecha_solicitud = fecha_solicitud_str.date()
                        
                        fecha_inicio = datetime.strptime(filtro_fecha_inicio, '%Y-%m-%d').date()
                        if fecha_solicitud < fecha_inicio:
                            continue
                except Exception as e:
                    continue
            
            if filtro_fecha_fin:
                try:
                    fecha_solicitud_str = solicitud.get('fecha_solicitud', '')
                    if fecha_solicitud_str:
                        # Manejar diferentes formatos de fecha
                        if isinstance(fecha_solicitud_str, str):
                            fecha_solicitud = datetime.strptime(str(fecha_solicitud_str).split()[0], '%Y-%m-%d').date()
                        else:
                            fecha_solicitud = fecha_solicitud_str.date()
                        
                        fecha_fin = datetime.strptime(filtro_fecha_fin, '%Y-%m-%d').date()
                        if fecha_solicitud > fecha_fin:
                            continue
                except Exception as e:
                    continue
            
            solicitudes_filtradas.append(solicitud)
        
        # Calcular estadísticas
        estados = {
            'pendiente': 0, 
            'aprobada': 0, 
            'rechazada': 0, 
            'parcial': 0, 
            'completada': 0, 
            'devuelta': 0
        }
        
        total_cantidad_solicitada = 0
        total_cantidad_entregada = 0
        
        for solicitud in solicitudes_filtradas:
            estado = solicitud.get('estado', 'pendiente').lower()
            if 'pendiente' in estado:
                estados['pendiente'] += 1
            elif 'aprobada' in estado:
                estados['aprobada'] += 1
            elif 'rechazada' in estado:
                estados['rechazada'] += 1
            elif 'parcial' in estado:
                estados['parcial'] += 1
            elif 'completada' in estado:
                estados['completada'] += 1
            elif 'devuelta' in estado:
                estados['devuelta'] += 1
            
            total_cantidad_solicitada += solicitud.get('cantidad_solicitada', 0)
            total_cantidad_entregada += solicitud.get('cantidad_entregada', 0)
        
        # Obtener listas para filtros
        oficinas = OficinaModel.obtener_todas() or []
        materiales = MaterialModel.obtener_todos() or []
        nombres_materiales = list(set([m.get('nombre', '') for m in materiales]))
        
        # Calcular tasa de aprobación
        total_solicitudes = len(solicitudes_filtradas)
        tasa_aprobacion = 0
        if total_solicitudes > 0:
            aprobadas_totales = estados['aprobada'] + estados['completada'] + estados['parcial']
            tasa_aprobacion = round((aprobadas_totales / total_solicitudes) * 100, 1)
        
        
        return render_template('reportes/solicitudes.html',
                             solicitudes=solicitudes_filtradas,
                             filtro_estado=filtro_estado,
                             filtro_oficina=filtro_oficina,
                             filtro_material=filtro_material,
                             filtro_solicitante=filtro_solicitante,
                             filtro_fecha_inicio=filtro_fecha_inicio,
                             filtro_fecha_fin=filtro_fecha_fin,
                             oficinas=oficinas,
                             nombres_materiales=nombres_materiales,
                             total_solicitudes=total_solicitudes,
                             pendientes=estados['pendiente'],
                             aprobadas=estados['aprobada'],
                             rechazadas=estados['rechazada'],
                             parciales=estados['parcial'],
                             completadas=estados['completada'],
                             devueltas=estados['devuelta'],
                             total_cantidad_solicitada=total_cantidad_solicitada,
                             total_cantidad_entregada=total_cantidad_entregada,
                             tasa_aprobacion=tasa_aprobacion)
                             
    except Exception as e:
        flash('Error al generar el reporte de solicitudes', 'danger')
        return render_template('reportes/solicitudes.html',
                             solicitudes=[],
                             oficinas=[],
                             nombres_materiales=[],
                             total_solicitudes=0,
                             pendientes=0,
                             aprobadas=0,
                             rechazadas=0,
                             parciales=0,
                             completadas=0,
                             devueltas=0)

# ----------------------------------
# EXPORTACIÓN DE SOLICITUDES A EXCEL
# ----------------------------------

@reportes_bp.route('/solicitudes/exportar/excel')
def exportar_solicitudes_excel():
    """Exporta las solicitudes filtradas a Excel - VERSIÓN CORREGIDA"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    # Verificar permisos
    if not _can_view_reportes():
        flash('No tiene permisos para exportar reportes de solicitudes', 'warning')
        return redirect('/reportes')
    
    try:
        # Obtener mismos filtros que en la vista
        filtro_estado = request.args.get('estado', 'todos')
        filtro_oficina = request.args.get('oficina', 'todas')
        filtro_material = request.args.get('material', '').strip()
        filtro_solicitante = request.args.get('solicitante', '').strip()
        filtro_fecha_inicio = request.args.get('fecha_inicio', '')
        filtro_fecha_fin = request.args.get('fecha_fin', '')
        
        # Obtener datos
        solicitudes = SolicitudModel.obtener_todas_con_detalle() or []
        
        # Aplicar filtro según permisos
        if get_office_filter() == 'own':
            solicitudes = filtrar_por_oficina_usuario(solicitudes, 'oficina_id')
        
        # Aplicar filtros adicionales (USANDO LA MISMA LÓGICA CORREGIDA)
        solicitudes_filtradas = []
        for solicitud in solicitudes:
            # Filtro por estado
            if filtro_estado != 'todos':
                estado_solicitud = solicitud.get('estado', '').lower()
                estado_filtro = filtro_estado.lower()
                # CORRECCIÓN: Mejor comparación de estados
                if estado_filtro == 'pendiente' and 'pendiente' not in estado_solicitud:
                    continue
                elif estado_filtro == 'aprobada' and 'aprobada' not in estado_solicitud:
                    continue
                elif estado_filtro == 'rechazada' and 'rechazada' not in estado_solicitud:
                    continue
                elif estado_filtro == 'parcial' and 'parcial' not in estado_solicitud:
                    continue
                elif estado_filtro == 'completada' and 'completada' not in estado_solicitud:
                    continue
                elif estado_filtro == 'devuelta' and 'devuelta' not in estado_solicitud:
                    continue
            
            # Filtro por oficina - CORRECCIÓN
            if filtro_oficina != 'todas':
                oficina_solicitud_id = solicitud.get('oficina_id')
                if str(oficina_solicitud_id) != str(filtro_oficina):
                    continue
            
            # Filtro por material - CORRECCIÓN
            if filtro_material:
                material_nombre = str(solicitud.get('material_nombre', '')).lower()
                material_filtro = filtro_material.lower().strip()
                if material_filtro not in material_nombre:
                    continue
            
            # Filtro por solicitante - CORRECCIÓN
            if filtro_solicitante:
                solicitante = str(solicitud.get('usuario_solicitante', '')).lower()
                solicitante_filtro = filtro_solicitante.lower().strip()
                if solicitante_filtro not in solicitante:
                    continue
            
            # Filtro por fecha
            if filtro_fecha_inicio:
                try:
                    fecha_solicitud_str = solicitud.get('fecha_solicitud', '')
                    if fecha_solicitud_str:
                        if isinstance(fecha_solicitud_str, str):
                            fecha_solicitud = datetime.strptime(str(fecha_solicitud_str).split()[0], '%Y-%m-%d').date()
                        else:
                            fecha_solicitud = fecha_solicitud_str.date()
                        
                        fecha_inicio = datetime.strptime(filtro_fecha_inicio, '%Y-%m-%d').date()
                        if fecha_solicitud < fecha_inicio:
                            continue
                except:
                    continue
            
            if filtro_fecha_fin:
                try:
                    fecha_solicitud_str = solicitud.get('fecha_solicitud', '')
                    if fecha_solicitud_str:
                        if isinstance(fecha_solicitud_str, str):
                            fecha_solicitud = datetime.strptime(str(fecha_solicitud_str).split()[0], '%Y-%m-%d').date()
                        else:
                            fecha_solicitud = fecha_solicitud_str.date()
                        
                        fecha_fin = datetime.strptime(filtro_fecha_fin, '%Y-%m-%d').date()
                        if fecha_solicitud > fecha_fin:
                            continue
                except:
                    continue
            
            solicitudes_filtradas.append(solicitud)
        
        # Preparar datos para Excel
        data = []
        for sol in solicitudes_filtradas:
            data.append({
                'ID': sol.get('id', ''),
                'Material': sol.get('material_nombre', ''),
                'Cantidad Solicitada': sol.get('cantidad_solicitada', 0),
                'Cantidad Entregada': sol.get('cantidad_entregada', 0),
                'Solicitante': sol.get('usuario_solicitante', ''),
                'Oficina': sol.get('oficina_nombre', ''),
                'Estado': sol.get('estado', ''),
                'Fecha Solicitud': sol.get('fecha_solicitud', ''),
                'Fecha Aprobación': sol.get('fecha_aprobacion', ''),
                'Observaciones': sol.get('observacion', ''),
                'Usuario Aprobador': sol.get('usuario_aprobador', ''),
                'Stock Actual Material': sol.get('cantidad_disponible', '')
            })

        df = pd.DataFrame(data)
        
        # Crear archivo Excel en memoria
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Solicitudes', index=False)
            
            # Ajustar ancho de columnas
            worksheet = writer.sheets['Solicitudes']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Agregar hoja de resumen
            summary_data = {
                'Resumen': [
                    f'Total Solicitudes: {len(solicitudes_filtradas)}',
                    f'Filtro Estado: {filtro_estado if filtro_estado != "todos" else "Todos"}',
                    f'Filtro Oficina: {filtro_oficina if filtro_oficina != "todas" else "Todas"}',
                    f'Filtro Material: {filtro_material if filtro_material else "Ninguno"}',
                    f'Filtro Solicitante: {filtro_solicitante if filtro_solicitante else "Ninguno"}',
                    f'Fecha Generación: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                ]
            }
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Resumen', index=False)
        
        output.seek(0)

        # Crear nombre de archivo
        fecha_actual = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'reporte_solicitudes_{fecha_actual}.xlsx'

        return send_file(output,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         as_attachment=True,
                         download_name=filename)
                         
    except Exception as e:
        flash('Error al exportar el reporte de solicitudes a Excel', 'danger')
        return redirect('/reportes/solicitudes')

# ============================================================================
# OTROS REPORTES
# ============================================================================

@reportes_bp.route('/materiales')
def reporte_materiales():
    """Reporte de materiales"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para ver reportes de materiales', 'warning')
        return redirect('/reportes')
    
    try:
        materiales = MaterialModel.obtener_todos() or []
        
        # Aplicar filtro según permisos
        if get_office_filter() == 'own':
            materiales = filtrar_por_oficina_usuario(materiales, 'oficina_id')
        
        # Calcular estadísticas
        valor_total_inventario = sum(m.get('valor_total', 0) or 0 for m in materiales)
        
        # Obtener estadísticas de solicitudes
        stats_dict = {}
        for mat in materiales:
            stats = SolicitudModel.obtener_estadisticas_por_material(mat['id'])
            stats_dict[mat['id']] = stats
        
        # Calcular totales
        total_solicitudes = sum(stats[0] for stats in stats_dict.values() if stats)
        total_entregado = sum(stats[3] for stats in stats_dict.values() if stats)
        
        return render_template('reportes/materiales.html',
                             materiales=materiales,
                             valor_total_inventario=valor_total_inventario,
                             stats_dict=stats_dict,
                             total_solicitudes=total_solicitudes,
                             total_entregado=total_entregado)
    except Exception as e:
        flash('Error al generar el reporte de materiales', 'danger')
        return render_template('reportes/materiales.html',
                             materiales=[],
                             valor_total_inventario=0,
                             stats_dict={},
                             total_solicitudes=0,
                             total_entregado=0)

@reportes_bp.route('/inventario')
def reporte_inventario():
    """Reporte de inventario corporativo"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para ver reportes de inventario', 'warning')
        return redirect('/reportes')
    
    try:
        materiales = MaterialModel.obtener_todos() or []
        
        # Aplicar filtro según permisos
        if get_office_filter() == 'own':
            materiales = filtrar_por_oficina_usuario(materiales, 'oficina_id')
        
        # Calcular estadísticas
        valor_total = 0
        for material in materiales:
            valor_total_material = material.get('valor_total', 0)
            try:
                valor_total += float(valor_total_material)
            except:
                valor_total += 0
        
        valor_promedio = valor_total / len(materiales) if materiales else 0
        
        # Agrupar por oficina
        ubicaciones_dict = {}
        for material in materiales:
            ubicacion = material.get('oficina_nombre', 'Sin oficina')
            if ubicacion not in ubicaciones_dict:
                ubicaciones_dict[ubicacion] = {'cantidad': 0, 'nombre': ubicacion}
            ubicaciones_dict[ubicacion]['cantidad'] += 1
        
        categorias = []
        ubicaciones = list(ubicaciones_dict.values())
        
        # Formatear productos
        productos = []
        for material in materiales:
            productos.append({
                'id': material.get('id'),
                'nombre': material.get('nombre', 'Sin nombre'),
                'valor_unitario': float(material.get('valor_unitario', 0)),
                'cantidad': int(material.get('cantidad', 0)),
                'valor_total': float(material.get('valor_total', 0)),
                'oficina_nombre': material.get('oficina_nombre', 'Sin oficina'),
                'fecha_creacion': material.get('fecha_creacion')
            })
        
        return render_template('reportes/inventario.html',
                            productos=productos,
                            total_productos=len(materiales),
                            valor_total_inventario=valor_total,
                            valor_promedio=valor_promedio,
                            categorias=categorias,
                            ubicaciones=ubicaciones)
        
    except Exception as e:
        flash('Error al generar el reporte de inventario', 'danger')
        return render_template('reportes/inventario.html',
                            productos=[],
                            total_productos=0,
                            valor_total_inventario=0,
                            valor_promedio=0,
                            categorias=[],
                            ubicaciones=[])

@reportes_bp.route('/novedades')
def reporte_novedades():
    """Reporte de novedades"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para ver reportes de novedades', 'warning')
        return redirect('/reportes')
    
    try:
        # Obtener todas las novedades con detalle completo
        from database import get_database_connection
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                n.NovedadId as id,
                n.TipoNovedad as tipo,
                n.Descripcion as descripcion,
                n.FechaRegistro as fecha_reporte,
                n.EstadoNovedad as estado,
                'media' as prioridad,
                n.ObservacionesResolucion as comentarios,
                s.OficinaSolicitanteId as oficina_id,
                o.NombreOficina as oficina_nombre,
                u.NombreUsuario as reportante_nombre,
                n.UsuarioRegistra as usuario_registra
            FROM NovedadesSolicitudes n
            LEFT JOIN SolicitudesMaterial s ON n.SolicitudId = s.SolicitudId
            LEFT JOIN Oficinas o ON s.OficinaSolicitanteId = o.OficinaId
            LEFT JOIN Usuarios u ON n.UsuarioRegistra = u.CorreoElectronico
            ORDER BY n.FechaRegistro DESC
        """)
        
        columns = [column[0] for column in cursor.description]
        novedades_raw = cursor.fetchall()
        novedades = []
        
        for row in novedades_raw:
            novedad = dict(zip(columns, row))
            novedades.append(novedad)
        
        conn.close()
        
        # Aplicar filtro según permisos
        if get_office_filter() == 'own':
            oficina_usuario = session.get('oficina_id')
            novedades = [n for n in novedades if n.get('oficina_id') == oficina_usuario]
        
        # Calcular estadísticas
        total_novedades = len(novedades)
        
        # Contar por estado
        # Contar por estado (estados reales de la BD: registrada, resuelta, aceptada, rechazada)
        estados = {
            'registrada': 0,    # Pendiente
            'en_proceso': 0,    # En proceso (si existe)
            'resuelta': 0,      # Resuelta
            'aceptada': 0,      # Aceptada
            'rechazada': 0      # Rechazada
        }
        for novedad in novedades:
            estado = novedad.get('estado', '').lower().strip()
            if estado in estados:
                estados[estado] += 1
            else:
                # Si el estado no está en nuestro diccionario, intentar mapearlo
                if 'registrada' in estado or 'pendiente' in estado:
                    estados['registrada'] += 1
                elif 'proceso' in estado:
                    estados['en_proceso'] += 1
                elif 'resuelta' in estado or 'resuelto' in estado:
                    estados['resuelta'] += 1
                elif 'aceptada' in estado or 'aceptado' in estado:
                    estados['aceptada'] += 1
                elif 'rechazada' in estado or 'rechazado' in estado:
                    estados['rechazada'] += 1
        # Contar por prioridad
        prioridades = {'alta': 0, 'media': 0, 'baja': 0}
        for novedad in novedades:
            prioridad = novedad.get('prioridad', 'media')
            if prioridad == 'alta':
                prioridades['alta'] += 1
            elif prioridad == 'media':
                prioridades['media'] += 1
            elif prioridad == 'baja':
                prioridades['baja'] += 1
        
        # Tipos de novedad únicos
        tipos_novedad = list(set([n.get('tipo', 'General') for n in novedades if n.get('tipo')]))
        
        # Reportantes únicos
        reportantes = list(set([n.get('reportante_nombre', 'Desconocido') for n in novedades if n.get('reportante_nombre')]))
        
        # Novedades recientes
        novedades_recientes = sorted(novedades, 
                                   key=lambda x: x.get('fecha_reporte', datetime.now()), 
                                   reverse=True)[:6]
        
        return render_template('reportes/novedades.html',
                             novedades=novedades,
                             total_novedades=total_novedades,
                             registradas=estados['registrada'],
                             pendientes=estados['registrada'],  # Alias para compatibilidad
                             en_proceso=estados['en_proceso'],
                             resueltas=estados['resuelta'],
                             aceptadas=estados['aceptada'],
                             rechazadas=estados['rechazada'],
                             prioridad_alta=prioridades['alta'],
                             prioridad_media=prioridades['media'],
                             prioridad_baja=prioridades['baja'],
                             tipos_novedad=tipos_novedad,
                             reportantes=reportantes,
                             novedades_recientes=novedades_recientes)
    except Exception as e:
        flash('Error al generar el reporte de novedades: Error interno', 'danger')
        logger.error("Error interno (%s)", type(e).__name__)

        return render_template('reportes/novedades.html',
                             novedades=[],
                             total_novedades=0,
                             registradas=0,
                             pendientes=0,  # Alias para compatibilidad
                             en_proceso=0,
                             resueltas=0,
                             aceptadas=0,
                             rechazadas=0,
                             prioridad_alta=0,
                             prioridad_media=0,
                             prioridad_baja=0,
                             tipos_novedad=[],
                             reportantes=[],
                             novedades_recientes=[])

@reportes_bp.route('/oficinas')
def reporte_oficinas():
    """Reporte de oficinas con inventario corporativo - VERSIÓN CORREGIDA COMPLETA"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para ver el reporte de oficinas', 'warning')
        return redirect('/reportes')
    
    try:
        from database import get_database_connection
        conn = get_database_connection()
        cursor = conn.cursor()
        
        
        # Aplicar filtro de oficina según permisos (all vs own)
        office_filter = get_office_filter()
        oficina_usuario_id = session.get('oficina_id')

        if office_filter == 'own':
            if not oficina_usuario_id:
                flash('No se pudo determinar la oficina del usuario para filtrar el reporte.', 'warning')
                cursor.execute("""
                    SELECT 
                        OficinaId,
                        NombreOficina,
                        Ubicacion
                    FROM Oficinas
                    WHERE 1 = 0
                """)
            else:
                cursor.execute("""
                    SELECT 
                        OficinaId,
                        NombreOficina,
                        Ubicacion
                    FROM Oficinas
                    WHERE Activo = 1 AND OficinaId = ?
                    ORDER BY NombreOficina
                """, (oficina_usuario_id,))
        else:
            cursor.execute("""
                SELECT 
                    OficinaId,
                    NombreOficina,
                    Ubicacion
                FROM Oficinas
                WHERE Activo = 1
                ORDER BY NombreOficina
            """)
        
        oficinas_data = []
        for row in cursor.fetchall():
            oficina = {
                'id': row[0],
                'nombre': row[1],
                'ubicacion': row[2] or 'No especificada',
                'region': 'Sin región'
            }
            
            # Obtener productos corporativos asignados a esta oficina
            cursor_prod = conn.cursor()
            cursor_prod.execute("""
                SELECT 
                    p.ProductoId,
                    p.CodigoUnico,
                    p.NombreProducto,
                    p.Descripcion,
                    p.CantidadDisponible,
                    p.ValorUnitario,
                    p.CantidadMinima,
                    CAST(NULL AS NVARCHAR(100)) as NombreCategoria
                FROM ProductosCorporativos p
                INNER JOIN Asignaciones a ON p.ProductoId = a.ProductoId
                WHERE a.OficinaId = ? AND p.Activo = 1 AND a.Activo = 1
                GROUP BY p.ProductoId, p.CodigoUnico, p.NombreProducto, p.Descripcion,
                         p.CantidadDisponible, p.ValorUnitario, p.CantidadMinima
            """, (oficina['id'],))
            
            materiales = []
            for prod_row in cursor_prod.fetchall():
                material = {
                    'id': prod_row[0],
                    'codigo': prod_row[1],
                    'nombre': prod_row[2],
                    'descripcion': prod_row[3],
                    'cantidad': prod_row[4] or 0,
                    'valor_unitario': prod_row[5] or 0,
                    'cantidad_minima': prod_row[6] or 0,
                    'categoria': prod_row[7] or 'Corporativo',
                    'estado': 'Activo',
                    'unidad': 'Unidad',
                    'valor_total': (prod_row[4] or 0) * (prod_row[5] or 0)
                }
                materiales.append(material)
            cursor_prod.close()
            
            # Obtener movimientos/asignaciones CON AsignacionId (CRÍTICO para certificados)
            cursor_mov = conn.cursor()
            cursor_mov.execute("""
                SELECT 
                    a.FechaAsignacion,
                    a.Estado,
                    p.NombreProducto,
                    1 as Cantidad,
                    a.UsuarioAsignador,
                    a.Observaciones,
                    a.AsignacionId
                FROM Asignaciones a
                INNER JOIN ProductosCorporativos p ON a.ProductoId = p.ProductoId
                WHERE a.OficinaId = ? AND a.Activo = 1
                ORDER BY a.FechaAsignacion DESC
            """, (oficina['id'],))
            
            movimientos = []
            for mov_row in cursor_mov.fetchall():
                estado_upper = (mov_row[1] or '').upper().strip()
                
                if estado_upper == 'CONFIRMADO':
                    accion_display = 'CONFIRMACION'
                elif estado_upper == 'ASIGNADO':
                    accion_display = 'ASIGNAR'
                elif estado_upper == 'DEVUELTO':
                    accion_display = 'DEVOLUCION'
                else:
                    accion_display = 'Asignación a Usuario'
                
                movimiento = {
                    'fecha': mov_row[0],
                    'accion': accion_display,
                    'material': mov_row[2],
                    'cantidad': mov_row[3],
                    'usuario': mov_row[4] or 'Sistema',
                    'observaciones': mov_row[5] or '',
                    'asignacion_id': mov_row[6]  # ✅ CRÍTICO para botones de certificado
                }
                movimientos.append(movimiento)
            cursor_mov.close()
            
            # Calcular totales
            oficina['materiales'] = materiales
            oficina['movimientos'] = movimientos
            oficina['total_materiales'] = len(materiales)
            oficina['total_movimientos'] = len(movimientos)
            oficina['valor_total'] = sum([m['valor_total'] for m in materiales])
            
            # Contar solicitudes (opcional, puede fallar si no existe la tabla)
            try:
                cursor_sol = conn.cursor()
                cursor_sol.execute("""
                    SELECT COUNT(*) 
                    FROM Solicitudes 
                    WHERE OficinaId = ? AND Activo = 1
                """, (oficina['id'],))
                oficina['total_solicitudes'] = cursor_sol.fetchone()[0] or 0
                cursor_sol.close()
            except:
                oficina['total_solicitudes'] = 0
            
            oficinas_data.append(oficina)
        
        conn.close()
        
        # Totales generales
        total_oficinas = len(oficinas_data)
        total_materiales = sum([o['total_materiales'] for o in oficinas_data])
        total_solicitudes = sum([o['total_solicitudes'] for o in oficinas_data])
        total_movimientos = sum([o['total_movimientos'] for o in oficinas_data])
        valor_total = sum([o['valor_total'] for o in oficinas_data])
        
        return render_template('reportes/oficinas.html',
                             oficinas=oficinas_data,
                             total_oficinas=total_oficinas,
                             total_materiales=total_materiales,
                             total_solicitudes=total_solicitudes,
                             total_movimientos=total_movimientos,
                             valor_total=valor_total)
    
    except Exception as e:
        flash('Error al generar el reporte de oficinas: Error interno', 'danger')
        logger.error("Error interno (%s)", type(e).__name__)

        return redirect('/reportes')

@reportes_bp.route('/prestamos')
def reporte_prestamos():
    """Reporte de préstamos"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not (_can_view_reportes() or can_access('prestamos', 'view') or can_access('prestamos', 'view_own')):
        flash('No tiene permisos para ver reportes de préstamos', 'warning')
        return redirect('/reportes')
    
    try:
        from database import get_database_connection
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        filtro_estado = request.args.get('estado', 'todos')
        filtro_oficina = request.args.get('oficina', 'todas')
        filtro_material = request.args.get('material', '').strip()
        filtro_solicitante = request.args.get('solicitante', '').strip()
        filtro_fecha_inicio = request.args.get('fecha_inicio', '')
        filtro_fecha_fin = request.args.get('fecha_fin', '')
        
        query = """
            SELECT 
                pe.PrestamoId,
                pe.ElementoId,
                ep.NombreElemento as Material,
                pe.UsuarioSolicitanteId,
                u.NombreUsuario as SolicitanteNombre,
                pe.OficinaId,
                o.NombreOficina as OficinaNombre,
                pe.CantidadPrestada as Cantidad,
                pe.FechaPrestamo as Fecha,
                pe.FechaDevolucionPrevista as FechaPrevista,
                pe.FechaDevolucionReal,
                pe.Estado,
                pe.Evento,
                pe.Observaciones,
                pe.UsuarioPrestador,
                ep.ValorUnitario,
                (pe.CantidadPrestada * ep.ValorUnitario) as Subtotal,
                CASE 
                    WHEN pe.Estado = 'PRESTADO' AND pe.FechaDevolucionPrevista < CAST(GETDATE() AS DATE)
                    THEN 1
                    ELSE 0
                END as EstaAtrasado
            FROM PrestamosElementos pe
            INNER JOIN ElementosPublicitarios ep ON pe.ElementoId = ep.ElementoId
            INNER JOIN Usuarios u ON pe.UsuarioSolicitanteId = u.UsuarioId
            INNER JOIN Oficinas o ON pe.OficinaId = o.OficinaId
            WHERE pe.Activo = 1
        """
        
        params = []
        
        if filtro_estado and filtro_estado != 'todos' and filtro_estado != 'ATRASADO':
            query += " AND pe.Estado = ?"
            params.append(filtro_estado)
        
        rol_usuario = session.get('rol', '').lower()
        oficina_id_usuario = session.get('oficina_id')
        
        if rol_usuario in ['administrador', 'lider_inventario', 'aprobador', 'tesoreria']:
            if filtro_oficina and filtro_oficina != 'todas':
                query += " AND pe.OficinaId = ?"
                params.append(filtro_oficina)
        else:
            query += " AND pe.OficinaId = ?"
            params.append(oficina_id_usuario)
        
        if filtro_material:
            query += " AND ep.NombreElemento LIKE ?"
            params.append(f'%{filtro_material}%')
        
        if filtro_solicitante:
            query += " AND u.NombreUsuario LIKE ?"
            params.append(f'%{filtro_solicitante}%')
        
        if filtro_fecha_inicio:
            query += " AND CAST(pe.FechaPrestamo AS DATE) >= ?"
            params.append(filtro_fecha_inicio)
        
        if filtro_fecha_fin:
            query += " AND CAST(pe.FechaPrestamo AS DATE) <= ?"
            params.append(filtro_fecha_fin)
        
        query += " ORDER BY pe.FechaPrestamo DESC"
        
        cursor.execute(query, params)
        
        prestamos_raw = []
        for row in cursor.fetchall():
            prestamo = {
                'id': row[0],
                'elemento_id': row[1],
                'material': row[2],
                'material_nombre': row[2],  
                'usuario_solicitante_id': row[3],
                'solicitante_nombre': row[4],
                'oficina_id': row[5],
                'oficina_nombre': row[6],
                'cantidad': row[7],
                'valor_unitario': float(row[15] or 0),
                'subtotal': float(row[16] or 0),
                'fecha': row[8],
                'fecha_prestamo': row[8],  
                'fecha_prevista': row[9],
                'fecha_devolucion_prevista': row[9],  
                'fecha_devolucion_real': row[10],
                'estado': row[11],
                'evento': row[12],
                'observaciones': row[13],
                'usuario_prestador': row[14],
                'esta_atrasado': bool(row[17])
            }
            prestamos_raw.append(prestamo)
        
        if filtro_estado == 'ATRASADO':
            prestamos = [p for p in prestamos_raw if p['esta_atrasado']]
        else:
            prestamos = prestamos_raw
        
        oficinas = []
        if rol_usuario in ['administrador', 'lider_inventario', 'aprobador', 'tesoreria']:
            cursor.execute("SELECT OficinaId, NombreOficina FROM Oficinas WHERE Activo = 1 ORDER BY NombreOficina")
            oficinas = [{'id': row[0], 'nombre': row[1]} for row in cursor.fetchall()]
        
        conn.close()
        
        total_prestamos = len(prestamos)
        
        prestamos_activos = len([p for p in prestamos if p['estado'] == 'PRESTADO'])
        devueltos = len([p for p in prestamos if p['estado'] == 'DEVUELTO'])
        rechazados = len([p for p in prestamos if p['estado'] == 'RECHAZADO'])
        aprobados_parcial = len([p for p in prestamos if p['estado'] == 'APROBADO_PARCIAL'])
        
        atrasados = len([p for p in prestamos if p['esta_atrasado']])
        
        cantidad_total_prestada = sum(p['cantidad'] for p in prestamos)
        cantidad_devuelta = sum(p['cantidad'] for p in prestamos if p['estado'] == 'DEVUELTO')
        
        fecha_hoy = datetime.now().date()
        
        return render_template('reportes/prestamos.html',
                             prestamos=prestamos,
                             total_prestamos=total_prestamos,
                             prestamos_activos=prestamos_activos,
                             devueltos=devueltos,
                             atrasados=atrasados,
                             rechazados=rechazados,
                             aprobados_parcial=aprobados_parcial,
                             cantidad_total_prestada=cantidad_total_prestada,
                             cantidad_devuelta=cantidad_devuelta,
                             filtro_estado=filtro_estado,
                             filtro_oficina=filtro_oficina,
                             filtro_material=filtro_material,
                             filtro_solicitante=filtro_solicitante,
                             filtro_fecha_inicio=filtro_fecha_inicio,
                             filtro_fecha_fin=filtro_fecha_fin,
                             oficinas=oficinas,
                             hoy=fecha_hoy)
                             
    except Exception:
        flash('Error al generar el reporte de préstamos', 'danger')
        
        fecha_hoy = datetime.now().date()
        
        return render_template('reportes/prestamos.html',
                             prestamos=[],
                             total_prestamos=0,
                             prestamos_activos=0,
                             devueltos=0,
                             atrasados=0,
                             rechazados=0,
                             aprobados_parcial=0,
                             cantidad_total_prestada=0,
                             cantidad_devuelta=0,
                             filtro_estado='todos',
                             filtro_oficina='todas',
                             filtro_material='',
                             filtro_solicitante='',
                             filtro_fecha_inicio='',
                             filtro_fecha_fin='',
                             oficinas=[],
                             hoy=fecha_hoy)


@reportes_bp.route('/materiales/exportar/excel')
def exportar_materiales_excel():
    """Exporta materiales a Excel"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    try:
        materiales = MaterialModel.obtener_todos() or []
        
        # Aplicar filtro según permisos
        if get_office_filter() == 'own':
            materiales = filtrar_por_oficina_usuario(materiales, 'oficina_id')

        data = []
        for mat in materiales:
            data.append({
                'ID': mat.get('id', ''),
                'Nombre': mat.get('nombre', ''),
                'Valor Unitario': mat.get('valor_unitario', 0),
                'Stock Actual': mat.get('cantidad', 0),
                'Stock Mínimo': mat.get('stock_minimo', 0) if mat.get('stock_minimo') else 0,
                'Valor Total': mat.get('valor_total', 0),
                'Oficina': mat.get('oficina_nombre', ''),
                'Creado por': mat.get('usuario_creador', ''),
                'Fecha Creación': mat.get('fecha_creacion', '')
            })

        df = pd.DataFrame(data)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Materiales', index=False)
        
        output.seek(0)
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        filename = f'reporte_materiales_{fecha_actual}.xlsx'

        return send_file(output,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         as_attachment=True,
                         download_name=filename)
    except Exception as e:
        flash('Error al exportar el reporte de materiales a Excel', 'danger')
        return redirect('/reportes')

# ============================================================================
# FUNCIONES DE EXPORTACIÓN A PDF - VERSIONES MEJORADAS
# ============================================================================

@reportes_bp.route('/exportar/inventario-corporativo/pdf')
def exportar_inventario_corporativo_pdf():
    """Exporta el inventario corporativo a PDF"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    # Verificar permisos
    if not _can_view_reportes():
        flash('No tiene permisos para exportar inventario corporativo', 'warning')
        return redirect('/reportes')
    
    try:
        # Intentar importar reportlab
        try:
            from reportlab.lib.pagesizes import letter, landscape
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib import colors
            from reportlab.lib.units import inch, cm
        except ImportError:
            flash('La librería ReportLab no está instalada. Instálela con: pip install reportlab', 'danger')
            return redirect('/reportes')
        
        from database import get_database_connection
        import io
        
        conn = get_database_connection()
        
        # CONSULTA CORREGIDA - USANDO LA TABLA MATERIALES REAL
        query = """
        SELECT 
            o.NombreOficina as Oficina,
            m.NombreElemento as Material,
            m.CantidadDisponible as Cantidad,
            m.ValorUnitario,
            (m.CantidadDisponible * m.ValorUnitario) as ValorTotal,
            m.CantidadMinima as StockMinimo,
            CASE WHEN m.Activo = 1 THEN 'Activo' ELSE 'Inactivo' END as Estado,
            m.UsuarioCreador as Responsable,
            FORMAT(m.FechaCreacion, 'dd/MM/yyyy') as Fecha_Creacion
        FROM Materiales m
        INNER JOIN Oficinas o ON m.OficinaCreadoraId = o.OficinaId
        WHERE m.Activo = 1
        ORDER BY o.NombreOficina, m.NombreElemento
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        resultados = cursor.fetchall()
        
        # Obtener nombres de columnas
        column_names = [column[0] for column in cursor.description]
        conn.close()
        
        # Verificar si hay datos
        if not resultados:
            flash('No hay datos de inventario corporativo para exportar', 'warning')
            return redirect('/reportes')
        
        # Crear PDF en memoria
        buffer = io.BytesIO()
        
        # Usar landscape y ajustar márgenes para mejor uso del espacio
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(letter),
            topMargin=0.5*inch,
            bottomMargin=0.5*inch,
            leftMargin=0.5*inch,
            rightMargin=0.5*inch
        )
        
        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=14,
            alignment=1,  # Centrado
            spaceAfter=10
        )
        
        subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Normal'],
            fontSize=9,
            alignment=1,
            spaceAfter=15
        )
        
        # Preparar datos para la tabla
        data = []
        
        # Título
        data.append([Paragraph('<b>REPORTE DE INVENTARIO CORPORATIVO</b>', title_style)])
        data.append([Paragraph(f'Fecha de generación: {datetime.now().strftime("%d/%m/%Y %H:%M")}', subtitle_style)])
        data.append([''])  # Espacio
        
        # Encabezados de tabla
        data.append(column_names)
        
        # Filas de datos
        total_valor = 0
        for row in resultados:
            # Formatear valores monetarios
            formatted_row = []
            for i, value in enumerate(row):
                if column_names[i] in ['ValorUnitario', 'ValorTotal'] and value is not None:
                    try:
                        val = float(value)
                        if column_names[i] == 'ValorTotal':
                            total_valor += val
                        formatted_row.append(f"${val:,.2f}")
                    except (ValueError, TypeError):
                        formatted_row.append("$0.00")
                else:
                    formatted_row.append(str(value) if value is not None else 'N/A')
            data.append(formatted_row)
        
        total_materiales = len(resultados)
        
        # Agregar fila de totales
        data.append([''])  # Espacio
        data.append(['<b>RESUMEN</b>', '', '', '', '', '', '', '', ''])
        data.append(['Total Materiales:', str(total_materiales), '', '', '', '', '', '', ''])
        data.append(['Valor Total Inventario:', f"${total_valor:,.2f}", '', '', '', '', '', '', ''])
        
        # Crear tabla
        table = Table(data, repeatRows=4)  # Repetir encabezados en cada página
        
        # Estilo de la tabla
        table_style = TableStyle([
            ('SPAN', (0, 0), (8, 0)),  # Título
            ('SPAN', (0, 1), (8, 1)),  # Fecha
            
            # Encabezados
            ('BACKGROUND', (0, 3), (8, 3), colors.HexColor('#4F81BD')),
            ('TEXTCOLOR', (0, 3), (8, 3), colors.whitesmoke),
            ('ALIGN', (0, 3), (8, 3), 'CENTER'),
            ('FONTNAME', (0, 3), (8, 3), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 3), (8, 3), 8),
            ('BOTTOMPADDING', (0, 3), (8, 3), 6),
            
            # Datos
            ('GRID', (0, 3), (8, -5), 0.5, colors.grey),
            ('ALIGN', (2, 4), (4, -5), 'RIGHT'),   
            ('ALIGN', (0, 4), (1, -5), 'LEFT'),    
            ('FONTSIZE', (0, 4), (8, -5), 7),
            ('VALIGN', (0, 0), (8, -1), 'MIDDLE'),
            
            # Resumen
            ('SPAN', (0, -3), (0, -3)),
            ('FONTNAME', (0, -3), (1, -1), 'Helvetica-Bold'),
            ('ALIGN', (0, -2), (0, -1), 'LEFT'),
            ('ALIGN', (1, -2), (1, -1), 'RIGHT'),
            ('BACKGROUND', (0, -3), (1, -1), colors.HexColor('#E8F5E8')),
        ])
        
        # Alternar colores de filas
        for i in range(4, 4 + len(resultados), 2):
            if i < len(data) - 4:
                table_style.add('BACKGROUND', (0, i), (8, i), colors.HexColor('#F5F5F5'))
        
        table.setStyle(table_style)
        
        # Ajustar ancho de columnas
        col_widths = [
            2.5*cm,  # Oficina
            3.5*cm,  # Material
            1.5*cm,  # Cantidad
            2.0*cm,  # Valor Unitario
            2.0*cm,  # Valor Total
            1.5*cm,  # Stock Mínimo
            1.5*cm,  # Estado
            2.0*cm,  # Responsable
            2.0*cm   # Fecha
        ]
        table._argW = col_widths
        
        # Generar PDF
        doc.build([table])
        
        buffer.seek(0)
        
        # Crear nombre de archivo
        fecha_actual = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'reporte_inventario_corporativo_{fecha_actual}.pdf'
        
        
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        flash('Error al generar el PDF del inventario: Error interno', 'danger')
        return redirect('/reportes')

@reportes_bp.route('/prestamos/exportar/pdf')
def exportar_prestamos_pdf():
    """Exporta el reporte de préstamos a PDF"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not (_can_view_reportes() or can_access('prestamos', 'view') or can_access('prestamos', 'view_own')):
        flash('No tiene permisos para exportar reportes de préstamos', 'warning')
        return redirect('/reportes')
    
    try:
        # Verificar si reportlab está instalado
        try:
            from reportlab.lib.pagesizes import letter, landscape
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib import colors
            from reportlab.lib.units import inch, cm
        except ImportError:
            flash('La librería ReportLab no está instalada. Instálela con: pip install reportlab', 'danger')
            return redirect('/reportes')
        
        from database import get_database_connection
        import io
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Consulta para préstamos
        query = """
        SELECT 
            pe.PrestamoId as ID,
            ep.NombreElemento as Material,
            u.NombreUsuario as Solicitante,
            o.NombreOficina as Oficina,
            pe.CantidadPrestada as Cantidad,
            FORMAT(pe.FechaPrestamo, 'dd/MM/yyyy') as Fecha_Prestamo,
            FORMAT(pe.FechaDevolucionPrevista, 'dd/MM/yyyy') as Devolucion_Prevista,
            pe.Estado,
            pe.Evento
        FROM PrestamosElementos pe
        INNER JOIN ElementosPublicitarios ep ON pe.ElementoId = ep.ElementoId
        INNER JOIN Usuarios u ON pe.UsuarioSolicitanteId = u.UsuarioId
        INNER JOIN Oficinas o ON pe.OficinaId = o.OficinaId
        WHERE pe.Activo = 1
        ORDER BY pe.FechaPrestamo DESC
        """
        
        cursor.execute(query)
        resultados = cursor.fetchall()
        
        # Obtener nombres de columnas
        column_names = [column[0] for column in cursor.description]
        conn.close()
        
        # Verificar si hay datos
        if not resultados:
            flash('No hay datos de préstamos para exportar', 'warning')
            return redirect('/reportes')
        
        # Crear PDF en memoria
        buffer = io.BytesIO()
        
        # Usar landscape para mejor uso del espacio
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(letter),
            topMargin=0.5*inch,
            bottomMargin=0.5*inch,
            leftMargin=0.5*inch,
            rightMargin=0.5*inch
        )
        
        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=14,
            alignment=1,
            spaceAfter=15
        )
        
        # Preparar datos para la tabla
        data = []
        
        # Título
        data.append([Paragraph('<b>REPORTE DE PRÉSTAMOS</b>', title_style)])
        data.append([Paragraph(f'Fecha de generación: {datetime.now().strftime("%d/%m/%Y %H:%M")}', 
                               ParagraphStyle('Date', parent=styles['Normal'], fontSize=9, alignment=1))])
        data.append([''])  # Espacio
        
        # Encabezados de tabla
        data.append(column_names)
        
        # Filas de datos
        for row in resultados:
            data.append([str(value) if value is not None else '' for value in row])
        
        # Estadísticas
        total_prestamos = len(resultados)
        prestamos_activos = len([r for r in resultados if r[7] == 'PRESTADO'])
        
        data.append([''])  # Espacio
        data.append(['<b>ESTADÍSTICAS</b>', '', '', '', '', '', '', '', ''])
        data.append(['Total Préstamos:', str(total_prestamos), '', '', '', '', '', '', ''])
        data.append(['Préstamos Activos:', str(prestamos_activos), '', '', '', '', '', '', ''])
        
        # Crear tabla
        table = Table(data, repeatRows=3)  # Repetir encabezados
        
        # Estilo de la tabla - OPTIMIZADO PARA UNA PÁGINA
        table_style = TableStyle([
            ('SPAN', (0, 0), (-1, 0)),  # Título ocupa todas las columnas
            ('SPAN', (0, 1), (-1, 1)),  # Fecha ocupa todas las columnas
            
            ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor('#4F81BD')),  # Encabezados
            ('TEXTCOLOR', (0, 2), (-1, 2), colors.whitesmoke),
            ('ALIGN', (0, 2), (-1, 2), 'CENTER'),
            ('FONTNAME', (0, 2), (-1, 2), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 2), (-1, 2), 8),
            ('BOTTOMPADDING', (0, 2), (-1, 2), 6),
            
            ('GRID', (0, 2), (-1, -5), 0.5, colors.gray),  # Hasta antes de estadísticas
            ('FONTSIZE', (0, 3), (-1, -5), 7),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            
            # Estadísticas
            ('FONTNAME', (0, -3), (1, -1), 'Helvetica-Bold'),
            ('ALIGN', (0, -3), (1, -1), 'LEFT'),
            ('BACKGROUND', (0, -3), (1, -1), colors.HexColor('#F2F2F2')),
        ])
        
        # Alternar colores de filas
        for i in range(3, len(data)-4, 2):
            table_style.add('BACKGROUND', (0, i), (-1, i), colors.HexColor('#F2F2F2'))
        
        table.setStyle(table_style)
        
        # Ajustar ancho de columnas específicamente para préstamos
        col_widths = [
            1.0*cm,  # ID
            3.0*cm,  # Material
            2.5*cm,  # Solicitante
            2.0*cm,  # Oficina
            1.5*cm,  # Cantidad
            2.0*cm,  # Fecha Préstamo
            2.0*cm,  # Devolución Prevista
            1.5*cm,  # Estado
            2.5*cm   # Evento
        ]
        table._argW = col_widths
        
        # Generar PDF
        doc.build([table])
        
        buffer.seek(0)
        
        # Crear nombre de archivo
        fecha_actual = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'reporte_prestamos_{fecha_actual}.pdf'
        
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        flash('Error al generar el PDF de préstamos: Error interno', 'danger')
        return redirect('/reportes')

@reportes_bp.route('/materiales/exportar/pdf')
def exportar_materiales_pdf():
    """Exporta el reporte de materiales a PDF"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para exportar reportes de materiales', 'warning')
        return redirect('/reportes')
    
    try:
        # Verificar si reportlab está instalado
        try:
            from reportlab.lib.pagesizes import letter, landscape
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib import colors
            from reportlab.lib.units import inch, cm
        except ImportError:
            flash('La librería ReportLab no está instalada. Instálela con: pip install reportlab', 'danger')
            return redirect('/reportes')
        
        from database import get_database_connection
        import io
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Consulta para materiales
        query = """
        SELECT 
            m.NombreElemento as Material,
            o.NombreOficina as Oficina,
            m.CantidadDisponible as Stock,
            m.ValorUnitario,
            (m.CantidadDisponible * m.ValorUnitario) as Valor_Total,
            m.CantidadMinima as Stock_Minimo,
            CASE WHEN m.Activo = 1 THEN 'Activo' ELSE 'Inactivo' END as Estado,
            m.UsuarioCreador as Responsable,
            FORMAT(m.FechaCreacion, 'dd/MM/yyyy') as Fecha_Creacion
        FROM Materiales m
        INNER JOIN Oficinas o ON m.OficinaCreadoraId = o.OficinaId
        WHERE m.Activo = 1
        ORDER BY o.NombreOficina, m.NombreElemento
        """
        
        cursor.execute(query)
        resultados = cursor.fetchall()
        
        # Obtener nombres de columnas
        column_names = [column[0] for column in cursor.description]
        conn.close()
        
        # Verificar si hay datos
        if not resultados:
            flash('No hay datos de materiales para exportar', 'warning')
            return redirect('/reportes')
        
        # Crear PDF en memoria
        buffer = io.BytesIO()
        
        # Usar landscape para mejor uso del espacio
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(letter),
            topMargin=0.5*inch,
            bottomMargin=0.5*inch,
            leftMargin=0.5*inch,
            rightMargin=0.5*inch
        )
        
        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=14,
            alignment=1,
            spaceAfter=15
        )
        
        # Preparar datos para la tabla
        data = []
        
        # Título
        data.append([Paragraph('<b>REPORTE DE MATERIALES</b>', title_style)])
        data.append([Paragraph(f'Fecha de generación: {datetime.now().strftime("%d/%m/%Y %H:%M")}', 
                               ParagraphStyle('Date', parent=styles['Normal'], fontSize=9, alignment=1))])
        data.append([''])  # Espacio
        
        # Encabezados de tabla
        data.append(column_names)
        
        # Filas de datos
        total_valor = 0
        for row in resultados:
            formatted_row = []
            for i, value in enumerate(row):
                if column_names[i] in ['ValorUnitario', 'Valor_Total'] and value is not None:
                    try:
                        val = float(value)
                        if column_names[i] == 'Valor_Total':
                            total_valor += val
                        formatted_row.append(f"${val:,.2f}")
                    except (ValueError, TypeError):
                        formatted_row.append("$0.00")
                else:
                    formatted_row.append(str(value) if value is not None else '')
            data.append(formatted_row)
        
        total_materiales = len(resultados)
        
        data.append([''])  # Espacio
        data.append(['<b>RESUMEN</b>', '', '', '', '', '', '', '', ''])
        data.append(['Total Materiales:', str(total_materiales), '', '', '', '', '', '', ''])
        data.append(['Valor Total Inventario:', f"${total_valor:,.2f}", '', '', '', '', '', '', ''])
        
        # Crear tabla
        table = Table(data, repeatRows=3)  # Repetir encabezados
        
        # Estilo de la tabla
        table_style = TableStyle([
            ('SPAN', (0, 0), (-1, 0)),
            ('SPAN', (0, 1), (-1, 1)),
            
            ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor('#4F81BD')),
            ('TEXTCOLOR', (0, 2), (-1, 2), colors.whitesmoke),
            ('ALIGN', (0, 2), (-1, 2), 'CENTER'),
            ('FONTNAME', (0, 2), (-1, 2), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 2), (-1, 2), 8),
            ('BOTTOMPADDING', (0, 2), (-1, 2), 6),
            
            ('GRID', (0, 2), (-1, -5), 0.5, colors.gray),
            ('FONTSIZE', (0, 3), (-1, -5), 7),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            
            # Resumen
            ('FONTNAME', (0, -3), (1, -1), 'Helvetica-Bold'),
            ('ALIGN', (0, -3), (1, -1), 'LEFT'),
            ('BACKGROUND', (0, -3), (1, -1), colors.HexColor('#F2F2F2')),
        ])
        
        # Alternar colores de filas
        for i in range(3, len(data)-4, 2):
            table_style.add('BACKGROUND', (0, i), (-1, i), colors.HexColor('#F2F2F2'))
        
        table.setStyle(table_style)
        
        # Ajustar ancho de columnas
        col_widths = [
            3.0*cm,  # Material
            2.5*cm,  # Oficina
            1.5*cm,  # Stock
            2.0*cm,  # Valor Unitario
            2.0*cm,  # Valor Total
            1.5*cm,  # Stock Mínimo
            1.5*cm,  # Estado
            2.5*cm,  # Responsable
            2.0*cm   # Fecha
        ]
        table._argW = col_widths
        
        # Generar PDF
        doc.build([table])
        
        buffer.seek(0)
        
        # Crear nombre de archivo
        fecha_actual = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'reporte_materiales_{fecha_actual}.pdf'
        
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        flash('Error al generar el PDF de materiales: Error interno', 'danger')
        return redirect('/reportes')

@reportes_bp.route('/material/<int:material_id>')
def material_detalle(material_id):
    """Detalle de material específico"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para ver detalles de materiales', 'warning')
        return redirect('/reportes')
    
    try:
        from database import get_database_connection
        
        # Obtener el material
        material = MaterialModel.obtener_por_id(material_id)
        if not material:
            flash('Material no encontrado', 'danger')
            return redirect('/reportes/materiales')
        
        # Verificar permisos de oficina
        if get_office_filter() == 'own':
            if material.get('oficina_id') != session.get('oficina_id'):
                flash('No tiene permisos para ver este material', 'danger')
                return redirect('/reportes/materiales')
        
        # Obtener historial de solicitudes para este material
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Asegurar que material_id es un int
        material_id_int = int(material_id)
        
        logger.info(f"🔍 DEBUG: Buscando solicitudes para MaterialId = {material_id_int}")

        logger.info(f"🔍 DEBUG: Material obtenido: {material.get('nombre', 'N/A')}")

        # QUERY MEJORADO con mejor manejo
        query = """
            SELECT 
                s.SolicitudId,
                s.UsuarioSolicitante,
                u.NombreUsuario as UsuarioNombre,
                s.OficinaSolicitanteId,
                o.NombreOficina as OficinaNombre,
                s.CantidadSolicitada,
                s.FechaSolicitud,
                s.FechaAprobacion,
                s.EstadoId,
                e.NombreEstado,
                s.Observacion
            FROM SolicitudesMaterial s
            LEFT JOIN Usuarios u ON s.UsuarioSolicitante = u.CorreoElectronico
            INNER JOIN Oficinas o ON s.OficinaSolicitanteId = o.OficinaId
            INNER JOIN EstadosSolicitud e ON s.EstadoId = e.EstadoId
            WHERE s.MaterialId = ?
            ORDER BY s.FechaSolicitud DESC
        """
        
        try:
            cursor.execute(query, (material_id_int,))
            
            solicitudes = []
            rows = cursor.fetchall()
            logger.info(f"✅ DEBUG: Se encontraron {len(rows)} solicitudes en la BD")

            if len(rows) == 0:
                # Verificar si el MaterialId existe
                cursor.execute("SELECT COUNT(*) FROM Materiales WHERE MaterialId = ?", (material_id_int,))
                count_mat = cursor.fetchone()[0]
                logger.info(f"🔍 DEBUG: ¿MaterialId {material_id_int} existe en Materiales? {count_mat > 0}")

                # Verificar si hay solicitudes para cualquier material
                cursor.execute("SELECT COUNT(*) FROM SolicitudesMaterial WHERE MaterialId = ?", (material_id_int,))
                count_sol = cursor.fetchone()[0]
                logger.info(f"🔍 DEBUG: Solicitudes directas encontradas: {count_sol}")

            for row in rows:
                estado_nombre = row[9] if row[9] else 'Pendiente'
                solicitud = {
                    'id': row[0],
                    'usuario_solicitante': row[1],
                    'usuario_nombre': row[2] or row[1],   
                    'oficina_id': row[3],
                    'oficina_nombre': row[4],
                    'cantidad_solicitada': row[5],
                    'fecha_solicitud': row[6],
                    'fecha_aprobacion': row[7],
                    'estado_id': row[8],
                    'estado': estado_nombre.lower(),
                    'observacion': row[10]
                }
                solicitudes.append(solicitud)
                logger.info(f"  📋 Solicitud {row[0]}: Estado={estado_nombre}, Cantidad={row[5]}")

        except Exception as query_error:
            logger.error("Error interno (%s)", type(e).__name__)

            solicitudes = []
        
        conn.close()
        
        # Calcular estadísticas
        total_solicitudes = len(solicitudes)
        solicitudes_aprobadas = len([s for s in solicitudes if 'aprobada' in s['estado'].lower()])
        
        logger.info(f"DEBUG: Total solicitudes = {total_solicitudes}, Aprobadas = {solicitudes_aprobadas}")

        return render_template('reportes/material_detalle.html',
                             material=material,
                             solicitudes=solicitudes,
                             total_solicitudes=total_solicitudes,
                             solicitudes_aprobadas=solicitudes_aprobadas)
        
    except Exception as e:
        logger.error("Error interno (%s)", type(e).__name__)

        flash('Error al obtener el detalle del material: Error interno', 'danger')
        return redirect('/reportes/materiales')

@reportes_bp.route('/exportar/inventario-corporativo/excel')
def exportar_inventario_corporativo_excel():
    """Exporta TODO el inventario corporativo a Excel"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    # Verificar permisos
    if not _can_view_reportes():
        flash('No tiene permisos para exportar inventario corporativo', 'warning')
        return redirect('/reportes')
    
    try:
        from database import get_database_connection
        
        conn = get_database_connection()
        
        # CONSULTA CORREGIDA según tu estructura de base de datos
        query = """
        SELECT 
            o.NombreOficina,
            o.Ubicacion,
            m.NombreElemento as Material,
            m.CantidadDisponible as Stock,
            m.ValorUnitario,
            (m.CantidadDisponible * m.ValorUnitario) as ValorTotal,
            m.CantidadMinima as StockMinimo,
            m.UsuarioCreador as Responsable,
            CASE WHEN m.Activo = 1 THEN 'Activo' ELSE 'Inactivo' END as Estado,
            m.FechaCreacion
        FROM Materiales m
        INNER JOIN Oficinas o ON m.OficinaCreadoraId = o.OficinaId
        WHERE m.Activo = 1
        ORDER BY o.NombreOficina, m.NombreElemento
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        
        # Crear archivo Excel en memoria
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Hoja 1: Inventario completo
            df.to_excel(writer, sheet_name='Inventario Corporativo', index=False)
            
            # Hoja 2: Resumen por oficina
            resumen_df = df.groupby(['NombreOficina', 'Ubicacion']).agg({
                'Material': 'count',
                'Stock': 'sum',
                'ValorTotal': 'sum'
            }).reset_index()
            resumen_df.columns = ['Oficina', 'Ubicación', 'Cantidad Materiales', 'Stock Total', 'Valor Total Inventario']
            resumen_df['Valor Total Inventario'] = resumen_df['Valor Total Inventario'].round(2)
            resumen_df.to_excel(writer, sheet_name='Resumen por Oficina', index=False)
            
            # Hoja 3: Totales generales
            totales_data = {
                'Métrica': [
                    'Total Oficinas con Inventario',
                    'Total Materiales',
                    'Stock Total',
                    'Valor Total Inventario',
                    'Valor Promedio por Material',
                    'Fecha de Exportación'
                ],
                'Valor': [
                    resumen_df['Oficina'].nunique(),
                    df['Material'].count(),
                    int(df['Stock'].sum()),
                    f"${df['ValorTotal'].sum():,.2f}",
                    f"${df['ValorTotal'].mean():,.2f}" if len(df) > 0 else "$0.00",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            totales_df = pd.DataFrame(totales_data)
            totales_df.to_excel(writer, sheet_name='Totales Generales', index=False)
        
        output.seek(0)
        
        # Crear nombre de archivo
        fecha_actual = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'inventario_corporativo_completo_{fecha_actual}.xlsx'
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        flash('Error al exportar el inventario corporativo', 'danger')
        return redirect('/reportes')

# ============================================================================
# EXPORTACIÓN POR OFICINA
# ============================================================================

@reportes_bp.route('/exportar/oficina/<int:oficina_id>/<string:formato>')
def exportar_oficina_inventario(oficina_id, formato):
    """Exporta el inventario de una oficina específica"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    # Verificar permisos
    if not _can_view_reportes():
        flash('No tiene permisos para exportar inventario corporativo', 'warning')
        return redirect('/reportes')
    
    try:
        from database import get_database_connection
        
        # Obtener parámetros - CORREGIDO
        incluir_materiales = request.args.get('materiales', '1') == '1'
        incluir_movimientos = request.args.get('movimientos', '1') == '1'
        incluir_totales = request.args.get('totales', '1') == '1'
        # incluir_solicitudes no se usa en la consulta SQL, se eliminó
        
        conn = get_database_connection()
        
        # Obtener información de la oficina - CORREGIDO
        cursor = conn.cursor()
        cursor.execute("""
            SELECT OficinaId, NombreOficina, Ubicacion, Region, Estado
            FROM Oficinas 
            WHERE OficinaId = ?
        """, (oficina_id,))
        
        oficina_data = cursor.fetchone()
        if not oficina_data:
            flash('Oficina no encontrada', 'danger')
            return redirect('/reportes')
        
        oficina = {
            'id': oficina_data[0],
            'nombre': oficina_data[1],
            'ubicacion': oficina_data[2],
            'region': oficina_data[3],
            'estado': oficina_data[4]
        }
        
        # Obtener materiales del inventario corporativo de esta oficina - CORREGIDO
        materiales = []
        if incluir_materiales:
            # CONSULTA CORREGIDA: Usar la tabla Materiales correcta
            cursor.execute("""
                SELECT 
                    m.MaterialId,
                    m.NombreElemento,
                    m.Descripcion,
                    m.ValorUnitario,
                    m.CantidadDisponible,
                    m.CantidadMinima,
                    m.Activo,
                    m.UsuarioCreador,
                    m.FechaCreacion,
                    m.Categoria
                FROM Materiales m
                WHERE m.OficinaCreadoraId = ? AND m.Activo = 1
                ORDER BY m.NombreElemento
            """, (oficina_id,))
            
            for row in cursor.fetchall():
                valor_total = row[3] * row[4] if row[3] and row[4] else 0  # ValorUnitario * CantidadDisponible
                material = {
                    'id': row[0],
                    'nombre': row[1],
                    'descripcion': row[2] or '',
                    'valor_unitario': float(row[3] or 0),
                    'cantidad': row[4] or 0,
                    'valor_total': float(valor_total),
                    'stock_minimo': row[5] or 0,
                    'activo': bool(row[6]),
                    'usuario_creador': row[7] or '',
                    'fecha_creacion': row[8],
                    'categoria': row[9] or 'General'
                }
                materiales.append(material)
        
        
        movimientos = []
        if incluir_movimientos:
            try:
                
                cursor.execute("""
                    SELECT TOP 15
                        ach.Fecha,
                        ach.Accion,
                        ach.Cantidad,
                        ach.UsuarioAccion,
                        ach.Observaciones,
                        pc.NombreProducto as MaterialNombre
                    FROM AsignacionesCorporativasHistorial ach
                    LEFT JOIN ProductosCorporativos pc ON ach.ProductoId = pc.ProductoId
                    WHERE ach.OficinaId = ?
                    ORDER BY ach.Fecha DESC
                """, (oficina_id,))
                
                for row in cursor.fetchall():
                    if row[0]:   
                        movimiento = {
                            'fecha': row[0],
                            'accion': row[1] or 'Asignación',
                            'cantidad': row[2] or 1,
                            'usuario_nombre': row[3] or 'Sistema',
                            'observaciones': row[4] or '',
                            'material_nombre': row[5] or 'Producto Corporativo'
                        }
                        movimientos.append(movimiento)
                        
            except Exception as mov_error:
                pass
                
            
            try:
                cursor.execute("""
                    SELECT TOP 10
                        mh.FechaMovimiento,
                        tm.NombreTipoMovimiento as Accion,
                        mh.Cantidad,
                        u.NombreUsuario,
                        mh.Observaciones,
                        mat.NombreElemento as MaterialNombre
                    FROM MovimientosHistorial mh
                    INNER JOIN TiposMovimiento tm ON mh.TipoMovimientoId = tm.TipoMovimientoId
                    INNER JOIN Materiales mat ON mh.MaterialId = mat.MaterialId
                    INNER JOIN Usuarios u ON mh.UsuarioId = u.UsuarioId
                    WHERE mh.OficinaId = ?
                    ORDER BY mh.FechaMovimiento DESC
                """, (oficina_id,))
                
                for row in cursor.fetchall():
                    if row[0]:  
                        movimiento = {
                            'fecha': row[0],
                            'accion': row[1] or 'Movimiento',
                            'cantidad': row[2] or 0,
                            'usuario_nombre': row[3] or 'Usuario',
                            'observaciones': row[4] or '',
                            'material_nombre': row[5] or 'Material'
                        }
                        movimientos.append(movimiento)
                        
            except Exception as mov_error2:
                pass
        
        conn.close()
        
        # Calcular totales
        total_materiales = len(materiales)
        valor_total_inventario = sum(m.get('valor_total', 0) for m in materiales)
        total_movimientos = len(movimientos)
        
        # Ordenar movimientos por fecha (más reciente primero)
        movimientos.sort(key=lambda x: x.get('fecha', datetime.min), reverse=True)
        
        # Exportar según formato
        if formato.lower() == 'excel':
            return _exportar_oficina_excel(oficina, materiales, movimientos,
                                          total_materiales, valor_total_inventario, 
                                          total_movimientos, incluir_totales)
        elif formato.lower() == 'pdf':
            return _exportar_oficina_pdf(oficina, materiales, movimientos,
                                        total_materiales, valor_total_inventario,
                                        total_movimientos, incluir_totales)
        elif formato.lower() == 'csv':
            return _exportar_oficina_csv(oficina, materiales, movimientos,
                                        total_materiales, valor_total_inventario,
                                        total_movimientos, incluir_totales)
        else:
            flash('Formato de exportación no válido', 'danger')
            return redirect('/reportes')
            
    except Exception as e:
        flash('Error al exportar el inventario de la oficina', 'danger')
        return redirect('/reportes')

def _exportar_oficina_excel(oficina, materiales, movimientos, total_materiales, 
                           valor_total_inventario, total_movimientos, incluir_totales):
    """Exporta a Excel el inventario de una oficina"""
    try:
        import pandas as pd
        from io import BytesIO
        
        # Crear DataFrames
        data_frames = []
        sheet_names = []
        
        # Hoja 1: Información de la oficina
        oficina_info = {
            'Campo': ['Nombre Oficina', 'Ubicación', 'Región', 'Estado', 'ID Oficina', 
                     'Total Materiales', 'Valor Total Inventario', 'Total Movimientos',
                     'Fecha Exportación'],
            'Valor': [oficina['nombre'], oficina['ubicacion'], oficina['region'], 
                     oficina['estado'], oficina['id'], total_materiales, 
                     f"${valor_total_inventario:,.2f}", total_movimientos,
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        }
        df_oficina = pd.DataFrame(oficina_info)
        data_frames.append(df_oficina)
        sheet_names.append('Información Oficina')
        
        # Hoja 2: Materiales del inventario
        if materiales:
            materiales_data = []
            for mat in materiales:
                fecha_creacion = mat['fecha_creacion']
                if hasattr(fecha_creacion, 'strftime'):
                    fecha_str = fecha_creacion.strftime('%Y-%m-%d')
                else:
                    fecha_str = str(fecha_creacion) if fecha_creacion else 'N/A'
                
                materiales_data.append({
                    'ID': mat['id'],
                    'Nombre': mat['nombre'],
                    'Descripción': mat['descripcion'],
                    'Categoría': mat['categoria'],
                    'Valor Unitario': f"${mat['valor_unitario']:,.2f}",
                    'Cantidad': mat['cantidad'],
                    'Valor Total': f"${mat['valor_total']:,.2f}",
                    'Stock Mínimo': mat['stock_minimo'],
                    'Estado': 'Activo' if mat['activo'] else 'Inactivo',
                    'Responsable': mat['usuario_creador'],
                    'Fecha Creación': fecha_str
                })
            
            df_materiales = pd.DataFrame(materiales_data)
            data_frames.append(df_materiales)
            sheet_names.append('Materiales Inventario')
        
        # Hoja 3: Historial de movimientos
        if movimientos:
            movimientos_data = []
            for mov in movimientos:
                fecha = mov['fecha']
                if hasattr(fecha, 'strftime'):
                    fecha_str = fecha.strftime('%Y-%m-%d %H:%M')
                else:
                    fecha_str = str(fecha) if fecha else 'N/A'
                
                movimientos_data.append({
                    'Fecha': fecha_str,
                    'Acción': mov['accion'],
                    'Material': mov['material_nombre'],
                    'Cantidad': mov['cantidad'],
                    'Usuario': mov['usuario_nombre'],
                    'Observaciones': mov['observaciones']
                })
            
            df_movimientos = pd.DataFrame(movimientos_data)
            data_frames.append(df_movimientos)
            sheet_names.append('Historial Movimientos')
        
        # Crear archivo Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for i, df in enumerate(data_frames):
                df.to_excel(writer, sheet_name=sheet_names[i], index=False)
                
                # Ajustar ancho de columnas
                worksheet = writer.sheets[sheet_names[i]]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        output.seek(0)
        
        # Nombre del archivo seguro
        nombre_oficina_safe = "".join(c for c in oficina['nombre'] if c.isalnum() or c in (' ', '_')).rstrip()
        nombre_oficina_safe = nombre_oficina_safe.replace(' ', '_').replace('/', '_')[:50]
        fecha = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'inventario_{nombre_oficina_safe}_{fecha}.xlsx'
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        flash('Error al generar el archivo Excel', 'danger')
        return redirect('/reportes')

def _exportar_oficina_pdf(oficina, materiales, movimientos, total_materiales,
                         valor_total_inventario, total_movimientos, incluir_totales):
    """Exporta a PDF el inventario de una oficina"""
    try:
        # Por ahora, redirigir a Excel hasta que implementemos PDF
        flash('La exportación a PDF estará disponible próximamente. Usando Excel por ahora.', 'info')
        
        # Crear parámetros para redirección a Excel
        from urllib.parse import urlencode
        params = {
            'materiales': '1' if materiales else '0',
            'movimientos': '1' if movimientos else '0',
            'totales': '1' if incluir_totales else '0'
        }
        
        oficina_id_val = oficina["id"]
        return redirect(f"/reportes/oficinas/{oficina_id_val}/inventario/excel?" + urlencode(params))
        
    except Exception as e:
        flash('Error al generar el PDF', 'danger')
        return redirect('/reportes')

def _exportar_oficina_csv(oficina, materiales, movimientos, total_materiales,
                         valor_total_inventario, total_movimientos, incluir_totales):
    """Exporta a CSV el inventario de una oficina"""
    try:
        import pandas as pd
        from io import StringIO
        
        # Crear contenido CSV
        output = StringIO()
        
        # Encabezado con información de la oficina
        output.write(f"Inventario Corporativo - {oficina['nombre']}\n")
        output.write(f"Ubicación: {oficina['ubicacion']}\n")
        output.write(f"Fecha Exportación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Total Materiales: {total_materiales}\n")
        output.write(f"Valor Total Inventario: ${valor_total_inventario:,.2f}\n")
        output.write(f"Total Movimientos: {total_movimientos}\n")
        output.write("\n")
        
        # Sección de materiales
        if materiales:
            output.write("=== MATERIALES DEL INVENTARIO ===\n")
            df_materiales = pd.DataFrame(materiales)
            df_materiales = df_materiales[['nombre', 'cantidad', 'valor_unitario', 'valor_total']]
            df_materiales.columns = ['Material', 'Cantidad', 'Valor_Unitario', 'Valor_Total']
            df_materiales.to_csv(output, index=False)
            output.write("\n")
        
        # Sección de movimientos
        if movimientos:
            output.write("=== HISTORIAL DE MOVIMIENTOS ===\n")
            df_movimientos = pd.DataFrame(movimientos)
            df_movimientos = df_movimientos[['fecha', 'accion', 'material_nombre', 'cantidad', 'usuario_nombre']]
            df_movimientos.columns = ['Fecha', 'Acción', 'Material', 'Cantidad', 'Usuario']
            df_movimientos.to_csv(output, index=False)
        
        # Convertir a bytes
        output_str = output.getvalue()
        output_bytes = BytesIO(output_str.encode('utf-8'))
        output_bytes.seek(0)
        
        # Nombre del archivo
        nombre_oficina_safe = oficina['nombre'].replace(' ', '_').replace('/', '_')
        fecha = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'inventario_{nombre_oficina_safe}_{fecha}.csv'
        
        return send_file(
            output_bytes,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        flash('Error al generar el archivo CSV', 'danger')
        return redirect('/reportes')

# ============================================================================
# FUNCIÓN ADICIONAL PARA DEPURAR DATOS DE OFICINA
# ============================================================================

@reportes_bp.route('/debug/oficina/<int:oficina_id>')
def debug_oficina_data(oficina_id):
    """Endpoint para depurar datos de una oficina específica"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    try:
        from database import get_database_connection
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Obtener información básica de la oficina
        cursor.execute("""
            SELECT OficinaId, NombreOficina, Ubicacion, Region, Estado 
            FROM Oficinas 
            WHERE OficinaId = ?
        """, (oficina_id,))
        
        oficina = cursor.fetchone()
        
        # Obtener materiales de esta oficina
        cursor.execute("""
            SELECT COUNT(*) as total_materiales, 
                   SUM(CantidadDisponible) as total_stock,
                   SUM(CantidadDisponible * ValorUnitario) as valor_total
            FROM Materiales 
            WHERE OficinaCreadoraId = ? AND Activo = 1
        """, (oficina_id,))
        
        materiales_stats = cursor.fetchone()
        
        # Obtener algunos materiales como muestra
        cursor.execute("""
            SELECT TOP 5 MaterialId, NombreElemento, CantidadDisponible, ValorUnitario
            FROM Materiales 
            WHERE OficinaCreadoraId = ? AND Activo = 1
        """, (oficina_id,))
        
        materiales_sample = cursor.fetchall()
        
        conn.close()
        
        # Preparar respuesta JSON para depuración
        debug_info = {
            'oficina': {
                'id': oficina[0] if oficina else None,
                'nombre': oficina[1] if oficina else None,
                'ubicacion': oficina[2] if oficina else None,
                'region': oficina[3] if oficina else None,
                'estado': oficina[4] if oficina else None
            },
            'materiales_stats': {
                'total_materiales': materiales_stats[0] if materiales_stats else 0,
                'total_stock': materiales_stats[1] if materiales_stats else 0,
                'valor_total': materiales_stats[2] if materiales_stats else 0
            },
            'materiales_sample': [
                {
                    'id': row[0],
                    'nombre': row[1],
                    'cantidad': row[2],
                    'valor_unitario': row[3]
                } for row in materiales_sample
            ]
        }
        
        return jsonify(debug_info)
        
    except Exception as e:
        return jsonify({'error': 'Error interno'})

@reportes_bp.route('/material/<int:material_id>/historial')
def material_historial(material_id):
    """Obtiene el historial completo de un material"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para ver historial de materiales', 'warning')
        return redirect('/reportes')
    
    try:
        from database import get_database_connection
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Primero obtener información básica del material
        cursor.execute("""
            SELECT m.MaterialId, m.NombreElemento, m.Descripcion, m.Categoria,
                   m.ValorUnitario, m.CantidadDisponible, m.StockMinimo,
                   o.NombreOficina as Ubicacion, m.Asignable
            FROM Materiales m
            LEFT JOIN Oficinas o ON m.OficinaCreadoraId = o.OficinaId
            WHERE m.MaterialId = ?
        """, (material_id,))
        
        material_data = cursor.fetchone()
        if not material_data:
            return jsonify({'error': 'Material no encontrado'})
        
        material = {
            'id': material_data[0],
            'nombre': material_data[1],
            'descripcion': material_data[2],
            'categoria': material_data[3],
            'valor_unitario': float(material_data[4] or 0),
            'cantidad': material_data[5],
            'stock_minimo': material_data[6],
            'ubicacion': material_data[7],
            'asignable': bool(material_data[8]) if material_data[8] is not None else False
        }
        
        # Buscar historial de movimientos para este material
        historial = []
        
        # Buscar en tabla de solicitudes
        cursor.execute("""
            SELECT sm.FechaSolicitud as Fecha, 'Solicitud' as Accion,
                   sm.CantidadSolicitada as Cantidad, 
                   o.NombreOficina as Oficina,
                   sm.UsuarioSolicitante as Usuario,
                   sm.Observacion as Observaciones
            FROM SolicitudesMaterial sm
            INNER JOIN Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
            WHERE sm.MaterialId = ?
            ORDER BY sm.FechaSolicitud DESC
        """, (material_id,))
        
        for row in cursor.fetchall():
            historial.append({
                'fecha': row[0],
                'accion': row[1],
                'cantidad': row[2],
                'oficina': row[3],
                'usuario': row[4],
                'observaciones': row[5]
            })
        
        # Buscar en tabla de préstamos
        cursor.execute("""
            SELECT pe.FechaPrestamo as Fecha, 'Préstamo' as Accion,
                   pe.CantidadPrestada as Cantidad,
                   o.NombreOficina as Oficina,
                   u.NombreUsuario as Usuario,
                   pe.Observaciones
            FROM PrestamosElementos pe
            INNER JOIN Oficinas o ON pe.OficinaId = o.OficinaId
            INNER JOIN Usuarios u ON pe.UsuarioSolicitanteId = u.UsuarioId
            WHERE pe.ElementoId = ? AND pe.Activo = 1
            ORDER BY pe.FechaPrestamo DESC
        """, (material_id,))
        
        for row in cursor.fetchall():
            historial.append({
                'fecha': row[0],
                'accion': row[1],
                'cantidad': row[2],
                'oficina': row[3],
                'usuario': row[4],
                'observaciones': row[5]
            })
        
        conn.close()
        
        # Ordenar historial por fecha
        historial_ordenado = sorted(historial, key=lambda x: x['fecha'], reverse=True)
        
        return jsonify({
            'material': material,
            'historial': historial_ordenado[:50]  # Limitar a 50 registros
        })
        
    except Exception as e:
        return jsonify({'error': 'Error interno'})

# ==============================
# API PARA DETALLE DE PRÉSTAMOS 
# ==============================

@reportes_bp.route('/api/prestamos/<int:prestamo_id>/detalle')
def api_prestamo_detalle(prestamo_id):
    """API para obtener detalle de un préstamo específico - VERSIÓN CORREGIDA"""
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401
    
    try:
        from database import get_database_connection
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Obtener información detallada del préstamo - CONSULTA CORREGIDA
        cursor.execute("""
            SELECT 
                pe.PrestamoId,
                pe.ElementoId,
                ep.NombreElemento as MaterialNombre,
                pe.UsuarioSolicitanteId,
                u.NombreUsuario as SolicitanteNombre,
                pe.OficinaId,
                o.NombreOficina as OficinaNombre,
                pe.CantidadPrestada,
                pe.FechaPrestamo,
                pe.FechaDevolucionPrevista,
                pe.FechaDevolucionReal,
                pe.Estado,
                pe.Evento,
                pe.Observaciones,
                pe.UsuarioPrestador,
                pe.Activo,
                pe.UsuarioDevolucion,
                pe.UsuarioAprobador,
                pe.FechaAprobacion,
                pe.UsuarioRechazador,
                pe.FechaRechazo,
                DATEDIFF(day, pe.FechaPrestamo, GETDATE()) as DiasTranscurridos,
                CASE 
                    WHEN pe.Estado = 'PRESTADO' AND pe.FechaDevolucionPrevista < GETDATE() THEN 1
                    ELSE 0 
                END as Vencido,
                CASE 
                    WHEN pe.Estado = 'PRESTADO' 
                         AND pe.FechaDevolucionPrevista BETWEEN GETDATE() AND DATEADD(day, 7, GETDATE()) 
                    THEN 1
                    ELSE 0 
                END as PorVencer
            FROM PrestamosElementos pe
            INNER JOIN ElementosPublicitarios ep ON pe.ElementoId = ep.ElementoId
            INNER JOIN Usuarios u ON pe.UsuarioSolicitanteId = u.UsuarioId
            INNER JOIN Oficinas o ON pe.OficinaId = o.OficinaId
            WHERE pe.PrestamoId = ?
            AND pe.Activo = 1
        """, (prestamo_id,))
        
        prestamo_data = cursor.fetchone()
        
        if not prestamo_data:
            conn.close()
            return jsonify({'success': False, 'message': 'Préstamo no encontrado o inactivo'}), 404
        
        # Convertir a diccionario
        column_names = [column[0] for column in cursor.description]
        prestamo = dict(zip(column_names, prestamo_data))
        
        # Obtener historial de movimientos
        cursor.execute("""
            SELECT 
                FechaCambio as Fecha,
                EstadoNuevo as Accion,
                UsuarioCambio as Usuario,
                Observaciones
            FROM PrestamosHistorialEstados 
            WHERE PrestamoId = ?
            ORDER BY FechaCambio DESC
        """, (prestamo_id,))
        
        historial = []
        for row in cursor.fetchall():
            historial.append({
                'fecha': row[0].strftime('%Y-%m-%d %H:%M:%S') if row[0] else None,
                'accion': row[1],
                'usuario': row[2],
                'observaciones': row[3] or ''
            })
        
        prestamo['historial'] = historial
        
        # Convertir fechas a formato string para JSON
        date_fields = ['FechaPrestamo', 'FechaDevolucionPrevista', 'FechaDevolucionReal', 
                      'FechaAprobacion', 'FechaRechazo']
        for field in date_fields:
            if prestamo.get(field):
                prestamo[field] = prestamo[field].strftime('%Y-%m-%d %H:%M:%S')
        
        conn.close()
        
        return jsonify({
            'success': True,
            'prestamo': prestamo
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Error interno del servidor: Error interno'
        }), 500

# ============================================================================
# API PARA REGISTRAR DEVOLUCIÓN DE PRÉSTAMO
# ============================================================================

@reportes_bp.route('/api/prestamos/<int:prestamo_id>/devolver', methods=['POST'])
def api_prestamo_devolver(prestamo_id):
    """API para registrar devolución de un préstamo"""
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401
    
    # Verificar permisos
    if not can_access('prestamos', 'return'):
        return jsonify({'success': False, 'message': 'No tiene permisos para registrar devoluciones'}), 403
    
    try:
        from database import get_database_connection
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Verificar que el préstamo existe y está activo
        cursor.execute("""
            SELECT Estado, ElementoId, CantidadPrestada 
            FROM PrestamosElementos 
            WHERE PrestamoId = ? AND Activo = 1
        """, (prestamo_id,))
        
        prestamo = cursor.fetchone()
        if not prestamo:
            conn.close()
            return jsonify({'success': False, 'message': 'Préstamo no encontrado o inactivo'}), 404
        
        estado, elemento_id, cantidad = prestamo
        
        if estado != 'PRESTADO':
            conn.close()
            return jsonify({'success': False, 'message': 'El préstamo no está en estado PRESTADO'}), 400
        
        # Obtener usuario actual
        usuario_nombre = session.get('nombre_usuario', 'Sistema')
        
        # Registrar devolución
        cursor.execute("""
            UPDATE PrestamosElementos 
            SET Estado = 'DEVUELTO',
                FechaDevolucionReal = GETDATE(),
                UsuarioDevolucion = ?,
                FechaActualizacion = GETDATE(),
                UsuarioActualizacion = ?
            WHERE PrestamoId = ?
        """, (usuario_nombre, usuario_nombre, prestamo_id))
        
        # Registrar en historial de estados
        cursor.execute("""
            INSERT INTO PrestamosHistorialEstados 
            (PrestamoId, EstadoAnterior, EstadoNuevo, UsuarioCambio, FechaCambio, Observaciones, TipoAccion)
            VALUES (?, ?, ?, ?, GETDATE(), 'Devolución registrada', 'DEVOLUCION')
        """, (prestamo_id, 'PRESTADO', 'DEVUELTO', usuario_nombre))
        
        # Actualizar stock del elemento
        cursor.execute("""
            UPDATE ElementosPublicitarios 
            SET CantidadDisponible = CantidadDisponible + ?
            WHERE ElementoId = ?
        """, (cantidad, elemento_id))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Devolución registrada exitosamente'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Error al registrar la devolución: Error interno'
        }), 500

# ============================================================================
# AGREGAR NUEVA RUTA PARA INVENTARIO CORPORATIVO CON ASIGNACIONES
# ============================================================================

@reportes_bp.route('/inventario-corporativo')
def reporte_inventario_corporativo():
    """Reporte de inventario corporativo con asignaciones - VERSIÓN SIMPLIFICADA"""
    if not _require_login():
        return redirect(url_for('auth.login'))
    
    if not _can_view_reportes():
        flash('No tiene permisos para ver el inventario corporativo', 'warning')
        return redirect('/reportes')
    
    try:
        from database import get_database_connection
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # CONSULTA SIMPLIFICADA - SOLO TABLAS ESENCIALES
        cursor.execute("""
            SELECT 
                p.ProductoId,
                p.NombreProducto,
                p.CodigoUnico,
                p.Descripcion as NumeroSerie,
                p.CategoriaId,
                CAST(NULL AS NVARCHAR(100)) as NombreCategoria,
                p.FechaCreacion as FechaAdquisicion,
                p.ValorUnitario as ValorCompra,
                CAST(NULL AS VARCHAR(20)) as EstadoProducto,
                p.Descripcion as Observaciones,
                a.AsignacionId,
                a.OficinaId,
                o.NombreOficina,
                a.UsuarioADNombre,
                a.UsuarioADEmail,
                a.FechaAsignacion,
                UPPER(LTRIM(RTRIM(a.Estado))) as EstadoAsignacion,
                a.FechaConfirmacion,
                a.UsuarioConfirmacion,
                a.Observaciones as ObservacionesAsignacion
            FROM ProductosCorporativos p
            LEFT JOIN Asignaciones a ON p.ProductoId = a.ProductoId AND a.Activo = 1
            LEFT JOIN Oficinas o ON a.OficinaId = o.OficinaId
            WHERE p.Activo = 1
            ORDER BY p.FechaCreacion DESC, p.NombreProducto
        """)
        
        columns = [column[0] for column in cursor.description]
        productos_raw = cursor.fetchall()
        productos = []
        
        for row in productos_raw:
            producto = dict(zip(columns, row))
            productos.append(producto)
            if producto.get('AsignacionId'):
                logger.info(f"DEBUG - Producto {producto['ProductoId']}: Estado='{producto.get('EstadoAsignacion')}', AsignacionId={producto.get('AsignacionId')}")

        conn.close()
        
        # Aplicar filtro según permisos
        if get_office_filter() == 'own':
            oficina_usuario = session.get('oficina_id')
            productos = [p for p in productos if p.get('OficinaId') == oficina_usuario]
        
        # Calcular estadísticas
        total_productos = len(set([p['ProductoId'] for p in productos]))
        total_asignados = len([p for p in productos if p.get('AsignacionId')])
        total_confirmados = len([p for p in productos if p.get('EstadoAsignacion') == 'CONFIRMADO'])
        total_pendientes = len([p for p in productos if p.get('EstadoAsignacion') == 'ASIGNADO'])
        valor_total = sum([float(p.get('ValorCompra', 0) or 0) for p in productos if p.get('ProductoId')])
        
        logger.info(f"DEBUG - Total confirmados: {total_confirmados}, Total asignados: {total_asignados}")

        return render_template('reportes/inventario_corporativo.html',
                             productos=productos,
                             total_productos=total_productos,
                             total_asignados=total_asignados,
                             total_confirmados=total_confirmados,
                             total_pendientes=total_pendientes,
                             valor_total=valor_total)
    except Exception as e:
        flash('Error al generar el reporte de inventario corporativo: Error interno', 'danger')
        logger.error("Error interno (%s)", type(e).__name__)

        return render_template('reportes/inventario_corporativo.html',
                             productos=[],
                             total_productos=0,
                             total_asignados=0,
                             total_confirmados=0,
                             total_pendientes=0,
                             valor_total=0)

# ================================
# REPORTE DE COBROS POP (TESORERÍA)
# ================================

@reportes_bp.route('/cobros-pop')
def reporte_cobros_pop():
<<<<<<< HEAD
    """Cobros POP por periodo/oficina con detalle y diferido por solicitud."""
=======
    """Cobros mensuales por oficina (Material POP aprobado).

    Acceso: únicamente roles con permiso reportes.cobros_view
    (tesorería, administrador, líder inventario).

    Agrupa por oficina y producto, y calcula valores usando:
      - ValorUnitario (Materiales)
      - CantidadEntregada/CantidadSolicitada (SolicitudesMaterial)
      - PorcentajeOficina (SolicitudesMaterial)
      - ValorOficina/ValorTotalSolicitado (si están precalculados)
    """
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
    if not _require_login():
        return redirect(url_for('auth.login'))

    if not _can_view_cobros_pop():
        flash('No tiene permisos para ver el reporte de cobros', 'warning')
        return redirect('/reportes')

    periodo = _parse_periodo(request.args.get('periodo', ''))

    try:
        detalle = _consultar_cobros_pop(periodo)
<<<<<<< HEAD
        solicitudes = _consultar_cobros_pop_solicitudes(periodo)
        estados = CobroPOPMensualModel.obtener_estados_por_periodo(periodo)
        cuotas_resumen = CobroPOPDiferidoSolicitudModel.obtener_resumen_cuotas_periodo(periodo)

=======

        # Agrupar por oficina
        estados = CobroPOPMensualModel.obtener_estados_por_periodo(periodo)
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
        oficinas = {}
        for row in detalle:
            oid = row['oficina_id']
            if oid not in oficinas:
                st = estados.get(oid, {})
                oficinas[oid] = {
                    'oficina_id': oid,
                    'oficina_nombre': row['oficina_nombre'],
                    'estado_cobro': st.get('estado', 'PENDIENTE'),
                    'estado_fecha': st.get('fecha_cambio'),
                    'estado_usuario': st.get('usuario_cambio'),
                    'productos': [],
<<<<<<< HEAD
                    'solicitudes': [],
                    'planes': [],
                    'cuotas_mes': [],
                    'estado_cuota_mes': None,
=======
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
                    'total_solicitudes': 0,
                    'total_productos': 0,
                    'total_cantidad': 0,
                    'total_valor_total': 0.0,
                    'total_valor_cobro': 0.0,
<<<<<<< HEAD
                    'total_cuota_mes': 0.0,
                    'total_a_pagar': 0.0,
                }
=======
                }

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
            oficinas[oid]['productos'].append(row)
            oficinas[oid]['total_solicitudes'] += row['num_solicitudes']
            oficinas[oid]['total_cantidad'] += row['cantidad_total']
            oficinas[oid]['total_valor_total'] += row['valor_total']
            oficinas[oid]['total_valor_cobro'] += row['valor_cobro_oficina']

<<<<<<< HEAD
        for s in solicitudes:
            oid = s['oficina_id']
            if oid not in oficinas:
                st = estados.get(oid, {})
                oficinas[oid] = {
                    'oficina_id': oid,
                    'oficina_nombre': s['oficina_nombre'],
                    'estado_cobro': st.get('estado', 'PENDIENTE'),
                    'estado_fecha': st.get('fecha_cambio'),
                    'estado_usuario': st.get('usuario_cambio'),
                    'productos': [],
                    'solicitudes': [],
                    'planes': [],
                    'cuotas_mes': [],
                    'estado_cuota_mes': None,
                    'total_solicitudes': 0,
                    'total_productos': 0,
                    'total_cantidad': 0,
                    'total_valor_total': 0.0,
                    'total_valor_cobro': 0.0,
                    'total_cuota_mes': 0.0,
                    'total_a_pagar': 0.0,
                }
            oficinas[oid]['solicitudes'].append(s)

        oficina_ids = sorted(oficinas.keys())
        planes_map = CobroPOPDiferidoSolicitudModel.obtener_planes_y_cuotas_oficinas(oficina_ids) if oficina_ids else {}

        for oid, of in oficinas.items():
            of['total_productos'] = len(of['productos'])
            q = cuotas_resumen.get(oid, {})
            of['total_cuota_mes'] = float(q.get('total_cuota_mes') or 0)
            of['estado_cuota_mes'] = q.get('estado_cuota_mes')
            of['cuotas_mes'] = q.get('cuotas_mes') or []
            of['planes'] = (planes_map.get(oid) or {}).get('planes') or []
            planes_by_solicitud = {}
            for p in of['planes']:
                planes_by_solicitud.setdefault(int(p['solicitud_id']), []).append(p)
            for s in of['solicitudes']:
                sp = planes_by_solicitud.get(int(s['solicitud_id']), [])
                s['planes'] = sp
                s['tiene_diferido_activo'] = len(sp) > 0
                s['puede_diferir'] = float(s.get('valor_cobro_oficina') or 0) > 0 and not s['tiene_diferido_activo']
                s['inicio_actual'] = periodo
                s['inicio_proximo'] = _add_months(periodo, 1)
            of['total_a_pagar'] = float(of['total_valor_cobro'] or 0) + float(of['total_cuota_mes'] or 0)

        oficinas_list = sorted(oficinas.values(), key=lambda x: (x['oficina_nombre'] or '').lower())
        total_general = {
            'total_oficinas': len(oficinas_list),
            'total_solicitudes': sum(len(o.get('solicitudes') or []) for o in oficinas_list),
            'total_productos': sum(o['total_productos'] for o in oficinas_list),
            'total_valor_total': sum(o['total_valor_total'] for o in oficinas_list),
            'total_valor_cobro': sum(o['total_valor_cobro'] for o in oficinas_list),
            'total_valor_cuotas': sum(float(o.get('total_cuota_mes') or 0) for o in oficinas_list),
            'total_a_pagar': sum(float(o.get('total_a_pagar') or 0) for o in oficinas_list),
=======
        # total_productos
        for oid in list(oficinas.keys()):
            oficinas[oid]['total_productos'] = len(oficinas[oid]['productos'])

        oficinas_list = sorted(oficinas.values(), key=lambda x: (x['oficina_nombre'] or '').lower())

        total_general = {
            'total_oficinas': len(oficinas_list),
            'total_solicitudes': sum(o['total_solicitudes'] for o in oficinas_list),
            'total_productos': sum(o['total_productos'] for o in oficinas_list),
            'total_valor_total': sum(o['total_valor_total'] for o in oficinas_list),
            'total_valor_cobro': sum(o['total_valor_cobro'] for o in oficinas_list),
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
        }

        return render_template(
            'reportes/cobros_pop.html',
            periodo=periodo,
            oficinas=oficinas_list,
            total_general=total_general,
            can_cancel=_can_cancel_cobros_pop(),
            can_export=_can_export_cobros_pop(),
        )
<<<<<<< HEAD
    except Exception:
        logger.exception('Error cargando cobros POP')
=======

    except Exception:
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
        flash('Error al cargar el reporte de cobros', 'danger')
        return redirect('/reportes')


@reportes_bp.route('/cobros-pop/estado', methods=['POST'])
def actualizar_estado_cobro_pop():
<<<<<<< HEAD
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401
    if not _can_cancel_cobros_pop():
        return jsonify({'success': False, 'message': 'Sin permisos'}), 403
=======
    """Actualiza estado de cobro por oficina/periodo (AJAX)."""
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401

    if not _can_cancel_cobros_pop():
        return jsonify({'success': False, 'message': 'Sin permisos'}), 403

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
    data = request.get_json(silent=True) or request.form
    periodo = _parse_periodo(data.get('periodo', ''))
    oficina_id = data.get('oficina_id')
    estado = (data.get('estado') or 'CANCELADO').strip().upper()
<<<<<<< HEAD
=======

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
    try:
        oficina_id_int = int(oficina_id)
    except Exception:
        return jsonify({'success': False, 'message': 'Oficina inválida'}), 400
<<<<<<< HEAD
    usuario = session.get('usuario_nombre') or session.get('usuario_email') or 'Sistema'
    ok, msg = CobroPOPMensualModel.set_estado(periodo, oficina_id_int, estado, usuario)
    return jsonify({'success': ok, 'message': msg}), (200 if ok else 400)
=======

    usuario = session.get('usuario_nombre') or session.get('usuario_email') or 'Sistema'

    ok, msg = CobroPOPMensualModel.set_estado(periodo, oficina_id_int, estado, usuario)
    return jsonify({'success': ok, 'message': msg, 'periodo': periodo, 'oficina_id': oficina_id_int, 'estado': estado}), (200 if ok else 400)
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e


@reportes_bp.route('/cobros-pop/estado-todos', methods=['POST'])
def actualizar_estado_cobro_pop_todos():
<<<<<<< HEAD
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401
    if not _can_cancel_cobros_pop():
        return jsonify({'success': False, 'message': 'Sin permisos'}), 403
    data = request.get_json(silent=True) or request.form
    periodo = _parse_periodo(data.get('periodo', ''))
    estado = (data.get('estado') or 'CANCELADO').strip().upper()
    detalle = _consultar_cobros_pop(periodo)
    oficina_ids = sorted({r['oficina_id'] for r in detalle})
    usuario = session.get('usuario_nombre') or session.get('usuario_email') or 'Sistema'
    ok, msg = CobroPOPMensualModel.set_estado_masivo(periodo, oficina_ids, estado, usuario)
    return jsonify({'success': ok, 'message': msg}), (200 if ok else 400)


@reportes_bp.route('/cobros-pop/diferir-solicitud', methods=['POST'])
def cobros_pop_diferir_solicitud():
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401
    if not _can_cancel_cobros_pop():
        return jsonify({'success': False, 'message': 'Sin permisos'}), 403

    data = request.get_json(silent=True) or request.form
    periodo = _parse_periodo(data.get('periodo', ''))
    oficina_id = data.get('oficina_id')
    solicitud_id = data.get('solicitud_id')
    cuotas = data.get('cuotas')
    inicio_tipo = (data.get('inicio_tipo') or 'actual').strip().lower()
    try:
        oficina_id = int(oficina_id)
        solicitud_id = int(solicitud_id)
        cuotas = int(cuotas)
    except Exception:
        return jsonify({'success': False, 'message': 'Datos inválidos'}), 400

    detalle = _consultar_cobros_pop_solicitudes(periodo, oficina_id=oficina_id)
    solicitud = next((x for x in detalle if int(x['solicitud_id']) == solicitud_id), None)
    if not solicitud:
        return jsonify({'success': False, 'message': 'No se encontró la solicitud en el periodo/oficina'}), 404
    total = float(solicitud.get('valor_cobro_oficina') or 0)
    if total <= 0:
        return jsonify({'success': False, 'message': 'La solicitud no tiene valor a diferir'}), 400

    periodo_inicio = periodo if inicio_tipo == 'actual' else _add_months(periodo, 1)
    usuario = session.get('usuario_nombre') or session.get('usuario_email') or 'Sistema'
    ok, msg = CobroPOPDiferidoSolicitudModel.crear_plan(solicitud_id, oficina_id, periodo, total, cuotas, periodo_inicio, usuario)
    if not ok:
        logger.warning('No se pudo crear diferido POP. oficina_id=%s solicitud_id=%s periodo=%s inicio=%s motivo=%s', oficina_id, solicitud_id, periodo, periodo_inicio, msg)
        return jsonify({'success': False, 'message': msg}), 400
    parts = (msg or '').split('|')
    return jsonify({
        'success': True,
        'message': 'Diferido creado correctamente',
        'plan_id': int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None,
        'valor_cuota': float(parts[2]) if len(parts) > 2 else 0,
        'periodo_inicio': periodo_inicio,
        'total_diferido': total,
        'numero_cuotas': cuotas,
    }), 200


@reportes_bp.route('/cobros-pop/cuota/pagar-mes', methods=['POST'])
def cobros_pop_pagar_cuotas_mes():
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401
    if not _can_cancel_cobros_pop():
        return jsonify({'success': False, 'message': 'Sin permisos'}), 403
    data = request.get_json(silent=True) or request.form
    periodo = _parse_periodo(data.get('periodo', ''))
    oficina_id = data.get('oficina_id')
    pagado = bool(data.get('pagado', True))
    try:
        oficina_id = int(oficina_id)
    except Exception:
        return jsonify({'success': False, 'message': 'Oficina inválida'}), 400
    usuario = session.get('usuario_nombre') or session.get('usuario_email') or 'Sistema'
    ok, msg = CobroPOPDiferidoSolicitudModel.set_pago_mes(oficina_id, periodo, pagado, usuario)
    return jsonify({'success': ok, 'message': msg, 'periodo': periodo}), (200 if ok else 400)
=======
    """Actualiza estado de cobro para todas las oficinas con cobro en el periodo (AJAX)."""
    if not _require_login():
        return jsonify({'success': False, 'message': 'No autenticado'}), 401

    if not _can_cancel_cobros_pop():
        return jsonify({'success': False, 'message': 'Sin permisos'}), 403

    data = request.get_json(silent=True) or request.form
    periodo = _parse_periodo(data.get('periodo', ''))
    estado = (data.get('estado') or 'CANCELADO').strip().upper()

    detalle = _consultar_cobros_pop(periodo)
    oficina_ids = sorted({r['oficina_id'] for r in detalle})

    usuario = session.get('usuario_nombre') or session.get('usuario_email') or 'Sistema'
    ok, msg = CobroPOPMensualModel.set_estado_masivo(periodo, oficina_ids, estado, usuario)
    return jsonify({'success': ok, 'message': msg, 'periodo': periodo, 'estado': estado, 'oficinas': len(oficina_ids)}), (200 if ok else 400)
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e


@reportes_bp.route('/cobros-pop/export/<string:formato>')
def exportar_cobros_pop(formato: str):
<<<<<<< HEAD
    if not _require_login():
        return redirect(url_for('auth.login'))
=======
    """Exporta cobros POP a Excel o PDF.

    Query params:
      - periodo=YYYY-MM
      - oficina_id (opcional; si viene exporta solo esa oficina)
      - solo_estado=pendiente|cancelado|todos (opcional; aplica solo para export general)
    """
    if not _require_login():
        return redirect(url_for('auth.login'))

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
    if not _can_export_cobros_pop():
        flash('No tiene permisos para exportar cobros', 'warning')
        return redirect('/reportes')

    formato = (formato or '').strip().lower()
    if formato not in ('excel', 'pdf'):
        flash('Formato de exportación no válido', 'danger')
        return redirect('/reportes/cobros-pop')

    periodo = _parse_periodo(request.args.get('periodo', ''))
    oficina_id_raw = request.args.get('oficina_id')
<<<<<<< HEAD
=======
    solo_estado = (request.args.get('solo_estado') or 'todos').strip().lower()

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
    oficina_id = None
    if oficina_id_raw:
        try:
            oficina_id = int(oficina_id_raw)
        except Exception:
            oficina_id = None

<<<<<<< HEAD
    detalle = _consultar_cobros_pop_solicitudes(periodo, oficina_id=oficina_id)
    estados = CobroPOPMensualModel.obtener_estados_por_periodo(periodo)
    oficina_ids = sorted({int(r['oficina_id']) for r in detalle})
    planes_map = CobroPOPDiferidoSolicitudModel.obtener_planes_y_cuotas_oficinas(oficina_ids) if oficina_ids else {}
=======
    detalle = _consultar_cobros_pop(periodo, oficina_id=oficina_id)
    estados = CobroPOPMensualModel.obtener_estados_por_periodo(periodo)

    if oficina_id is None and solo_estado in ('pendiente', 'cancelado'):
        estado_obj = solo_estado.upper()
        oficinas_filtradas = {oid for oid, st in estados.items() if (st.get('estado') or 'PENDIENTE').upper() == estado_obj}
        if estado_obj == 'PENDIENTE':
            oficinas_con_reg = set(estados.keys())
            oficinas_en_detalle = {r['oficina_id'] for r in detalle}
            oficinas_sin_reg = oficinas_en_detalle - oficinas_con_reg
            oficinas_filtradas |= oficinas_sin_reg
        detalle = [r for r in detalle if r['oficina_id'] in oficinas_filtradas]
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e

    filas = []
    for r in detalle:
        st = estados.get(r['oficina_id'], {})
<<<<<<< HEAD
        planes_oficina = (planes_map.get(int(r['oficina_id'])) or {}).get('planes') or []
        planes_solicitud = [p for p in planes_oficina if int(p.get('solicitud_id') or 0) == int(r['solicitud_id']) and p.get('activo')]
        plan = planes_solicitud[0] if planes_solicitud else None
        cuota_mes = None
        if plan:
            for cuota in plan.get('cuotas') or []:
                if (cuota.get('periodo') or '') == periodo:
                    cuota_mes = cuota
                    break
        numero_cuotas_plan = int(plan.get('numero_cuotas') or 0) if plan else 0
        cuota_actual_numero = int(cuota_mes.get('numero_cuota') or 0) if cuota_mes else 0
        if plan and cuota_mes:
            cuota_actual_texto = f"{cuota_actual_numero} de {numero_cuotas_plan}"
            cuotas_faltantes = max(numero_cuotas_plan - cuota_actual_numero, 0)
        elif plan and (periodo or '') < (plan.get('periodo_inicio') or ''):
            cuota_actual_texto = 'AUN NO INICIA'
            cuotas_faltantes = numero_cuotas_plan
        elif plan and (periodo or '') > (plan.get('periodo_fin') or ''):
            cuota_actual_texto = 'FINALIZADO'
            cuotas_faltantes = 0
        else:
            cuota_actual_texto = 'N/A' if not plan else 'SIN CUOTA EN EL MES'
            cuotas_faltantes = numero_cuotas_plan if plan else 0

        filas.append({
            'Periodo': periodo,
            'Oficina': r['oficina_nombre'],
            'SolicitudId': r['solicitud_id'],
            'Estado Cobro': (st.get('estado') or 'PENDIENTE'),
            'FechaAprobacion': r['fecha_aprobacion'],
            'Cantidad Total': r['cantidad_total'],
            'Items': r['numero_registros'],
            '% Oficina': r['porcentaje_oficina'],
            'Valor Total': r['valor_total'],
            'Valor a Cobrar Oficina': r['valor_cobro_oficina'],
            'Tiene Diferido': 'SI' if plan else 'NO',
            'Inicio Diferido': plan.get('periodo_inicio') if plan else '',
            'Fin Diferido': plan.get('periodo_fin') if plan else '',
            'No. Cuotas': numero_cuotas_plan,
            'Cuota Actual': cuota_actual_texto,
            'Cuotas Faltantes': cuotas_faltantes,
            'Valor Cuota Mes': float(cuota_mes.get('valor_cuota') or 0) if cuota_mes else 0.0,
            'Estado Cuota Mes': 'PAGADO' if cuota_mes and cuota_mes.get('pagado') else ('PENDIENTE' if cuota_mes else 'N/A'),
=======
        filas.append({
            'Periodo': periodo,
            'Oficina': r['oficina_nombre'],
            'Estado Cobro': (st.get('estado') or 'PENDIENTE'),
            'Producto': r['material_nombre'],
            'Valor Unitario': r['valor_unitario'],
            '% Oficina': r['porcentaje_oficina'],
            'Cantidad Entregada': r['cantidad_total'],
            'Valor Total': r['valor_total'],
            'Valor a Cobrar Oficina': r['valor_cobro_oficina'],
            'Solicitudes (conteo)': r['num_solicitudes'],
>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
        })

    if not filas:
        flash('No hay datos para exportar en el periodo seleccionado', 'warning')
        return redirect(f'/reportes/cobros-pop?periodo={periodo}')

    if formato == 'excel':
        df = pd.DataFrame(filas)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Cobros_POP', index=False)
<<<<<<< HEAD
=======
            ws = writer.sheets['Cobros_POP']
            for col in ws.columns:
                max_len = 0
                letter = col[0].column_letter
                for cell in col:
                    try:
                        max_len = max(max_len, len(str(cell.value)))
                    except Exception:
                        pass
                ws.column_dimensions[letter].width = min(max_len + 2, 55)

            total_valor = float(df['Valor Total'].sum())
            total_cobro = float(df['Valor a Cobrar Oficina'].sum())
            resumen = pd.DataFrame({
                'Resumen': [
                    f'Periodo: {periodo}',
                    f'Oficina: {oficina_id if oficina_id else "Todas"}',
                    f'Fecha Generación: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                    f'Total Valor: {total_valor:,.2f}',
                    f'Total a Cobrar (Oficinas): {total_cobro:,.2f}',
                ]
            })
            resumen.to_excel(writer, sheet_name='Resumen', index=False)

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
        output.seek(0)
        fecha = datetime.now().strftime('%Y%m%d_%H%M%S')
        suf = f'_oficina_{oficina_id}' if oficina_id else ''
        filename = f'reporte_cobros_pop_{periodo}{suf}_{fecha}.xlsx'
<<<<<<< HEAD
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)

    try:
        import io
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), topMargin=0.5*inch, bottomMargin=0.5*inch, leftMargin=0.5*inch, rightMargin=0.5*inch)
        styles = getSampleStyleSheet()
        title_style = styles['Title']
        normal_style = styles['BodyText']
        small_style = ParagraphStyle('SmallCell', parent=normal_style, fontSize=7.5, leading=9)
        header_style = ParagraphStyle('HeaderCell', parent=styles['Heading5'], fontName='Helvetica-Bold', fontSize=8, leading=9, textColor=colors.white, alignment=1)

        story = [
            Paragraph(f'Reporte Cobros POP - {periodo}', title_style),
            Paragraph('El PDF muestra el avance del diferido: cuota actual y cuotas faltantes por solicitud.', normal_style),
            Spacer(1, 0.15*inch)
        ]

        data = [[
            Paragraph('Oficina', header_style),
            Paragraph('Solicitud', header_style),
            Paragraph('Estado<br/>cobro', header_style),
            Paragraph('Diferido', header_style),
            Paragraph('Inicio', header_style),
            Paragraph('Plan<br/>cuotas', header_style),
            Paragraph('Cuota<br/>actual', header_style),
            Paragraph('Faltan', header_style),
            Paragraph('Valor cuota<br/>mes', header_style),
            Paragraph('Estado cuota<br/>mes', header_style),
            Paragraph('Valor<br/>cobro', header_style),
        ]]

        for f in filas:
            data.append([
                Paragraph(str(f['Oficina']), small_style),
                Paragraph(str(f['SolicitudId']), small_style),
                Paragraph(str(f['Estado Cobro']), small_style),
                Paragraph(str(f['Tiene Diferido']), small_style),
                Paragraph(str(f['Inicio Diferido'] or ''), small_style),
                Paragraph(str(f"{int(f['No. Cuotas'] or 0)}"), small_style),
                Paragraph(str(f['Cuota Actual'] or ''), small_style),
                Paragraph(str(f['Cuotas Faltantes'] or 0), small_style),
                Paragraph(f"{float(f['Valor Cuota Mes']):,.2f}", small_style),
                Paragraph(str(f['Estado Cuota Mes']), small_style),
                Paragraph(f"{float(f['Valor a Cobrar Oficina']):,.2f}", small_style),
            ])

        col_widths = [1.45*inch, 0.72*inch, 0.78*inch, 0.6*inch, 0.7*inch, 0.62*inch, 0.9*inch, 0.55*inch, 0.95*inch, 0.92*inch, 0.9*inch]
        table = Table(data, repeatRows=1, colWidths=col_widths)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#4f5b66')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 8),
            ('FONTSIZE', (0,1), (-1,-1), 7.5),
            ('GRID', (0,0), (-1,-1), 0.25, colors.black),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('ALIGN', (1,1), (1,-1), 'CENTER'),
            ('ALIGN', (3,1), (7,-1), 'CENTER'),
            ('ALIGN', (8,1), (10,-1), 'RIGHT'),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.whitesmoke, colors.HexColor('#eef2f7')]),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        story.append(table)
        doc.build(story)
        buffer.seek(0)
=======
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename,
        )

    # PDF
    try:
        import io
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(letter),
            topMargin=0.5*inch,
            bottomMargin=0.5*inch,
            leftMargin=0.5*inch,
            rightMargin=0.5*inch,
        )

        styles = getSampleStyleSheet()
        story = []
        titulo = f'Reporte Cobros POP - {periodo}' + (f' (Oficina {oficina_id})' if oficina_id else '')
        story.append(Paragraph(titulo, styles['Title']))
        story.append(Spacer(1, 0.2*inch))

        headers = ['Oficina', 'Estado', 'Producto', 'V. Unit', '%', 'Cant', 'V. Total', 'V. Cobro']
        data = [headers]
        for f in filas:
            data.append([
                f['Oficina'],
                f['Estado Cobro'],
                f['Producto'],
                f"{float(f['Valor Unitario']):,.2f}",
                f"{float(f['% Oficina']):.2f}",
                str(f['Cantidad Entregada']),
                f"{float(f['Valor Total']):,.2f}",
                f"{float(f['Valor a Cobrar Oficina']):,.2f}",
            ])

        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.grey),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 9),
            ('FONTSIZE', (0,1), (-1,-1), 8),
            ('GRID', (0,0), (-1,-1), 0.25, colors.black),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('ALIGN', (3,1), (-1,-1), 'RIGHT'),
        ]))
        story.append(table)

        story.append(Spacer(1, 0.2*inch))
        total_valor = sum(float(x['Valor Total']) for x in filas)
        total_cobro = sum(float(x['Valor a Cobrar Oficina']) for x in filas)
        story.append(Paragraph(f'Total Valor: {total_valor:,.2f} | Total a Cobrar (Oficinas): {total_cobro:,.2f}', styles['Normal']))

        doc.build(story)
        buffer.seek(0)

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
        fecha = datetime.now().strftime('%Y%m%d_%H%M%S')
        suf = f'_oficina_{oficina_id}' if oficina_id else ''
        filename = f'reporte_cobros_pop_{periodo}{suf}_{fecha}.pdf'
        return send_file(buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)
<<<<<<< HEAD
    except Exception:
        flash('Error al generar el PDF de cobros', 'danger')
        return redirect(f'/reportes/cobros-pop?periodo={periodo}')
=======

    except Exception:
        flash('Error al generar el PDF de cobros', 'danger')
        return redirect(f'/reportes/cobros-pop?periodo={periodo}')

>>>>>>> 91ce8b42868ef3d49fe542f90b205d8d93e4f57e
