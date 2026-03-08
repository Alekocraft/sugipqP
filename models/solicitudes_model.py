# models/solicitudes_model.py
import logging
import os
from utils.helpers import sanitizar_log_text
logger = logging.getLogger(__name__)


def _error_id() -> str:
    """Genera un identificador corto para correlación de errores sin exponer detalles."""
    return os.urandom(4).hex()

from database import get_database_connection


class SolicitudModel:
    # ==========================
    # CREAR / APROBAR / RECHAZAR
    # ==========================

    @staticmethod
    def crear(oficina_id, material_id, cantidad_solicitada, porcentaje_oficina, usuario_nombre, observacion=""):
        conn = get_database_connection()
        if conn is None:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute(
                "{CALL sp_CrearSolicitud (?, ?, ?, ?, ?, ?)}",
                (oficina_id, material_id, cantidad_solicitada, porcentaje_oficina, usuario_nombre, observacion)
            )
            result = cursor.fetchone()
            conn.commit()
            return result[0] if result else None
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def crear_solicitud(material_id, cantidad_solicitada, usuario_solicitante, oficina_solicitante, porcentaje_oficina=100, observacion=""):
        """
        Wrapper de compatibilidad para crear una solicitud.

        Acepta usuario_solicitante como nombre visible o como identificador.
        Si recibe un texto, lo usa directamente para evitar guardar el correo
        como solicitante cuando en sesión ya existe el nombre mostrado.
        """
        usuario_nombre = ''

        if isinstance(usuario_solicitante, str):
            usuario_nombre = usuario_solicitante.strip()

        if not usuario_nombre:
            from models.usuarios_model import UsuarioModel
            usuario = UsuarioModel.obtener_por_id(usuario_solicitante)
            if usuario:
                usuario_nombre = (usuario.get('nombre') or usuario.get('usuario') or '').strip()

        if not usuario_nombre:
            usuario_nombre = f"Usuario_{usuario_solicitante}"

        return SolicitudModel.crear(
            oficina_id=oficina_solicitante,
            material_id=material_id,
            cantidad_solicitada=cantidad_solicitada,
            porcentaje_oficina=porcentaje_oficina,
            usuario_nombre=usuario_nombre,
            observacion=observacion
        )

    @staticmethod
    def aprobar(solicitud_id, usuario_aprobador_id):
        conn = get_database_connection()
        if conn is None:
            return False, "❌ Error de conexión a la base de datos"
        cursor = conn.cursor()
        try:
            # PRIMERO: Obtener informaciÃ³n de la solicitud
            cursor.execute("""
                SELECT sm.MaterialId, sm.CantidadSolicitada, sm.EstadoId,
                       m.ValorUnitario, m.CantidadDisponible, sm.PorcentajeOficina
                FROM dbo.SolicitudesMaterial sm
                INNER JOIN dbo.Materiales m ON sm.MaterialId = m.MaterialId
                WHERE sm.SolicitudId = ? AND sm.EstadoId = 1
            """, (solicitud_id,))
            
            solicitud_info = cursor.fetchone()
            
            if not solicitud_info:
                return False, "❌ Solicitud no encontrada o no estÃ¡ pendiente"
            
            material_id, cantidad_solicitada, estado_id, valor_unitario, stock_disponible, porcentaje_oficina = solicitud_info
            
            # VERIFICAR STOCK DISPONIBLE
            if cantidad_solicitada > stock_disponible:
                return False, f"❌ Stock insuficiente. Disponible: {stock_disponible}, Solicitado: {cantidad_solicitada}"
            
            # OBTENER APROBADOR ID
            aprobador_id = SolicitudModel._obtener_aprobador_id(usuario_aprobador_id)
            
            # CALCULAR VALORES FINANCIEROS
            valor_total_solicitado = valor_unitario * cantidad_solicitada
            valor_oficina = valor_total_solicitado * (porcentaje_oficina / 100)
            valor_sede_principal = valor_total_solicitado - valor_oficina
            
            # EJECUTAR APROBACIÃ“N COMPLETA
            cursor.execute("""
                BEGIN TRANSACTION;
                
                -- 1. APROBAR LA SOLICITUD
                UPDATE dbo.SolicitudesMaterial 
                SET EstadoId = 2, -- Aprobada
                    AprobadorId = ?,
                    FechaAprobacion = GETDATE(),
                    CantidadEntregada = ?,
                    ValorTotalSolicitado = ?,
                    ValorOficina = ?,
                    ValorSedePrincipal = ?,
                    FechaUltimaEntrega = GETDATE()
                WHERE SolicitudId = ? AND EstadoId = 1;
                
                -- 2. ACTUALIZAR STOCK (restar cantidad solicitada)
                UPDATE dbo.Materiales 
                SET CantidadDisponible = CantidadDisponible - ?
                WHERE MaterialId = ?;
                
                -- 3. REGISTRAR EN HISTORIAL DE ENTREGAS
                INSERT INTO dbo.HistorialEntregas (
                    SolicitudId, CantidadEntregada, UsuarioEntrega, Observaciones
                ) VALUES (?, ?, 'Sistema', 'AprobaciÃ³n completa');
                
                COMMIT TRANSACTION;
            """, (
                aprobador_id, cantidad_solicitada, valor_total_solicitado, 
                valor_oficina, valor_sede_principal, solicitud_id,
                cantidad_solicitada, material_id,
                solicitud_id, cantidad_solicitada
            ))
            
            conn.commit()
            return True, f"✅ Solicitud aprobada exitosamente. Stock actualizado: -{cantidad_solicitada} unidades"
            
        except Exception as e:
            conn.rollback()
            err = str(e)
            if "LÃ­mite mensual" in err:
                return False, "❌ LÃ­mite mensual excedido"
            if "Stock insuficiente" in err or "excede el inventario" in err:
                return False, "❌ Stock insuficiente"
            if "Solicitud no encontrada" in err:
                return False, "❌ Solicitud no encontrada"
            return False, "Error inesperado, contacte a soporte"
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def aprobar_parcial(solicitud_id, usuario_aprobador_id, cantidad_aprobada):
        conn = get_database_connection()
        if conn is None:
            return False, "Error de conexión"
        cursor = conn.cursor()
        try:
            aprobador_id = SolicitudModel._obtener_aprobador_id(usuario_aprobador_id)
            cursor.execute(
                "{CALL sp_AprobarParcialSolicitud (?, ?, ?)}",
                (solicitud_id, aprobador_id, cantidad_aprobada)
            )
            conn.commit()
            return True, f"✅ {cantidad_aprobada} unidades aprobadas y entregadas"
        except Exception as e:
            conn.rollback()
            err = str(e)
            if "Cantidad aprobada invÃ¡lida" in err:
                return False, "❌ Cantidad aprobada invÃ¡lida"
            if "solicitudes pendientes" in err:
                return False, "❌ Solo solicitudes pendientes"
            if "Solicitud no encontrada" in err:
                return False, "❌ Solicitud no encontrada"
            return False, "Error inesperado, contacte a soporte"
        finally:
            cursor.close()
            conn.close()
            
    @staticmethod
    def rechazar(solicitud_id, usuario_aprobador_id, observacion=""):
        conn = get_database_connection()
        if conn is None:
            return False, "❌ Error de conexión a la base de datos"
        cursor = conn.cursor()
        try:
            aprobador_id = SolicitudModel._obtener_aprobador_id(usuario_aprobador_id)
            cursor.execute(
                "{CALL sp_RechazarSolicitud (?, ?, ?)}",
                (solicitud_id, aprobador_id, observacion)
            )
            conn.commit()
            return True, "✓ Solicitud rechazada exitosamente"
        except Exception as e:
            conn.rollback()
            return False, "Error inesperado, contacte a soporte"
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def actualizar_estado_solicitud(solicitud_id, nuevo_estado_id):
        conn = get_database_connection()
        if conn is None:
            return False
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE SolicitudesMaterial SET EstadoId = ? WHERE SolicitudId = ?",
                (nuevo_estado_id, solicitud_id)
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception:
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    # ==========================
    # DEVOLUCIONES
    # ==========================

    @staticmethod
    def obtener_info_devolucion(solicitud_id):
        """
        - 1er SELECT: info de la solicitud, material, oficina, estado
        - 2do SELECT: suma de devoluciones COMPLETADAS
        - Se convierten TODAS las cantidades a int para que jsonify no explote.
        """
        conn = get_database_connection()
        if conn is None:
            return None

        cursor = conn.cursor()
        try:
            # 1) Info base de la solicitud (con imagen del material)
            cursor.execute("""
                SELECT 
                    sm.SolicitudId,
                    sm.EstadoId,
                    es.NombreEstado,
                    sm.CantidadSolicitada,
                    ISNULL(sm.CantidadEntregada, 0) AS CantidadEntregada,
                    m.NombreElemento,
                    o.NombreOficina,
                    sm.UsuarioSolicitante,
                    m.RutaImagen
                FROM dbo.SolicitudesMaterial sm
                INNER JOIN dbo.EstadosSolicitud es ON sm.EstadoId = es.EstadoId
                INNER JOIN dbo.Materiales m ON sm.MaterialId = m.MaterialId
                INNER JOIN dbo.Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
                WHERE sm.SolicitudId = ?
            """, (solicitud_id,))
            row = cursor.fetchone()
            if not row:
                return None

            solicitud_id_db = int(row[0])
            estado_id = int(row[1]) if row[1] is not None else 0
            estado = row[2]
            # convertir a int para evitar Decimal en JSON
            cantidad_solicitada = int(row[3]) if row[3] is not None else 0
            cantidad_entregada = int(row[4]) if row[4] is not None else 0
            material_nombre = row[5]
            oficina_nombre = row[6]
            solicitante_nombre = row[7]
            material_imagen = row[8]

            # 2) Total devuelto hasta ahora (solo devoluciones COMPLETADAS)
            cursor.execute("""
                SELECT ISNULL(SUM(CantidadDevuelta), 0)
                FROM dbo.Devoluciones
                WHERE SolicitudId = ? AND EstadoDevolucion = 'COMPLETADA'
            """, (solicitud_id_db,))
            row_dev = cursor.fetchone()
            cantidad_ya_devuelta = int(row_dev[0]) if row_dev and row_dev[0] is not None else 0

            cantidad_puede_devolver = cantidad_entregada - cantidad_ya_devuelta
            if cantidad_puede_devolver < 0:
                cantidad_puede_devolver = 0

            return {
                "solicitud_id": solicitud_id_db,
                "estado_id": estado_id,
                "estado": estado,
                "cantidad_solicitada": cantidad_solicitada,
                "cantidad_entregada": cantidad_entregada,
                "cantidad_ya_devuelta": cantidad_ya_devuelta,
                "cantidad_puede_devolver": cantidad_puede_devolver,
                "material_nombre": material_nombre,
                "oficina_nombre": oficina_nombre,
                "solicitante_nombre": solicitante_nombre,
                "material_imagen": material_imagen,
            }
        except Exception as e:
            logger.error("❌ ERROR en obtener_info_devolucion: ref=%s", sanitizar_log_text(_error_id()))
            return None
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def registrar_devolucion(solicitud_id, cantidad_devuelta, usuario_nombre, observacion=""):
        conn = get_database_connection()
        if conn is None:
            return False, "❌ Error de conexión"
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    sm.MaterialId,
                    sm.CantidadSolicitada,
                    sm.CantidadEntregada,
                    sm.EstadoId,
                    ISNULL(sm.CantidadEntregada,0) 
                      - ISNULL((SELECT SUM(d.CantidadDevuelta) FROM Devoluciones d WHERE d.SolicitudId = sm.SolicitudId), 0) AS CantidadPuedeDevolver
                FROM dbo.SolicitudesMaterial sm
                WHERE sm.SolicitudId = ?
            """, (solicitud_id,))
            row = cursor.fetchone()
            if not row:
                return False, "❌ Solicitud no encontrada"

            material_id = row[0]
            estado_id = row[3]
            cantidad_puede_devolver = row[4] or 0

            if estado_id not in (2, 4):
                return False, "❌ Solo se pueden devolver solicitudes aprobadas o entregadas"

            if cantidad_devuelta <= 0:
                return False, "❌ La cantidad a devolver debe ser mayor a 0"

            if cantidad_devuelta > cantidad_puede_devolver:
                return False, f"❌ No puede devolver mÃ¡s de {cantidad_puede_devolver} unidades"

            nueva_pendiente = cantidad_puede_devolver - cantidad_devuelta

            cursor.execute("""
                INSERT INTO Devoluciones (
                    SolicitudId, MaterialId, CantidadDevuelta, FechaDevolucion,
                    UsuarioDevolucion, Observaciones, EstadoDevolucion, CondicionMaterial
                )
                VALUES (?, ?, ?, GETDATE(), ?, ?, 'COMPLETADA', 'BUENO')
            """, (solicitud_id, material_id, cantidad_devuelta, usuario_nombre, observacion))

            cursor.execute("""
                UPDATE Materiales
                SET CantidadDisponible = CantidadDisponible + ?
                WHERE MaterialId = ?
            """, (cantidad_devuelta, material_id))

            if nueva_pendiente <= 0:
                cursor.execute("""
                    UPDATE SolicitudesMaterial
                    SET EstadoId = 5,
                        FechaUltimaEntrega = GETDATE()
                    WHERE SolicitudId = ?
                """, (solicitud_id,))
            else:
                cursor.execute("""
                    UPDATE SolicitudesMaterial
                    SET FechaUltimaEntrega = GETDATE()
                    WHERE SolicitudId = ?
                """, (solicitud_id,))

            conn.commit()
            return True, "✅ DevoluciÃ³n registrada exitosamente"
        except Exception as e:
            conn.rollback()
            return False, "Error inesperado, contacte a soporte"
        finally:
            cursor.close()
            conn.close()

    # ==========================
    # CONSULTAS LISTADO
    # ==========================

    @staticmethod
    def obtener_todas(estado=None, oficina=None, material=None, solicitante=None):
        """
        Obtiene todas las solicitudes con informaciÃ³n de novedades
        """
        conn = get_database_connection()
        if conn is None:
            return []
        
        cursor = conn.cursor()
        try:
            sql = """
                SELECT 
                    sm.SolicitudId,
                    sm.OficinaSolicitanteId,
                    sm.MaterialId,
                    sm.CantidadSolicitada,
                    sm.FechaSolicitud,
                    sm.EstadoId,
                    sm.PorcentajeOficina,
                    sm.AprobadorId,
                    sm.FechaAprobacion,
                    sm.CantidadEntregada,
                    sm.FechaUltimaEntrega,
                    sm.ValorTotalSolicitado,
                    sm.ValorOficina,
                    sm.ValorSedePrincipal,
                    sm.FechaCalculoValores,
                    sm.UsuarioSolicitante,
                    sm.Observacion,
                    sm.TieneNovedad,
                    o.NombreOficina as OficinaNombre,
                    m.NombreElemento as MaterialNombre,
                    es.NombreEstado as EstadoNombre,
                    a.NombreAprobador as AprobadorNombre,
                    -- Obtener informaciÃ³n de novedad si existe
                    ns.EstadoNovedad,
                    ns.TipoNovedad,
                    ns.Descripcion as NovedadDescripcion,
                    ns.CantidadAfectada,
                    -- Calcular cantidad devuelta
                    ISNULL((SELECT SUM(d.CantidadDevuelta) 
                           FROM Devoluciones d 
                           WHERE d.SolicitudId = sm.SolicitudId), 0) as CantidadDevuelta,
                    -- ✅ Agregar imagen del material y de la novedad
                    m.RutaImagen as MaterialImagen,
                    ns.RutaImagen as NovedadImagen
                FROM SolicitudesMaterial sm
                INNER JOIN Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
                INNER JOIN Materiales m ON sm.MaterialId = m.MaterialId
                INNER JOIN EstadosSolicitud es ON sm.EstadoId = es.EstadoId
                LEFT JOIN Aprobadores a ON sm.AprobadorId = a.AprobadorId
                LEFT JOIN NovedadesSolicitudes ns ON sm.SolicitudId = ns.SolicitudId 
                    AND ns.EstadoNovedad = 'registrada'
            """
            
            where_clauses = []
            params = []
            
            if estado and estado != 'todos':
                # Caso especial: filtrar todas las novedades (estados 7, 8 y 9)
                if estado == 'todas_novedades':
                    where_clauses.append("sm.EstadoId IN (7, 8, 9)")
                # Si el estado es un nombre (string no numÃ©rico), buscar por nombre de estado
                elif not str(estado).isdigit():
                    where_clauses.append("es.NombreEstado = ?")
                    params.append(estado)
                # Si es un nÃºmero, buscar por ID
                else:
                    where_clauses.append("sm.EstadoId = ?")
                    params.append(estado)
            
            if oficina and oficina != 'todas':
                where_clauses.append("o.NombreOficina = ?")
                params.append(oficina)
            
            if material:
                where_clauses.append("m.NombreElemento LIKE ?")
                params.append(f"%{material}%")
            
            if solicitante:
                where_clauses.append("sm.UsuarioSolicitante LIKE ?")
                params.append(f"%{solicitante}%")
            
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            
            sql += " ORDER BY sm.FechaSolicitud DESC"
            
            cursor.execute(sql, params)
            
            solicitudes = []
            for row in cursor.fetchall():
                solicitudes.append({
                    'solicitud_id': row[0],
                    'oficina_solicitante_id': row[1],
                    'material_id': row[2],
                    'cantidad_solicitada': row[3],
                    'fecha_solicitud': row[4],
                    'estado_id': row[5],
                    'porcentaje_oficina': row[6],
                    'aprobador_id': row[7],
                    'fecha_aprobacion': row[8],
                    'cantidad_entregada': row[9] or 0,
                    'fecha_ultima_entrega': row[10],
                    'valor_total_solicitado': row[11],
                    'valor_oficina': row[12],
                    'valor_sede_principal': row[13],
                    'fecha_calculo_valores': row[14],
                    'usuario_solicitante': row[15],
                    'observacion': row[16],
                    'tiene_novedad': bool(row[17]),
                    'oficina_nombre': row[18],
                    'material_nombre': row[19],
                    'estado_nombre': row[20],
                    'aprobador_nombre': row[21],
                    'estado_novedad': row[22],
                    'tipo_novedad': row[23],
                    'novedad_descripcion': row[24],
                    'cantidad_afectada': row[25] or 0,
                    'cantidad_devuelta': row[26] or 0,
                    # ✅ Agregar imÃ¡genes
                    'material_imagen': row[27],
                    'novedad_imagen': row[28]
                })
            
            return solicitudes
            
        except Exception as e:
            logger.info("❌ Error obteniendo solicitudes: ref=%s", sanitizar_log_text(_error_id()))
            logger.error("Excepción en solicitudes_model")
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def obtener_todas_ordenadas(oficina_id=None):
        conn = get_database_connection()
        if conn is None:
            return []
        cursor = conn.cursor()
        try:
            sql = """
                SELECT 
                    sm.SolicitudId,
                    m.NombreElemento,
                    sm.UsuarioSolicitante,
                    o.NombreOficina,
                    sm.OficinaSolicitanteId,
                    sm.CantidadSolicitada,
                    es.NombreEstado,
                    sm.FechaSolicitud,
                    sm.Observacion,
                    sm.MaterialId,
                    sm.PorcentajeOficina,
                    sm.ValorTotalSolicitado,
                    sm.ValorOficina,
                    sm.ValorSedePrincipal,
                    m.ValorUnitario,
                    m.CantidadDisponible,
                    sm.FechaAprobacion,
                    sm.CantidadEntregada
                FROM dbo.SolicitudesMaterial sm
                INNER JOIN dbo.Materiales m ON sm.MaterialId = m.MaterialId
                INNER JOIN dbo.Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
                INNER JOIN dbo.EstadosSolicitud es ON sm.EstadoId = es.EstadoId
            """
            params = ()
            if oficina_id:
                sql += " WHERE sm.OficinaSolicitanteId = ?"
                params = (oficina_id,)
            sql += " ORDER BY sm.FechaSolicitud DESC"
            cursor.execute(sql, params)
            return SolicitudModel._mapear_solicitudes(cursor.fetchall())
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def obtener_por_id(solicitud_id):
        conn = get_database_connection()
        if conn is None:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    sm.SolicitudId,
                    m.NombreElemento,
                    sm.UsuarioSolicitante,
                    o.NombreOficina,
                    sm.OficinaSolicitanteId,
                    sm.CantidadSolicitada,
                    es.NombreEstado,
                    sm.FechaSolicitud,
                    sm.Observacion,
                    sm.MaterialId,
                    sm.PorcentajeOficina,
                    sm.ValorTotalSolicitado,
                    sm.ValorOficina,
                    sm.ValorSedePrincipal,
                    m.ValorUnitario,
                    m.CantidadDisponible,
                    sm.FechaAprobacion,
                    sm.CantidadEntregada,
                    m.RutaImagen as MaterialImagen
                FROM dbo.SolicitudesMaterial sm
                INNER JOIN dbo.Materiales m ON sm.MaterialId = m.MaterialId
                INNER JOIN dbo.Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
                INNER JOIN dbo.EstadosSolicitud es ON sm.EstadoId = es.EstadoId
                WHERE sm.SolicitudId = ?
            """, (solicitud_id,))
            rows = cursor.fetchall()
            if rows:
                resultado = SolicitudModel._mapear_solicitudes(rows)[0]
                # ✅ Agregar imagen del material
                resultado['material_imagen'] = rows[0][18] if len(rows[0]) > 18 else None
                return resultado
            return None
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def obtener_para_aprobador(oficina_id=None):
        conn = get_database_connection()
        if conn is None:
            return []
        cursor = conn.cursor()
        try:
            sql = """
                SELECT 
                    sm.SolicitudId,
                    m.NombreElemento,
                    sm.UsuarioSolicitante,
                    o.NombreOficina,
                    sm.OficinaSolicitanteId,
                    sm.CantidadSolicitada,
                    es.NombreEstado,
                    sm.FechaSolicitud,
                    sm.Observacion,
                    sm.MaterialId,
                    sm.PorcentajeOficina,
                    sm.ValorTotalSolicitado,
                    sm.ValorOficina,
                    sm.ValorSedePrincipal,
                    m.ValorUnitario,
                    m.CantidadDisponible,
                    sm.FechaAprobacion,
                    sm.CantidadEntregada
                FROM dbo.SolicitudesMaterial sm
                INNER JOIN dbo.Materiales m ON sm.MaterialId = m.MaterialId
                INNER JOIN dbo.Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
                INNER JOIN dbo.EstadosSolicitud es ON sm.EstadoId = es.EstadoId
                WHERE sm.EstadoId = 1
            """
            params = ()
            if oficina_id:
                sql += " AND sm.OficinaSolicitanteId = ?"
                params = (oficina_id,)
            cursor.execute(sql, params)
            return SolicitudModel._mapear_solicitudes(cursor.fetchall())
        except Exception:
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def obtener_devoluciones(solicitud_id):
        conn = get_database_connection()
        if conn is None:
            return []
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    d.DevolucionId,
                    d.SolicitudId,
                    d.UsuarioDevolucion,
                    d.CantidadDevuelta,
                    d.FechaDevolucion,
                    d.Observaciones
                FROM Devoluciones d
                WHERE d.SolicitudId = ?
                ORDER BY d.FechaDevolucion DESC
            """, (solicitud_id,))
            devoluciones = []
            for row in cursor.fetchall():
                devoluciones.append({
                    "devolucion_id": row[0],
                    "solicitud_id": row[1],
                    "usuario_nombre": row[2],
                    "cantidad_devuelta": row[3],
                    "fecha_devolucion": row[4],
                    "observacion": row[5] or ""
                })
            return devoluciones
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def puede_devolver(solicitud_id):
        info = SolicitudModel.obtener_info_devolucion(solicitud_id)
        if not info:
            return False, "Solicitud no encontrada", None
        if info["estado_id"] not in (2, 4):
            return False, "Solo se pueden devolver solicitudes aprobadas o entregadas", info
        if info["cantidad_puede_devolver"] <= 0:
            return False, "No hay cantidad disponible para devolver", info
        return True, "Puede devolver", info

    # ==========================
    # PRIVADOS
    # ==========================

    @staticmethod
    def _obtener_aprobador_id(usuario_id):
        """Obtiene el ID del aprobador. Si el usuario no tiene aprobador asignado, usa su propio ID."""
        conn = get_database_connection()
        if conn is None:
            return usuario_id or 1  # Fallback al ID del usuario o 1
    
        cursor = conn.cursor()
        try:
            # Primero buscar si el usuario es aprobador
            cursor.execute("""
                SELECT u.UsuarioId, u.AprobadorId, a.AprobadorId as EsAprobador
                FROM Usuarios u
                LEFT JOIN Aprobadores a ON u.UsuarioId = a.AprobadorId
                WHERE u.UsuarioId = ?
            """, (usuario_id,))
        
            row = cursor.fetchone()
        
            if row:
                usuario_db = row[0]
                aprobador_asignado = row[1]
                es_aprobador = row[2]
            
                # Si el usuario tiene un aprobador asignado en su registro, usarlo
                if aprobador_asignado:
                    return aprobador_asignado
                # Si el usuario existe en la tabla Aprobadores, es un aprobador
                elif es_aprobador:
                    return usuario_db  # Es aprobador, usar su propio ID
                # Si no es aprobador pero necesita aprobar, usar un aprobador por defecto
                else:
                    # Buscar un aprobador por defecto (primero en la tabla Aprobadores)
                    cursor.execute("SELECT TOP 1 AprobadorId FROM Aprobadores WHERE Activo = 1 ORDER BY AprobadorId")
                    aprobador_default = cursor.fetchone()
                    if aprobador_default:
                        return aprobador_default[0]
                    else:
                        return usuario_db  # Fallback al ID del usuario
        
            # Si no encuentra al usuario, retornar un valor por defecto
            logger.info("⚠️ Usuario %s no encontrado. Usando aprobador por defecto.", sanitizar_log_text(usuario_id))
            cursor.execute("SELECT TOP 1 AprobadorId FROM Aprobadores WHERE Activo = 1 ORDER BY AprobadorId")
            aprobador_default = cursor.fetchone()
            return aprobador_default[0] if aprobador_default else 1
        
        except Exception as e:
            logger.info("❌ Error obteniendo aprobador_id: ref=%s", sanitizar_log_text(_error_id()))
            logger.error("Excepción en solicitudes_model")
            return usuario_id or 1  # Fallback seguro
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def _mapear_solicitudes(rows):
        solicitudes = []
        for row in rows:
            solicitudes.append({
                "id": row[0],
                "material_nombre": row[1],
                "usuario_solicitante": row[2],
                "oficina_nombre": row[3],
                "oficina_id": row[4],
                "cantidad_solicitada": row[5],
                "estado": row[6],
                "fecha_solicitud": row[7],
                "observacion": row[8] or "",
                "material_id": row[9],
                "porcentaje_oficina": float(row[10]) if row[10] is not None else 0.0,
                "valor_total_solicitado": float(row[11]) if row[11] is not None else 0.0,
                "valor_oficina": float(row[12]) if row[12] is not None else 0.0,
                "valor_sede": float(row[13]) if row[13] is not None else 0.0,
                "valor_unitario": float(row[14]) if row[14] is not None else 0.0,
                "stock_disponible": row[15] or 0,
                "fecha_aprobacion": row[16],
                "cantidad_entregada": row[17] or 0,
            })
        return solicitudes

    # ==========================
    # MÃ‰TODOS ADICIONALES
    # ==========================

    @staticmethod
    def obtener_estadisticas_por_material(material_id):
        """Obtiene estadÃ­sticas de solicitudes para un material especÃ­fico"""
        conn = get_database_connection()
        if conn is None:
            return [0, 0, 0, 0, 0, 0, 0]
        
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_solicitudes,
                    SUM(CASE WHEN EstadoId = 2 THEN 1 ELSE 0 END) as aprobadas,
                    SUM(CASE WHEN EstadoId = 1 THEN 1 ELSE 0 END) as pendientes,
                    SUM(ISNULL(CantidadEntregada, 0)) as total_entregado,
                    SUM(CASE WHEN EstadoId = 5 THEN 1 ELSE 0 END) as devueltas,
                    SUM(CASE WHEN EstadoId = 3 THEN 1 ELSE 0 END) as rechazadas,
                    SUM(CASE WHEN TieneNovedad = 1 THEN 1 ELSE 0 END) as con_novedad
                FROM SolicitudesMaterial
                WHERE MaterialId = ?
            """, (material_id,))
            
            row = cursor.fetchone()
            if row:
                return [
                    int(row[0] or 0),
                    int(row[1] or 0),
                    int(row[2] or 0),
                    int(row[3] or 0),
                    int(row[4] or 0),
                    int(row[5] or 0),
                    int(row[6] or 0)
                ]
            return [0, 0, 0, 0, 0, 0, 0]
            
        except Exception as e:
            logger.info("Error obteniendo estadÃ­sticas para material {material_id}: ref=%s", sanitizar_log_text(_error_id()))
            return [0, 0, 0, 0, 0, 0, 0]
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def obtener_por_nombre(nombre):
        conn = get_database_connection()
        if conn is None:
            return None
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT OficinaId, NombreOficina, DirectorOficina, Ubicacion, 
                       EsPrincipal, Activo, FechaCreacion, Email
                FROM Oficinas
                WHERE UPPER(NombreOficina) = UPPER(?)
            """, (nombre,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'nombre': row[1],
                    'director': row[2],
                    'ubicacion': row[3],
                    'es_principal': bool(row[4]) if row[4] is not None else False,
                    'activo': bool(row[5]) if row[5] is not None else True,
                    'fecha_creacion': row[6],
                    'email': row[7]
                }
            return None
        except Exception as e:
            logger.info("Error obteniendo oficina por nombre: ref=%s", sanitizar_log_text(_error_id()))
            return None
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def obtener_todas_con_detalle():
        """Obtiene todas las solicitudes con detalles completos"""
        conn = get_database_connection()
        if not conn:
            return []
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    sm.SolicitudId,
                    sm.OficinaSolicitanteId,
                    o.NombreOficina,
                    sm.MaterialId,
                    m.NombreElemento,
                    sm.CantidadSolicitada,
                    sm.CantidadEntregada,
                    sm.FechaSolicitud,
                    sm.EstadoId,
                    es.NombreEstado,
                    sm.AprobadorId,
                    a.NombreAprobador,
                    sm.FechaAprobacion,
                    sm.PorcentajeOficina,
                    sm.UsuarioSolicitante,
                    sm.Observacion,
                    sm.ValorTotalSolicitado,
                    sm.ValorOficina,
                    sm.ValorSedePrincipal
                FROM SolicitudesMaterial sm
                INNER JOIN Oficinas o ON sm.OficinaSolicitanteId = o.OficinaId
                INNER JOIN Materiales m ON sm.MaterialId = m.MaterialId
                INNER JOIN EstadosSolicitud es ON sm.EstadoId = es.EstadoId
                LEFT JOIN Aprobadores a ON sm.AprobadorId = a.AprobadorId
                ORDER BY sm.FechaSolicitud DESC
            """)
            
            columns = [column[0] for column in cursor.description]
            solicitudes = []
            for row in cursor.fetchall():
                solicitud = dict(zip(columns, row))
                # Renombrar campos para consistencia
                solicitud['id'] = solicitud.pop('SolicitudId')
                solicitud['oficina_id'] = solicitud.pop('OficinaSolicitanteId')
                solicitud['oficina_nombre'] = solicitud.pop('NombreOficina')
                solicitud['material_id'] = solicitud.pop('MaterialId')
                solicitud['material_nombre'] = solicitud.pop('NombreElemento')
                solicitud['cantidad_solicitada'] = solicitud.pop('CantidadSolicitada')
                solicitud['cantidad_entregada'] = solicitud.pop('CantidadEntregada')
                solicitud['fecha_solicitud'] = solicitud.pop('FechaSolicitud')
                solicitud['estado_id'] = solicitud.pop('EstadoId')
                solicitud['estado'] = solicitud.pop('NombreEstado')
                solicitud['aprobador_id'] = solicitud.pop('AprobadorId')
                solicitud['aprobador_nombre'] = solicitud.pop('NombreAprobador')
                solicitud['fecha_aprobacion'] = solicitud.pop('FechaAprobacion')
                solicitud['porcentaje_oficina'] = solicitud.pop('PorcentajeOficina')
                solicitud['usuario_solicitante'] = solicitud.pop('UsuarioSolicitante')
                solicitud['observacion'] = solicitud.pop('Observacion')
                solicitudes.append(solicitud)
            
            return solicitudes
            
        except Exception as e:
            logger.info("Error obteniendo solicitudes con detalle: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # ==========================
    # DEVOLUCIONES CON APROBACIÃ“N
    # ==========================

    @staticmethod
    def solicitar_devolucion(solicitud_id, cantidad_devuelta, usuario_solicita, motivo="", ruta_imagen=None):
        """
        Registra una SOLICITUD de devolución (pendiente de aprobaciÃ³n).
        No modifica el stock hasta que sea aprobada.
        """
        conn = get_database_connection()
        if conn is None:
            return False, "❌ Error de conexión a la base de datos"
        
        cursor = conn.cursor()
        try:
            # Validar la solicitud
            cursor.execute("""
                SELECT 
                    sm.MaterialId,
                    sm.CantidadEntregada,
                    sm.EstadoId,
                    ISNULL(sm.CantidadEntregada, 0) 
                      - ISNULL((SELECT SUM(d.CantidadDevuelta) FROM Devoluciones d 
                                WHERE d.SolicitudId = sm.SolicitudId AND d.EstadoDevolucion = 'COMPLETADA'), 0) AS CantidadPuedeDevolver
                FROM dbo.SolicitudesMaterial sm
                WHERE sm.SolicitudId = ?
            """, (solicitud_id,))
            row = cursor.fetchone()
            
            if not row:
                return False, "❌ Solicitud no encontrada"
            
            material_id = row[0]
            estado_id = row[2]
            cantidad_puede_devolver = row[3] or 0
            
            # Validar estado (solo aprobadas o entregadas parciales)
            if estado_id not in (2, 4, 5):  # Aprobada, Entregada Parcial, Completada
                return False, "❌ Solo se pueden devolver solicitudes aprobadas o entregadas"
            
            if cantidad_devuelta <= 0:
                return False, "❌ La cantidad a devolver debe ser mayor a 0"
            
            if cantidad_devuelta > cantidad_puede_devolver:
                return False, f"❌ No puede devolver mÃ¡s de {cantidad_puede_devolver} unidades"
            
            # Verificar que no haya otra devolución pendiente
            cursor.execute("""
                SELECT COUNT(*) FROM Devoluciones 
                WHERE SolicitudId = ? AND EstadoDevolucion = 'PENDIENTE'
            """, (solicitud_id,))
            pendientes = cursor.fetchone()[0]
            
            if pendientes > 0:
                return False, "❌ Ya existe una solicitud de devolución pendiente para esta solicitud"
            
            # Insertar solicitud de devolución con estado PENDIENTE
            cursor.execute("""
                INSERT INTO Devoluciones (
                    SolicitudId, MaterialId, CantidadDevuelta, FechaDevolucion,
                    UsuarioDevolucion, Observaciones, EstadoDevolucion, CondicionMaterial, RutaImagen
                )
                VALUES (?, ?, ?, GETDATE(), ?, ?, 'PENDIENTE', 'BUENO', ?)
            """, (solicitud_id, material_id, cantidad_devuelta, usuario_solicita, motivo, ruta_imagen))
            
            conn.commit()
            return True, "✅ Solicitud de devolución registrada. Pendiente de aprobaciÃ³n."
            
        except Exception as e:
            conn.rollback()
            logger.info("❌ Error en solicitud de devolución: ref=%s", sanitizar_log_text(_error_id()))
            logger.error("Excepción en solicitudes_model")
            return False, "Error inesperado, contacte a soporte"
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def aprobar_devolucion(devolucion_id, usuario_aprueba, observaciones=""):
        """
        Aprueba una devolución y actualiza el stock.
        """
        conn = get_database_connection()
        if conn is None:
            return False, "❌ Error de conexión a la base de datos"
        
        cursor = conn.cursor()
        try:
            # Obtener informaciÃ³n de la devolución
            cursor.execute("""
                SELECT d.SolicitudId, d.MaterialId, d.CantidadDevuelta, d.EstadoDevolucion
                FROM Devoluciones d
                WHERE d.DevolucionId = ?
            """, (devolucion_id,))
            row = cursor.fetchone()
            
            if not row:
                return False, "❌ DevoluciÃ³n no encontrada"
            
            solicitud_id, material_id, cantidad_devuelta, estado = row
            
            if estado != 'PENDIENTE':
                return False, f"❌ Esta devolución ya fue procesada (estado: {estado})"
            
            # Actualizar estado de la devolución a COMPLETADA
            cursor.execute("""
                UPDATE Devoluciones 
                SET EstadoDevolucion = 'COMPLETADA',
                    FechaAprobacion = GETDATE(),
                    UsuarioAprobador = ?,
                    ObservacionesAprobacion = ?
                WHERE DevolucionId = ?
            """, (usuario_aprueba, observaciones, devolucion_id))
            
            # Actualizar stock del material
            cursor.execute("""
                UPDATE Materiales
                SET CantidadDisponible = CantidadDisponible + ?
                WHERE MaterialId = ?
            """, (cantidad_devuelta, material_id))
            
            # Verificar si se devolviÃ³ todo lo entregado
            cursor.execute("""
                SELECT 
                    sm.CantidadEntregada,
                    ISNULL((SELECT SUM(d.CantidadDevuelta) FROM Devoluciones d 
                            WHERE d.SolicitudId = sm.SolicitudId AND d.EstadoDevolucion = 'COMPLETADA'), 0) as TotalDevuelto
                FROM SolicitudesMaterial sm
                WHERE sm.SolicitudId = ?
            """, (solicitud_id,))
            row = cursor.fetchone()
            
            if row:
                cantidad_entregada = row[0] or 0
                total_devuelto = row[1] or 0
                
                # Si se devolviÃ³ todo, cambiar estado a "Devuelta"
                if total_devuelto >= cantidad_entregada:
                    cursor.execute("""
                        UPDATE SolicitudesMaterial
                        SET EstadoId = 6, FechaUltimaEntrega = GETDATE()
                        WHERE SolicitudId = ?
                    """, (solicitud_id,))
            
            conn.commit()
            return True, f"✅ DevoluciÃ³n aprobada. Se reintegraron {cantidad_devuelta} unidades al inventario."
            
        except Exception as e:
            conn.rollback()
            logger.info("❌ Error aprobando devolución: ref=%s", sanitizar_log_text(_error_id()))
            logger.error("Excepción en solicitudes_model")
            return False, "Error inesperado, contacte a soporte"
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def rechazar_devolucion(devolucion_id, usuario_rechaza, observaciones=""):
        """
        Rechaza una solicitud de devolución.
        """
        conn = get_database_connection()
        if conn is None:
            return False, "❌ Error de conexión a la base de datos"
        
        cursor = conn.cursor()
        try:
            # Verificar estado actual
            cursor.execute("""
                SELECT EstadoDevolucion FROM Devoluciones WHERE DevolucionId = ?
            """, (devolucion_id,))
            row = cursor.fetchone()
            
            if not row:
                return False, "❌ DevoluciÃ³n no encontrada"
            
            if row[0] != 'PENDIENTE':
                return False, f"❌ Esta devolución ya fue procesada (estado: {row[0]})"
            
            # Actualizar estado a RECHAZADA
            cursor.execute("""
                UPDATE Devoluciones 
                SET EstadoDevolucion = 'RECHAZADA',
                    FechaAprobacion = GETDATE(),
                    UsuarioAprobador = ?,
                    ObservacionesAprobacion = ?
                WHERE DevolucionId = ?
            """, (usuario_rechaza, observaciones, devolucion_id))
            
            conn.commit()
            return True, "✅ DevoluciÃ³n rechazada"
            
        except Exception as e:
            conn.rollback()
            logger.info("❌ Error rechazando devolución: ref=%s", sanitizar_log_text(_error_id()))
            return False, "Error inesperado, contacte a soporte"
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def obtener_devolucion_pendiente(solicitud_id):
        """
        Obtiene la devolución pendiente de aprobaciÃ³n para una solicitud.
        """
        conn = get_database_connection()
        if conn is None:
            return None
        
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    d.DevolucionId,
                    d.SolicitudId,
                    d.MaterialId,
                    d.CantidadDevuelta,
                    d.FechaDevolucion,
                    d.UsuarioDevolucion,
                    d.Observaciones,
                    d.EstadoDevolucion,
                    d.RutaImagen,
                    m.NombreElemento as MaterialNombre,
                    m.RutaImagen as MaterialImagen
                FROM Devoluciones d
                INNER JOIN Materiales m ON d.MaterialId = m.MaterialId
                WHERE d.SolicitudId = ? AND d.EstadoDevolucion = 'PENDIENTE'
                ORDER BY d.FechaDevolucion DESC
            """, (solicitud_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'devolucion_id': row[0],
                    'solicitud_id': row[1],
                    'material_id': row[2],
                    'cantidad_devuelta': row[3],
                    'fecha_devolucion': row[4].strftime('%d/%m/%Y %H:%M') if row[4] else '',
                    'usuario_solicita': row[5],
                    'motivo': row[6] or '',
                    'estado': row[7],
                    'devolucion_imagen': row[8],
                    'material_nombre': row[9],
                    'material_imagen': row[10]
                }
            return None
            
        except Exception as e:
            logger.info("❌ Error obteniendo devolución pendiente: ref=%s", sanitizar_log_text(_error_id()))
            return None
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def tiene_devolucion_pendiente(solicitud_id):
        """Verifica si una solicitud tiene devolución pendiente de aprobaciÃ³n"""
        conn = get_database_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM Devoluciones 
                WHERE SolicitudId = ? AND EstadoDevolucion = 'PENDIENTE'
            """, (solicitud_id,))
            return cursor.fetchone()[0] > 0
        except:
            return False
        finally:
            cursor.close()
            conn.close()