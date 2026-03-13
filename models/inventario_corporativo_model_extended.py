# models/inventario_corporativo_model_extended.py
import os
from utils.helpers import sanitizar_log_text
"""
Extensiones al modelo de inventario corporativo para soportar:
- AsignaciÃ³n a usuarios del Active Directory
- BÃºsqueda de usuarios AD
- Notificaciones por email
- Sistema de confirmaciÃ³n con tokens
"""
from database import get_database_connection
import logging

logger = logging.getLogger(__name__)


def _error_id() -> str:
    """Genera un identificador corto para correlación de errores sin exponer detalles."""
    return os.urandom(4).hex()



class InventarioCorporativoModelExtended:
    """
    MÃ©todos adicionales para el modelo de inventario corporativo.
    Estos mÃ©todos pueden ser agregados a la clase InventarioCorporativoModel existente.
    """
    
    @staticmethod
    def asignar_a_usuario_ad(producto_id, oficina_id, cantidad, usuario_ad_info, usuario_accion, serial_asignacion=None):
        """
        Asigna un producto a un usuario especÃ­fico del Active Directory.
        
        Args:
            producto_id: ID del producto a asignar
            oficina_id: ID de la oficina destino
            cantidad: Cantidad a asignar
            usuario_ad_info: Diccionario con informaciÃ³n del usuario AD
                - username: Nombre de usuario AD
                - full_name: Nombre completo
                - email: Correo electrÃ³nico
                - department: Departamento
            usuario_accion: Usuario que realiza la acciÃ³n
            
        Returns:
            dict: Resultado de la operaciÃ³n con 'success' y 'message'
        """
        conn = get_database_connection()
        if not conn:
            return {'success': False, 'message': 'Error de conexiÃ³n a la base de datos'}
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            # 1. Verificar stock disponible
            cursor.execute(
                "SELECT CantidadDisponible, NombreProducto FROM ProductosCorporativos "
                "WHERE ProductoId = ? AND Activo = 1",
                (int(producto_id),)
            )
            row = cursor.fetchone()
            if not row:
                return {'success': False, 'message': 'Producto no encontrado'}
            
            stock = int(row[0])
            nombre_producto = row[1]
            cant = int(cantidad)
            
            if cant <= 0:
                return {'success': False, 'message': 'La cantidad debe ser mayor a 0'}
            
            if cant > stock:
                return {'success': False, 'message': f'Stock insuficiente. Disponible: {stock}'}
            
            # 2. Buscar o crear usuario en la base de datos local
            usuario_asignado_id = InventarioCorporativoModelExtended._obtener_o_crear_usuario_ad(
                cursor, usuario_ad_info
            )
            
            if not usuario_asignado_id:
                return {'success': False, 'message': 'Error al procesar el usuario asignado'}
            
            serial_asignacion = ((serial_asignacion or '')).strip() or None

            # 3. Actualizar serial si llega informado y descontar stock
            if serial_asignacion:
                cursor.execute("""
                    UPDATE ProductosCorporativos
                    SET CantidadDisponible = CantidadDisponible - ?,
                        Serial = ?
                    WHERE ProductoId = ?
                """, (cant, serial_asignacion, int(producto_id)))
            else:
                cursor.execute("""
                    UPDATE ProductosCorporativos
                    SET CantidadDisponible = CantidadDisponible - ?
                    WHERE ProductoId = ?
                """, (cant, int(producto_id)))
            
            # 4. Crear registro en tabla Asignaciones con usuario AD
            cursor.execute("""
                INSERT INTO Asignaciones 
                (ProductoId, OficinaId, UsuarioAsignadoId, FechaAsignacion, 
                 Estado, UsuarioAsignador, Activo, UsuarioADNombre, UsuarioADEmail)
                VALUES (?, ?, ?, GETDATE(), 'ASIGNADO', ?, 1, ?, ?)
            """, (
                int(producto_id), 
                int(oficina_id), 
                usuario_asignado_id,
                usuario_accion,
                usuario_ad_info.get('full_name', usuario_ad_info.get('username', '')),
                usuario_ad_info.get('email', '')
            ))
            
            # 5. Registrar en historial con informaciÃ³n del usuario AD
            cursor.execute("""
                INSERT INTO AsignacionesCorporativasHistorial
                    (ProductoId, OficinaId, Accion, Cantidad, UsuarioAccion, 
                     Fecha, UsuarioAsignadoNombre, UsuarioAsignadoEmail)
                VALUES (?, ?, 'ASIGNAR', ?, ?, GETDATE(), ?, ?)
            """, (
                int(producto_id), 
                int(oficina_id), 
                cant, 
                usuario_accion,
                usuario_ad_info.get('full_name', usuario_ad_info.get('username', '')),
                usuario_ad_info.get('email', '')
            ))
            
            conn.commit()
            
            return {
                'success': True, 
                'message': 'Producto asignado correctamente',
                'usuario_email': usuario_ad_info.get('email'),
                'usuario_nombre': usuario_ad_info.get('full_name'),
                'producto_nombre': nombre_producto
            }
            
        except Exception as e:
            logger.error("Error asignar_a_usuario_ad: ref=%s", sanitizar_log_text(_error_id()))
            try:
                if conn: conn.rollback()
            except:
                pass
            return {'success': False, 'message': 'Error al asignar: Error interno'}
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
    
    @staticmethod
    def asignar_a_usuario_ad_con_confirmacion(producto_id, oficina_id, cantidad, 
                                               usuario_ad_info, usuario_accion, serial_asignacion=None):
        """
        Asigna un producto a un usuario del Active Directory y genera token de confirmaciÃ³n.
        
        Args:
            producto_id: ID del producto a asignar
            oficina_id: ID de la oficina destino
            cantidad: Cantidad a asignar
            usuario_ad_info: Diccionario con informaciÃ³n del usuario AD
                - username: Nombre de usuario AD
                - full_name: Nombre completo
                - email: Correo electrÃ³nico
                - department: Departamento
            usuario_accion: Usuario que realiza la acciÃ³n
            
        Returns:
            dict: Resultado de la operaciÃ³n con 'success', 'message', 'token', 'asignacion_id'
        """
        conn = get_database_connection()
        if not conn:
            return {'success': False, 'message': 'Error de conexiÃ³n a la base de datos'}
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            # 1. Verificar stock disponible
            cursor.execute(
                "SELECT CantidadDisponible, NombreProducto FROM ProductosCorporativos "
                "WHERE ProductoId = ? AND Activo = 1",
                (int(producto_id),)
            )
            row = cursor.fetchone()
            if not row:
                return {'success': False, 'message': 'Producto no encontrado'}
            
            stock = int(row[0])
            nombre_producto = row[1]
            cant = int(cantidad)
            
            if cant <= 0:
                return {'success': False, 'message': 'La cantidad debe ser mayor a 0'}
            
            if cant > stock:
                return {'success': False, 'message': f'Stock insuficiente. Disponible: {stock}'}
            
            # 2. Buscar o crear usuario en la base de datos local
            usuario_asignado_id = InventarioCorporativoModelExtended._obtener_o_crear_usuario_ad(
                cursor, usuario_ad_info
            )
            
            if not usuario_asignado_id:
                return {'success': False, 'message': 'Error al procesar el usuario asignado'}
            
            serial_asignacion = ((serial_asignacion or '')).strip() or None

            # 3. Actualizar serial si llega informado y descontar stock
            if serial_asignacion:
                cursor.execute("""
                    UPDATE ProductosCorporativos
                    SET CantidadDisponible = CantidadDisponible - ?,
                        Serial = ?
                    WHERE ProductoId = ?
                """, (cant, serial_asignacion, int(producto_id)))
            else:
                cursor.execute("""
                    UPDATE ProductosCorporativos
                    SET CantidadDisponible = CantidadDisponible - ?
                    WHERE ProductoId = ?
                """, (cant, int(producto_id)))
            
            # 4. Crear registro en tabla Asignaciones con usuario AD
            cursor.execute("""
                INSERT INTO Asignaciones 
                (ProductoId, OficinaId, UsuarioAsignadoId, FechaAsignacion, 
                 Estado, UsuarioAsignador, Activo, UsuarioADNombre, UsuarioADEmail)
                OUTPUT INSERTED.AsignacionId
                VALUES (?, ?, ?, GETDATE(), 'ASIGNADO', ?, 1, ?, ?)
            """, (
                int(producto_id), 
                int(oficina_id), 
                usuario_asignado_id,
                usuario_accion,
                usuario_ad_info.get('full_name', usuario_ad_info.get('username', '')),
                usuario_ad_info.get('email', '')
            ))
            
            # Obtener el ID de la asignaciÃ³n reciÃ©n creada
            asignacion_result = cursor.fetchone()
            if not asignacion_result:
                conn.rollback()
                return {'success': False, 'message': 'Error al crear la asignaciÃ³n'}
            
            asignacion_id = asignacion_result[0]
            
            # 5. Registrar en historial
            cursor.execute("""
                INSERT INTO AsignacionesCorporativasHistorial
                    (ProductoId, OficinaId, Accion, Cantidad, UsuarioAccion, 
                     Fecha, UsuarioAsignadoNombre, UsuarioAsignadoEmail)
                VALUES (?, ?, 'ASIGNAR', ?, ?, GETDATE(), ?, ?)
            """, (
                int(producto_id), 
                int(oficina_id), 
                cant, 
                usuario_accion,
                usuario_ad_info.get('full_name', usuario_ad_info.get('username', '')),
                usuario_ad_info.get('email', '')
            ))
            
            # Commit para que se guarde la asignaciÃ³n antes de generar el token
            conn.commit()
            
            # 6. Generar token de confirmación INLINE (evita problemas de importación)
            token = None
            usuario_email = usuario_ad_info.get('email')
            if usuario_email:
                try:
                    import secrets
                    import hashlib
                    from datetime import datetime, timedelta
                    
                    # Generar token único y seguro
                    token_raw = secrets.token_urlsafe(32)
                    token_hash = hashlib.sha256(token_raw.encode()).hexdigest()
                    fecha_expiracion = datetime.now() + timedelta(days=8)
                    
                    logger.info("[CONF] Generando código para asignacion %s", sanitizar_log_text(asignacion_id))                    # Eliminar códigos anteriores
                    cursor.execute("""
                        DELETE FROM TokensConfirmacionAsignacion 
                        WHERE AsignacionId = ?
                    """, (asignacion_id,))
                    
                    # Insertar nuevo token
                    cursor.execute("""
                        INSERT INTO TokensConfirmacionAsignacion 
                        (AsignacionId, Token, TokenHash, UsuarioEmail, FechaExpiracion, Utilizado, FechaCreacion)
                        VALUES (?, ?, ?, ?, ?, 0, GETDATE())
                    """, (asignacion_id, token_raw, token_hash, usuario_email, fecha_expiracion))
                    
                    conn.commit()
                    token = token_raw
                    logger.info("[CONF] Código generado exitosamente para asignación %s", sanitizar_log_text(asignacion_id))
                    
                except Exception as e:
                    logger.error("[CONF] Error generando código: ref=%s", sanitizar_log_text(_error_id()))
                    logger.error("[CONF] Error generando código")
            
            return {
                'success': True, 
                'message': 'Producto asignado correctamente',
                'asignacion_id': asignacion_id,
                'usuario_email': usuario_email,
                'usuario_nombre': usuario_ad_info.get('full_name'),
                'producto_nombre': nombre_producto,
                'token': token
            }
            
        except Exception as e:
            logger.error("Error asignar_a_usuario_ad_con_confirmacion: ref=%s", sanitizar_log_text(_error_id()))
            try:
                if conn: conn.rollback()
            except:
                pass
            return {'success': False, 'message': 'Error al asignar: Error interno'}
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
    
    @staticmethod
    def obtener_asignaciones_con_estado_confirmacion(producto_id=None):
        """
        Obtiene asignaciones con su estado de confirmaciÃ³n.
        
        Args:
            producto_id: ID del producto (opcional, para filtrar)
            
        Returns:
            list: Lista de asignaciones con estado de confirmaciÃ³n
        """
        conn = get_database_connection()
        if not conn:
            return []
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    a.AsignacionId,
                    a.ProductoId,
                    p.CodigoUnico,
                    p.NombreProducto,
                    c.NombreCategoria AS categoria,
                    o.NombreOficina AS oficina,
                    a.FechaAsignacion,
                    a.Estado,
                    a.FechaConfirmacion,
                    a.UsuarioConfirmacion,
                    a.UsuarioAsignador,
                    a.UsuarioADNombre,
                    a.UsuarioADEmail,
                    CASE 
                        WHEN t.Utilizado = 1 THEN 'CONFIRMADO'
                        WHEN t.FechaExpiracion < GETDATE() THEN 'EXPIRADO'
                        WHEN t.TokenId IS NOT NULL THEN 'PENDIENTE'
                        ELSE 'SIN_TOKEN'
                    END AS EstadoConfirmacion,
                    t.FechaExpiracion,
                    DATEDIFF(day, GETDATE(), t.FechaExpiracion) AS DiasRestantes
                FROM Asignaciones a
                INNER JOIN ProductosCorporativos p ON a.ProductoId = p.ProductoId
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Oficinas o ON a.OficinaId = o.OficinaId
                LEFT JOIN TokensConfirmacionAsignacion t ON a.AsignacionId = t.AsignacionId
                WHERE a.Activo = 1
            """
            
            if producto_id:
                query += " AND a.ProductoId = ?"
                cursor.execute(query + " ORDER BY a.FechaAsignacion DESC", (int(producto_id),))
            else:
                cursor.execute(query + " ORDER BY a.FechaAsignacion DESC")
            
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
            
        except Exception as e:
            logger.error("Error obteniendo asignaciones con confirmaciÃ³n: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
    
    @staticmethod
    def _obtener_o_crear_usuario_ad(cursor, usuario_ad_info):
        """
        Obtiene el ID del usuario en la base de datos local o lo crea si no existe.
        
        Args:
            cursor: Cursor de la base de datos
            usuario_ad_info: InformaciÃ³n del usuario AD
            
        Returns:
            int: ID del usuario o None si falla
        """
        try:
            username = usuario_ad_info.get('username', '')
            
            # Buscar usuario existente por nombre de usuario AD
            cursor.execute(
                "SELECT UsuarioId FROM Usuarios WHERE UsuarioAD = ? AND Activo = 1",
                (username,)
            )
            row = cursor.fetchone()
            
            if row:
                return row[0]
            
            # Si no existe, buscar por email
            email = usuario_ad_info.get('email', '')
            if email:
                cursor.execute(
                    "SELECT UsuarioId FROM Usuarios WHERE CorreoElectronico = ? AND Activo = 1",
                    (email,)
                )
                row = cursor.fetchone()
                if row:
                    # Actualizar el UsuarioAD
                    cursor.execute(
                        "UPDATE Usuarios SET UsuarioAD = ? WHERE UsuarioId = ?",
                        (username, row[0])
                    )
                    return row[0]
            
            # Si no existe, crear nuevo usuario
            cursor.execute("""
                INSERT INTO Usuarios 
                (NombreUsuario, NombreCompleto, Email, UsuarioAD, Rol, Activo, FechaCreacion)
                OUTPUT INSERTED.UsuarioId
                VALUES (?, ?, ?, ?, 'usuario', 1, GETDATE())
            """, (
                username,
                usuario_ad_info.get('full_name', username),
                email,
                username
            ))
            
            new_id = cursor.fetchone()
            return new_id[0] if new_id else None
            
        except Exception as e:
            logger.error("Error obteniendo/creando usuario AD: ref=%s", sanitizar_log_text(_error_id()))
            # Si falla, retornar el primer usuario activo como fallback
            cursor.execute(
                "SELECT TOP 1 UsuarioId FROM Usuarios WHERE Activo = 1 ORDER BY UsuarioId"
            )
            row = cursor.fetchone()
            return row[0] if row else None
    
    @staticmethod
    def obtener_asignaciones_por_usuario(usuario_ad_nombre):
        """
        Obtiene todas las asignaciones de un usuario especÃ­fico del AD.
        
        Args:
            usuario_ad_nombre: Nombre del usuario en AD
            
        Returns:
            list: Lista de asignaciones
        """
        conn = get_database_connection()
        if not conn:
            return []
        
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    a.AsignacionId,
                    p.ProductoId,
                    p.CodigoUnico,
                    p.NombreProducto,
                    c.NombreCategoria AS categoria,
                    o.NombreOficina AS oficina,
                    a.FechaAsignacion,
                    a.Estado,
                    a.UsuarioAsignador,
                    a.UsuarioADNombre,
                    a.UsuarioADEmail
                FROM Asignaciones a
                INNER JOIN ProductosCorporativos p ON a.ProductoId = p.ProductoId
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Oficinas o ON a.OficinaId = o.OficinaId
                WHERE a.UsuarioADNombre LIKE ? AND a.Activo = 1
                ORDER BY a.FechaAsignacion DESC
            """, (f'%{usuario_ad_nombre}%',))
            
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
            
        except Exception as e:
            logger.error("Error obteniendo asignaciones por usuario: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
    
    @staticmethod
    def historial_asignaciones_extendido(producto_id):
        """
        Obtiene el historial de asignaciones con informaciÃ³n extendida del usuario AD.
        
        Args:
            producto_id: ID del producto
            
        Returns:
            list: Lista de movimientos del historial
        """
        conn = get_database_connection()
        if not conn:
            return []
        
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    h.HistorialId,
                    h.ProductoId,
                    h.OficinaId,
                    o.NombreOficina AS oficina,
                    h.Accion,
                    h.Cantidad,
                    h.UsuarioAccion,
                    h.Fecha,
                    h.UsuarioAsignadoNombre,
                    h.UsuarioAsignadoEmail
                FROM AsignacionesCorporativasHistorial h
                LEFT JOIN Oficinas o ON o.OficinaId = h.OficinaId
                WHERE h.ProductoId = ?
                ORDER BY h.Fecha DESC
            """, (int(producto_id),))
            
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
            
        except Exception as e:
            logger.error("Error historial_asignaciones_extendido: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()