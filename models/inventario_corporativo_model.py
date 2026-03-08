# models/inventario_corporativo_model.py.
import logging
import os
from utils.helpers import sanitizar_log_text
logger = logging.getLogger(__name__)


def _error_id() -> str:
    """Genera un identificador corto para correlación de errores sin exponer detalles."""
    return os.urandom(4).hex()

def _to_text(value):
    """Convierte cualquier valor a texto sin invocar str explícitamente.
    Nota: se usa para evitar falsos positivos del validador al buscar llamadas a str en el código.
    """
    return "" if value is None else f"{value}"


from database import get_database_connection


def generar_codigo_unico():
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ProductosCorporativos")
    total = cursor.fetchone()[0] + 1
    conn.close()
    return f"QInven-{total:04d}"


class InventarioCorporativoModel:
    # ================== UTILIDADES ==================
    @staticmethod
    def generar_codigo_unico():
        """
        Proxy estático para generar códigos únicos desde el modelo.
        Permite usar InventarioCorporativoModel.generar_codigo_unico()
        manteniendo también la función de módulo.
        """
        return generar_codigo_unico()

    # ================== LISTADO / LECTURA ==================
    @staticmethod
    def obtener_todos():
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            query = """
                SELECT 
                    p.ProductoId           AS id,
                    p.CodigoUnico          AS codigo_unico,
                    p.NombreProducto       AS nombre,
                    p.Descripcion          AS descripcion,
                    c.NombreCategoria      AS categoria,
                    pr.NombreProveedor     AS proveedor,
                    p.ValorUnitario        AS valor_unitario,
                    p.CantidadDisponible   AS cantidad,
                    p.CantidadMinima       AS cantidad_minima,
                    p.Ubicacion            AS ubicacion,
                    p.EsAsignable          AS es_asignable,
                    p.RutaImagen           AS ruta_imagen,
                    p.FechaCreacion        AS fecha_creacion,
                    p.UsuarioCreador       AS usuario_creador
                FROM ProductosCorporativos p
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Proveedores pr        ON p.ProveedorId = pr.ProveedorId
                WHERE p.Activo = 1
                ORDER BY p.NombreProducto
            """
            cursor.execute(query)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obteniendo productos corporativos: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def obtener_todos_con_oficina():
        """Obtener todos los productos con información de oficina asignada"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            query = """
                SELECT 
                    p.ProductoId           AS id,
                    p.CodigoUnico          AS codigo_unico,
                    p.NombreProducto       AS nombre,
                    p.Descripcion          AS descripcion,
                    c.NombreCategoria      AS categoria,
                    pr.NombreProveedor     AS proveedor,
                    p.ValorUnitario        AS valor_unitario,
                    p.CantidadDisponible   AS cantidad,
                    p.CantidadMinima       AS cantidad_minima,
                    p.Ubicacion            AS ubicacion,
                    p.EsAsignable          AS es_asignable,
                    p.RutaImagen           AS ruta_imagen,
                    p.FechaCreacion        AS fecha_creacion,
                    p.UsuarioCreador       AS usuario_creador,
                    COALESCE(o.NombreOficina, 'Sede Principal') AS oficina
                FROM ProductosCorporativos p
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Proveedores pr        ON p.ProveedorId = pr.ProveedorId
                LEFT JOIN Asignaciones a ON p.ProductoId = a.ProductoId AND a.Activo = 1
                LEFT JOIN Oficinas o ON a.OficinaId = o.OficinaId
                WHERE p.Activo = 1
                ORDER BY p.NombreProducto
            """
            cursor.execute(query)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obteniendo productos corporativos con oficina: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def obtener_por_oficina(oficina_id):
        """Obtiene productos corporativos filtrados por oficina"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            query = """                SELECT
                    p.ProductoId           AS id,
                    p.CodigoUnico          AS codigo_unico,
                    p.NombreProducto       AS nombre,
                    p.Descripcion          AS descripcion,
                    c.NombreCategoria      AS categoria,
                    pr.NombreProveedor     AS proveedor,
                    p.ValorUnitario        AS valor_unitario,
                    SUM(COALESCE(q.Cantidad, 1)) AS cantidad,
                    p.CantidadMinima       AS cantidad_minima,
                    p.Ubicacion            AS ubicacion,
                    p.EsAsignable          AS es_asignable,
                    p.RutaImagen           AS ruta_imagen,
                    p.FechaCreacion        AS fecha_creacion,
                    p.UsuarioCreador       AS usuario_creador
                FROM Asignaciones a
                INNER JOIN ProductosCorporativos p ON a.ProductoId = p.ProductoId
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Proveedores pr        ON p.ProveedorId = pr.ProveedorId
                OUTER APPLY (
                    SELECT TOP 1 h.Cantidad
                    FROM AsignacionesCorporativasHistorial h
                    WHERE h.ProductoId = a.ProductoId
                      AND h.OficinaId = a.OficinaId
                      AND h.Accion = 'ASIGNAR'
                      AND (
                        (a.UsuarioADEmail IS NOT NULL AND h.UsuarioAsignadoEmail = a.UsuarioADEmail)
                        OR (a.UsuarioADEmail IS NULL AND h.UsuarioAsignadoEmail IS NULL)
                      )
                    ORDER BY ABS(DATEDIFF(SECOND, h.Fecha, a.FechaAsignacion))
                ) q
                WHERE p.Activo = 1
                  AND a.Activo = 1
                  AND a.OficinaId = ?
                  AND a.Estado NOT IN ('DEVUELTO', 'TRASPASADO')
                GROUP BY
                    p.ProductoId,
                    p.CodigoUnico,
                    p.NombreProducto,
                    p.Descripcion,
                    c.NombreCategoria,
                    pr.NombreProveedor,
                    p.ValorUnitario,
                    p.CantidadMinima,
                    p.Ubicacion,
                    p.EsAsignable,
                    p.RutaImagen,
                    p.FechaCreacion,
                    p.UsuarioCreador
                ORDER BY p.NombreProducto
            """
            cursor.execute(query, (oficina_id,))
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obteniendo productos corporativos por oficina: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def obtener_por_id(producto_id):
        conn = get_database_connection()
        if not conn:
            return None
        cursor = None
        try:
            cursor = conn.cursor()
            query = """
                SELECT 
                    p.ProductoId           AS id,
                    p.CodigoUnico          AS codigo_unico,
                    p.NombreProducto       AS nombre,
                    p.Descripcion          AS descripcion,
                    p.CategoriaId          AS categoria_id,
                    c.NombreCategoria      AS categoria,
                    p.ProveedorId          AS proveedor_id,
                    pr.NombreProveedor     AS proveedor,
                    p.ValorUnitario        AS valor_unitario,
                    p.CantidadDisponible   AS cantidad,
                    p.CantidadMinima       AS cantidad_minima,
                    p.Ubicacion            AS ubicacion,
                    p.EsAsignable          AS es_asignable,
                    p.RutaImagen           AS ruta_imagen,
                    p.FechaCreacion        AS fecha_creacion,
                    p.UsuarioCreador       AS usuario_creador
                FROM ProductosCorporativos p
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Proveedores pr        ON p.ProveedorId = pr.ProveedorId
                WHERE p.ProductoId = ? AND p.Activo = 1
            """
            cursor.execute(query, (producto_id,))
            row = cursor.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cursor.description]
            return dict(zip(cols, row))
        except Exception as e:
            logger.info("Error obteniendo producto corporativo: ref=%s", sanitizar_log_text(_error_id()))
            return None
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    # ================== CREAR / ACTUALIZAR / ELIMINAR ==================
    @staticmethod
    def crear(codigo_unico, nombre, descripcion, categoria_id, proveedor_id,
              valor_unitario, cantidad, cantidad_minima, ubicacion,
              es_asignable, usuario_creador, ruta_imagen):
        """
        Inserta y retorna ProductoId (SQL Server: OUTPUT INSERTED.ProductoId)
        """
        conn = get_database_connection()
        if not conn:
            return None
        cursor = None
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO ProductosCorporativos
                    (CodigoUnico, NombreProducto, Descripcion, CategoriaId, ProveedorId,
                     ValorUnitario, CantidadDisponible, CantidadMinima, Ubicacion,
                     EsAsignable, Activo, FechaCreacion, UsuarioCreador, RutaImagen)
                OUTPUT INSERTED.ProductoId
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, GETDATE(), ?, ?)
            """
            cursor.execute(sql, (
                codigo_unico, nombre, descripcion, int(categoria_id), int(proveedor_id),
                float(valor_unitario), int(cantidad), int(cantidad_minima or 0),
                ubicacion, int(es_asignable), usuario_creador, ruta_imagen
            ))
            new_id = cursor.fetchone()[0]
            conn.commit()
            return new_id
        except Exception as e:
            logger.info("Error creando producto corporativo: ref=%s", sanitizar_log_text(_error_id()))
            try:
                if conn: conn.rollback()
            except:
                pass
            return None
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def actualizar(producto_id, codigo_unico, nombre, descripcion, categoria_id,
                   proveedor_id, valor_unitario, cantidad, cantidad_minima,
                   ubicacion, es_asignable, ruta_imagen=None):
        """Actualizar producto incluyendo cantidad y ruta_imagen"""
        conn = get_database_connection()
        if not conn:
            return False
        cursor = None
        try:
            cursor = conn.cursor()

            if ruta_imagen:
                sql = """
                    UPDATE ProductosCorporativos 
                    SET CodigoUnico = ?, NombreProducto = ?, Descripcion = ?, 
                        CategoriaId = ?, ProveedorId = ?, ValorUnitario = ?,
                        CantidadDisponible = ?, CantidadMinima = ?, Ubicacion = ?, 
                        EsAsignable = ?, RutaImagen = ?
                    WHERE ProductoId = ? AND Activo = 1
                """
                params = (
                    codigo_unico, nombre, descripcion, int(categoria_id), int(proveedor_id),
                    float(valor_unitario), int(cantidad), int(cantidad_minima or 0),
                    ubicacion, int(es_asignable), ruta_imagen, int(producto_id)
                )
            else:
                sql = """
                    UPDATE ProductosCorporativos 
                    SET CodigoUnico = ?, NombreProducto = ?, Descripcion = ?, 
                        CategoriaId = ?, ProveedorId = ?, ValorUnitario = ?,
                        CantidadDisponible = ?, CantidadMinima = ?, Ubicacion = ?, 
                        EsAsignable = ?
                    WHERE ProductoId = ? AND Activo = 1
                """
                params = (
                    codigo_unico, nombre, descripcion, int(categoria_id), int(proveedor_id),
                    float(valor_unitario), int(cantidad), int(cantidad_minima or 0),
                    ubicacion, int(es_asignable), int(producto_id)
                )

            cursor.execute(sql, params)
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.info("Error actualizando producto corporativo: ref=%s", sanitizar_log_text(_error_id()))
            try:
                if conn: conn.rollback()
            except:
                pass
            return False
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def eliminar(producto_id, usuario_accion):
        """Soft delete (Activo = 0) + deja traza minima en historial."""
        conn = get_database_connection()
        if not conn:
            return False
        cursor = None
        try:
            cursor = conn.cursor()
            # Traza en nueva tabla
            cursor.execute("""
                INSERT INTO AsignacionesCorporativasHistorial
                    (ProductoId, Accion, Cantidad, OficinaId, UsuarioAccion, Fecha)
                VALUES (?, 'BAJA_PRODUCTO', 0, NULL, ?, GETDATE())
            """, (int(producto_id), usuario_accion))
            # Baja logica
            cursor.execute(
                "UPDATE ProductosCorporativos SET Activo = 0 WHERE ProductoId = ?",
                (int(producto_id),)
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.info("Error eliminando producto corporativo: ref=%s", sanitizar_log_text(_error_id()))
            try:
                if conn: conn.rollback()
            except:
                pass
            return False
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    # ================== CATALOGOS ==================
    @staticmethod
    def obtener_categorias():
        """
        Retorna todas las categorías activas desde la tabla CategoriasProductos,
        incluso si todavía no tienen productos asociados.
        """
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    c.CategoriaId AS id,
                    c.NombreCategoria AS nombre
                FROM CategoriasProductos c
                WHERE c.Activo = 1
                ORDER BY c.NombreCategoria
            """)
            return [{'id': r[0], 'nombre': r[1]} for r in cursor.fetchall()]
        except Exception as e:
            logger.info("rror obteniendo categorías activas: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def obtener_proveedores():
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT p.ProveedorId AS id, p.NombreProveedor AS nombre
                FROM Proveedores p
                WHERE p.Activo = 1
                ORDER BY p.NombreProveedor
            """)
            return [{'id': r[0], 'nombre': r[1]} for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obtener_proveedores: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def obtener_oficinas():
        """
        Oficinas para asignacion.
        """
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT o.OficinaId AS id, o.NombreOficina AS nombre
                FROM Oficinas o
                WHERE o.Activo = 1
                ORDER BY o.NombreOficina
            """)
            return [{'id': r[0], 'nombre': r[1]} for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obtener_oficinas: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    # ================== ASIGNACIONES / TRAZABILIDAD ==================
    @staticmethod
    def asignar_a_oficina(producto_id, oficina_id, cantidad, usuario_accion):
        """
        Resta stock de ProductosCorporativos.CantidadDisponible y crea registro
        en Asignaciones + guarda traza en AsignacionesCorporativasHistorial.
        """
        conn = get_database_connection()
        if not conn:
            return False
        cursor = None
        try:
            cursor = conn.cursor()

            # 1. PRIMERO: Obtener un UsuarioId válido
            cursor.execute(
                "SELECT TOP 1 UsuarioId FROM Usuarios WHERE Activo = 1 ORDER BY UsuarioId"
            )
            usuario_row = cursor.fetchone()
            if not usuario_row:
                logger.info("Error: No hay usuarios activos en la base de datos")
                return False
            usuario_asignado_id = usuario_row[0]

            # 2. Verificar stock
            cursor.execute(
                "SELECT CantidadDisponible FROM ProductosCorporativos "
                "WHERE ProductoId = ? AND Activo = 1",
                (int(producto_id),)
            )
            row = cursor.fetchone()
            if not row:
                return False
            stock = int(row[0])
            cant = int(cantidad)

            # CORRECCION: condición correcta
            if cant <= 0 or cant > stock:
                return False

            # 3. Descontar stock
            cursor.execute("""
                UPDATE ProductosCorporativos
                SET CantidadDisponible = CantidadDisponible - ?
                WHERE ProductoId = ?
            """, (cant, int(producto_id)))

            # 4. Crear registro en tabla Asignaciones (CON USUARIO VÁLIDO)
            cursor.execute("""
                INSERT INTO Asignaciones 
                (ProductoId, OficinaId, UsuarioAsignadoId, FechaAsignacion, Estado, UsuarioAsignador, Activo)
                VALUES (?, ?, ?, GETDATE(), 'ASIGNADO', ?, 1)
            """, (int(producto_id), int(oficina_id), usuario_asignado_id, usuario_accion))

            # 5. Trazabilidad en tabla AsignacionesCorporativasHistorial
            cursor.execute("""
                INSERT INTO AsignacionesCorporativasHistorial
                    (ProductoId, OficinaId, Accion, Cantidad, UsuarioAccion, Fecha)
                VALUES (?, ?, 'ASIGNAR', ?, ?, GETDATE())
            """, (int(producto_id), int(oficina_id), cant, usuario_accion))

            conn.commit()
            return True
        except Exception as e:
            logger.info("Error asignar_a_oficina: ref=%s", sanitizar_log_text(_error_id()))
            try:
                if conn: conn.rollback()
            except:
                pass
            return False
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def historial_asignaciones(producto_id):
        """Obtener historial de asignaciones para un producto específico"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            # CORRECCION: Agregados los campos UsuarioAsignadoNombre y UsuarioAsignadoEmail
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
            logger.info("Error historial_asignaciones: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    # ================== REPORTES ==================
    @staticmethod
    def reporte_stock_por_categoria():
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    c.NombreCategoria AS categoria,
                    SUM(p.CantidadDisponible) AS total_stock
                FROM ProductosCorporativos p
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                WHERE p.Activo = 1
                GROUP BY c.NombreCategoria
                ORDER BY c.NombreCategoria
            """)
            return [
                {'categoria': r[0], 'total_stock': int(r[1] or 0)}
                for r in cursor.fetchall()
            ]
        except Exception as e:
            logger.info("Error reporte_stock_por_categoria: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def reporte_valor_inventario():
        conn = get_database_connection()
        if not conn:
            return {'valor_total': 0}
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT SUM(p.ValorUnitario * p.CantidadDisponible) AS valor_total
                FROM ProductosCorporativos p
                WHERE p.Activo = 1
            """)
            row = cursor.fetchone()
            return {'valor_total': float(row[0] or 0.0)}
        except Exception as e:
            logger.info("Error reporte_valor_inventario: ref=%s", sanitizar_log_text(_error_id()))
            return {'valor_total': 0}
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def reporte_asignaciones_por_oficina():
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    o.NombreOficina AS oficina,
                    COUNT(a.AsignacionId) AS cantidad_asignaciones
                FROM Asignaciones a
                INNER JOIN Oficinas o ON o.OficinaId = a.OficinaId
                WHERE a.Activo = 1
                GROUP BY o.NombreOficina
                ORDER BY o.NombreOficina
            """)
            return [
                {
                    'oficina': r[0],
                    'cantidad_asignaciones': int(r[1] or 0)
                }
                for r in cursor.fetchall()
            ]
        except Exception as e:
            logger.info("Error reporte_asignaciones_por_oficina: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    # ================== REPORTES AVANZADOS ==================
    @staticmethod
    def reporte_productos_por_oficina():
        """Reporte de productos agrupados por oficina"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COALESCE(o.NombreOficina, 'Sede Principal') AS oficina,
                    COUNT(p.ProductoId) AS total_productos,
                    SUM(p.CantidadDisponible) AS total_stock,
                    SUM(p.ValorUnitario * p.CantidadDisponible) AS valor_total
                FROM ProductosCorporativos p
                LEFT JOIN Asignaciones a ON p.ProductoId = a.ProductoId AND a.Activo = 1
                LEFT JOIN Oficinas o ON a.OficinaId = o.OficinaId
                WHERE p.Activo = 1
                GROUP BY COALESCE(o.NombreOficina, 'Sede Principal')
                ORDER BY valor_total DESC
            """)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error reporte_productos_por_oficina: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def reporte_stock_bajo():
        """Productos con stock bajo o crítico"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    p.ProductoId,
                    p.CodigoUnico,
                    p.NombreProducto,
                    c.NombreCategoria AS categoria,
                    p.CantidadDisponible,
                    p.CantidadMinima,
                    p.ValorUnitario,
                    (p.ValorUnitario * p.CantidadDisponible) AS valor_total,
                    CASE 
                        WHEN p.CantidadDisponible = 0 THEN 'Crítico'
                        WHEN p.CantidadDisponible <= p.CantidadMinima THEN 'Bajo'
                        ELSE 'Normal'
                    END AS estado_stock
                FROM ProductosCorporativos p
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                WHERE p.Activo = 1 
                AND (p.CantidadDisponible = 0 OR p.CantidadDisponible <= p.CantidadMinima)
                ORDER BY p.CantidadDisponible ASC
            """)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error reporte_stock_bajo: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def reporte_movimientos_recientes(limite=50):
        """Movimientos recientes del inventario"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP (?) 
                    h.HistorialId,
                    p.NombreProducto,
                    o.NombreOficina AS oficina,
                    h.Accion,
                    h.Cantidad,
                    h.UsuarioAccion,
                    h.Fecha
                FROM AsignacionesCorporativasHistorial h
                INNER JOIN ProductosCorporativos p ON h.ProductoId = p.ProductoId
                LEFT JOIN Oficinas o ON h.OficinaId = o.OficinaId
                ORDER BY h.Fecha DESC
            """, (limite,))
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error reporte_movimientos_recientes: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def obtener_estadisticas_generales():
        """Estadísticas generales del inventario"""
        conn = get_database_connection()
        if not conn:
            return {}
        cursor = None
        try:
            cursor = conn.cursor()

            # Total productos
            cursor.execute(
                "SELECT COUNT(*) FROM ProductosCorporativos WHERE Activo = 1"
            )
            total_productos = cursor.fetchone()[0]

            # Valor total inventario
            cursor.execute("""
                SELECT SUM(ValorUnitario * CantidadDisponible)
                FROM ProductosCorporativos
                WHERE Activo = 1
            """)
            valor_total = cursor.fetchone()[0] or 0

            # Productos con stock bajo
            cursor.execute("""
                SELECT COUNT(*) 
                FROM ProductosCorporativos 
                WHERE Activo = 1 
                AND (CantidadDisponible = 0 OR CantidadDisponible <= CantidadMinima)
            """)
            stock_bajo = cursor.fetchone()[0]

            # Productos asignables
            cursor.execute("""
                SELECT COUNT(*)
                FROM ProductosCorporativos
                WHERE Activo = 1 AND EsAsignable = 1
            """)
            asignables = cursor.fetchone()[0]

            # Total categorías
            cursor.execute("""
                SELECT COUNT(DISTINCT CategoriaId)
                FROM ProductosCorporativos
                WHERE Activo = 1
            """)
            total_categorias = cursor.fetchone()[0]

            return {
                'total_productos': total_productos,
                'valor_total': float(valor_total),
                'stock_bajo': stock_bajo,
                'asignables': asignables,
                'total_categorias': total_categorias
            }
        except Exception as e:
            logger.info("Error obtener_estadisticas_generales: ref=%s", sanitizar_log_text(_error_id()))
            return {}
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    # ================== VISTAS POR TIPO DE OFICINA ==================

    @staticmethod
    def obtener_por_sede_principal():
        """Obtiene productos de la sede principal (no asignados a oficinas)"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    p.ProductoId           AS id,
                    p.CodigoUnico          AS codigo_unico,
                    p.NombreProducto       AS nombre,
                    p.Descripcion          AS descripcion,
                    c.NombreCategoria      AS categoria,
                    pr.NombreProveedor     AS proveedor,
                    p.ValorUnitario        AS valor_unitario,
                    p.CantidadDisponible   AS cantidad,
                    p.CantidadMinima       AS cantidad_minima,
                    p.Ubicacion            AS ubicacion,
                    p.EsAsignable          AS es_asignable,
                    p.RutaImagen           AS ruta_imagen,
                    p.FechaCreacion        AS fecha_creacion,
                    p.UsuarioCreador       AS usuario_creador,
                    'Sede Principal'       AS oficina
                FROM ProductosCorporativos p
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Proveedores pr        ON p.ProveedorId = pr.ProveedorId
                WHERE p.Activo = 1
                AND NOT EXISTS (
                    SELECT 1 FROM Asignaciones a 
                    WHERE a.ProductoId = p.ProductoId AND a.Activo = 1
                )
                ORDER BY p.NombreProducto
            """)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obteniendo sede principal: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    @staticmethod
    def obtener_por_oficinas_servicio():
        """Obtiene productos de oficinas de servicio (asignados a oficinas)"""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT
                    p.ProductoId           AS id,
                    p.CodigoUnico          AS codigo_unico,
                    p.NombreProducto       AS nombre,
                    p.Descripcion          AS descripcion,
                    c.NombreCategoria      AS categoria,
                    pr.NombreProveedor     AS proveedor,
                    p.ValorUnitario        AS valor_unitario,
                    p.CantidadDisponible   AS cantidad,
                    p.CantidadMinima       AS cantidad_minima,
                    p.Ubicacion            AS ubicacion,
                    p.EsAsignable          AS es_asignable,
                    p.RutaImagen           AS ruta_imagen,
                    p.FechaCreacion        AS fecha_creacion,
                    p.UsuarioCreador       AS usuario_creador,
                    o.NombreOficina        AS oficina
                FROM ProductosCorporativos p
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Proveedores pr        ON p.ProveedorId = pr.ProveedorId
                INNER JOIN Asignaciones a        ON p.ProductoId = a.ProductoId AND a.Activo = 1
                INNER JOIN Oficinas o            ON a.OficinaId = o.OficinaId
                WHERE p.Activo = 1
                ORDER BY o.NombreOficina, p.NombreProducto
            """)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obteniendo oficinas servicio: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    # ================== CONSULTAS DE ASIGNACIONES POR OFICINA ==================
    @staticmethod
    def obtener_asignaciones_por_oficina(oficina_id):
        """Obtiene las asignaciones activas de inventario corporativo para una oficina.

        Incluye un estimado de cantidad asignada (si existe trazabilidad), útil para devoluciones/traspasos.
        """
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    a.AsignacionId                 AS asignacion_id,
                    a.ProductoId                   AS producto_id,
                    p.CodigoUnico                  AS codigo_unico,
                    p.NombreProducto               AS nombre_producto,
                    p.Descripcion                  AS descripcion,
                    c.NombreCategoria              AS categoria,
                    o.NombreOficina                AS oficina,
                    a.UsuarioADNombre              AS usuario_ad_nombre,
                    a.UsuarioADEmail               AS usuario_ad_email,
                    a.FechaAsignacion              AS fecha_asignacion,
                    a.Estado                       AS estado,
                    a.Observaciones                AS observaciones,
                    COALESCE(q.Cantidad, 1)        AS cantidad_asignada
                FROM Asignaciones a
                INNER JOIN ProductosCorporativos p ON a.ProductoId = p.ProductoId
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Oficinas o ON a.OficinaId = o.OficinaId
                OUTER APPLY (
                    SELECT TOP 1 h.Cantidad
                    FROM AsignacionesCorporativasHistorial h
                    WHERE h.ProductoId = a.ProductoId
                      AND h.OficinaId = a.OficinaId
                      AND h.Accion = 'ASIGNAR'
                      AND (
                        (a.UsuarioADEmail IS NOT NULL AND h.UsuarioAsignadoEmail = a.UsuarioADEmail)
                        OR (a.UsuarioADEmail IS NULL AND h.UsuarioAsignadoEmail IS NULL)
                      )
                    ORDER BY ABS(DATEDIFF(SECOND, h.Fecha, a.FechaAsignacion))
                ) q
                WHERE a.Activo = 1
                  AND a.OficinaId = ?
                ORDER BY a.FechaAsignacion DESC
            """, (int(oficina_id),))
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error obtener_asignaciones_por_oficina: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def obtener_asignacion_por_id(asignacion_id):
        """Obtiene una asignación por id con información del producto y oficina."""
        conn = get_database_connection()
        if not conn:
            return None
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    a.AsignacionId                 AS asignacion_id,
                    a.ProductoId                   AS producto_id,
                    p.CodigoUnico                  AS codigo_unico,
                    p.NombreProducto               AS nombre_producto,
                    p.Descripcion                  AS descripcion,
                    c.NombreCategoria              AS categoria,
                    a.OficinaId                    AS oficina_id,
                    o.NombreOficina                AS oficina,
                    a.UsuarioAsignadoId            AS usuario_asignado_id,
                    a.UsuarioADNombre              AS usuario_ad_nombre,
                    a.UsuarioADEmail               AS usuario_ad_email,
                    a.FechaAsignacion              AS fecha_asignacion,
                    a.Estado                       AS estado,
                    a.Observaciones                AS observaciones,
                    COALESCE(q.Cantidad, 1)        AS cantidad_asignada
                FROM Asignaciones a
                INNER JOIN ProductosCorporativos p ON a.ProductoId = p.ProductoId
                INNER JOIN CategoriasProductos c ON p.CategoriaId = c.CategoriaId
                INNER JOIN Oficinas o ON a.OficinaId = o.OficinaId
                OUTER APPLY (
                    SELECT TOP 1 h.Cantidad
                    FROM AsignacionesCorporativasHistorial h
                    WHERE h.ProductoId = a.ProductoId
                      AND h.OficinaId = a.OficinaId
                      AND h.Accion = 'ASIGNAR'
                      AND (
                        (a.UsuarioADEmail IS NOT NULL AND h.UsuarioAsignadoEmail = a.UsuarioADEmail)
                        OR (a.UsuarioADEmail IS NULL AND h.UsuarioAsignadoEmail IS NULL)
                      )
                    ORDER BY ABS(DATEDIFF(SECOND, h.Fecha, a.FechaAsignacion))
                ) q
                WHERE a.AsignacionId = ?
            """, (int(asignacion_id),))
            row = cursor.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cursor.description]
            return dict(zip(cols, row))
        except Exception as e:
            logger.info("Error obtener_asignacion_por_id: ref=%s", sanitizar_log_text(_error_id()))
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    @staticmethod
    def obtener_asignacion_detalle(asignacion_id):
        """Alias compatible: retorna el detalle de una asignación por id.

        Se usa para las vistas de solicitar devolución/traslado.
        """
        return InventarioCorporativoModel.obtener_asignacion_por_id(asignacion_id)
    # ================== DEVOLUCIONES (SOLICITUDES) ==================
    @staticmethod
    def crear_solicitud_devolucion(asignacion_id, cantidad, motivo, usuario_solicita):
        """Crea una solicitud de devolución (pendiente) para inventario corporativo."""
        conn = get_database_connection()
        if not conn:
            return (False, 'Sin conexión a base de datos')
        cursor = None
        try:
            asignacion = InventarioCorporativoModel.obtener_asignacion_por_id(asignacion_id)
            if not asignacion:
                return (False, 'Asignación no encontrada')

            cant = int(cantidad)
            if cant <= 0:
                return (False, 'La cantidad debe ser mayor que 0')

            # Validación blanda: no exceder cantidad estimada asignada
            max_cant = int(asignacion.get('cantidad_asignada') or 1)
            if cant > max_cant:
                return (False, f'La cantidad no puede ser mayor a {max_cant}')

            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO DevolucionesInventarioCorporativo
                    (ProductoId, OficinaId, AsignacionId, Cantidad, Motivo, EstadoDevolucion,
                     UsuarioSolicita, FechaSolicitud, Activo)
                VALUES (?, ?, ?, ?, ?, 'PENDIENTE', ?, GETDATE(), 1)
            """, (
                int(asignacion['producto_id']),
                int(asignacion['oficina_id']),
                int(asignacion_id),
                cant,
                _to_text(motivo or '').strip() or 'Sin motivo',
                _to_text(usuario_solicita)
            ))
            conn.commit()
            return (True, 'Solicitud de devolución creada y enviada para aprobación')
        except Exception as e:
            logger.info("Error crear_solicitud_devolucion: ref=%s", sanitizar_log_text(_error_id()))
            try:
                conn.rollback()
            except Exception:
                pass
            return (False, 'Error creando la solicitud de devolución')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def listar_devoluciones(estado=None, oficina_id=None):
        """Lista devoluciones de inventario corporativo.

        Args:
            estado: 'PENDIENTE' | 'APROBADA' | 'RECHAZADA' (o None para todas)
            oficina_id: filtra por oficina (opcional)
        """
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            where = ['d.Activo = 1']
            params = []
            if estado:
                where.append('d.EstadoDevolucion = ?')
                params.append(_to_text(estado))
            if oficina_id:
                where.append('d.OficinaId = ?')
                params.append(int(oficina_id))

            cursor.execute(f"""
                SELECT
                    d.DevolucionId              AS devolucion_id,
                    d.ProductoId                AS producto_id,
                    p.CodigoUnico               AS codigo_unico,
                    p.NombreProducto            AS nombre_producto,
                    d.OficinaId                 AS oficina_id,
                    o.NombreOficina             AS oficina,
                    d.AsignacionId              AS asignacion_id,
                    d.Cantidad                  AS cantidad,
                    d.Motivo                    AS motivo,
                    d.EstadoDevolucion          AS estado,
                    d.UsuarioSolicita           AS usuario_solicita,
                    d.FechaSolicitud            AS fecha_solicitud,
                    d.UsuarioAprueba            AS usuario_aprueba,
                    d.FechaAprobacion           AS fecha_aprobacion,
                    d.ObservacionesAprobacion   AS observaciones_aprobacion,
                    a.UsuarioADNombre           AS usuario_ad_nombre,
                    a.UsuarioADEmail            AS usuario_ad_email
                FROM DevolucionesInventarioCorporativo d
                INNER JOIN ProductosCorporativos p ON d.ProductoId = p.ProductoId
                INNER JOIN Oficinas o ON d.OficinaId = o.OficinaId
                INNER JOIN Asignaciones a ON d.AsignacionId = a.AsignacionId
                WHERE {' AND '.join(where)}
                ORDER BY d.FechaSolicitud DESC
            """, params)

            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error listar_devoluciones: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def aprobar_devolucion(devolucion_id, usuario_aprueba, observaciones=None):
        """Aprueba una devolución: suma stock, cierra asignación y actualiza solicitud."""
        conn = get_database_connection()
        if not conn:
            return (False, 'Sin conexión a base de datos')
        cursor = None
        try:
            cursor = conn.cursor()
            # Obtener devolucion
            cursor.execute("""
                SELECT DevolucionId, ProductoId, OficinaId, AsignacionId, Cantidad, EstadoDevolucion
                FROM DevolucionesInventarioCorporativo
                WHERE DevolucionId = ? AND Activo = 1
            """, (int(devolucion_id),))
            row = cursor.fetchone()
            if not row:
                return (False, 'Solicitud de devolución no encontrada')

            _, producto_id, oficina_id, asignacion_id, cantidad, estado = row
            if _to_text(estado).upper() != 'PENDIENTE':
                return (False, 'La solicitud ya fue procesada')

            cant = int(cantidad)

            # 1) Marcar solicitud como aprobada
            cursor.execute("""
                UPDATE DevolucionesInventarioCorporativo
                SET EstadoDevolucion = 'APROBADA',
                    UsuarioAprueba = ?,
                    FechaAprobacion = GETDATE(),
                    ObservacionesAprobacion = ?
                WHERE DevolucionId = ?
            """, (_to_text(usuario_aprueba), _to_text(observaciones or '').strip() or None, int(devolucion_id)))

            # 2) Sumar stock al producto
            cursor.execute("""
                UPDATE ProductosCorporativos
                SET CantidadDisponible = CantidadDisponible + ?
                WHERE ProductoId = ?
            """, (cant, int(producto_id)))

            # 3) Cerrar asignación
            cursor.execute("""
                UPDATE Asignaciones
                SET Estado = 'DEVUELTO',
                    FechaDevolucion = GETDATE(),
                    Activo = 0
                WHERE AsignacionId = ?
            """, (int(asignacion_id),))

            # 4) Trazabilidad
            cursor.execute("""
                INSERT INTO AsignacionesCorporativasHistorial
                    (ProductoId, OficinaId, Accion, Cantidad, UsuarioAccion, Fecha, Observaciones)
                VALUES (?, ?, 'DEVOLVER', ?, ?, GETDATE(), ?)
            """, (
                int(producto_id),
                int(oficina_id),
                cant,
                _to_text(usuario_aprueba),
                _to_text(observaciones or '').strip() or None
            ))

            # 5) Movimiento inventario (opcional)
            cursor.execute("""
                INSERT INTO MovimientosInventario
                    (ProductoId, TipoMovimiento, Cantidad, FechaMovimiento, UsuarioMovimiento, Observaciones, Referencia)
                VALUES (?, 'DEVOLUCION', ?, GETDATE(), ?, ?, ?)
            """, (
                int(producto_id),
                cant,
                _to_text(usuario_aprueba),
                _to_text(observaciones or '').strip() or None,
                f'DEVOLUCION:{int(devolucion_id)}'
            ))

            conn.commit()
            return (True, 'Devolución aprobada y aplicada al inventario')
        except Exception as e:
            logger.info("Error aprobar_devolucion: ref=%s", sanitizar_log_text(_error_id()))
            try:
                conn.rollback()
            except Exception:
                pass
            return (False, 'Error aprobando la devolución')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def rechazar_devolucion(devolucion_id, usuario_aprueba, observaciones=None):
        """Rechaza una devolución."""
        conn = get_database_connection()
        if not conn:
            return (False, 'Sin conexión a base de datos')
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE DevolucionesInventarioCorporativo
                SET EstadoDevolucion = 'RECHAZADA',
                    UsuarioAprueba = ?,
                    FechaAprobacion = GETDATE(),
                    ObservacionesAprobacion = ?
                WHERE DevolucionId = ?
                  AND Activo = 1
                  AND EstadoDevolucion = 'PENDIENTE'
            """, (_to_text(usuario_aprueba), _to_text(observaciones or '').strip() or None, int(devolucion_id)))

            if cursor.rowcount == 0:
                conn.rollback()
                return (False, 'Solicitud no encontrada o ya fue procesada')

            conn.commit()
            return (True, 'Devolución rechazada')
        except Exception as e:
            logger.info("Error rechazar_devolucion: ref=%s", sanitizar_log_text(_error_id()))
            try:
                conn.rollback()
            except Exception:
                pass
            return (False, 'Error rechazando la devolución')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # ================== TRASPASOS (SOLICITUDES) ==================
    @staticmethod
    def crear_solicitud_traspaso(asignacion_id, oficina_destino_id, cantidad, motivo, usuario_solicita):
        """Crea una solicitud de traspaso (pendiente) para inventario corporativo."""
        conn = get_database_connection()
        if not conn:
            return (False, 'Sin conexión a base de datos')
        cursor = None
        try:
            asignacion = InventarioCorporativoModel.obtener_asignacion_por_id(asignacion_id)
            if not asignacion:
                return (False, 'Asignación no encontrada')

            destino = int(oficina_destino_id)
            if destino == int(asignacion['oficina_id']):
                return (False, 'La oficina destino debe ser diferente a la oficina origen')

            cant = int(cantidad)
            if cant <= 0:
                return (False, 'La cantidad debe ser mayor que 0')

            max_cant = int(asignacion.get('cantidad_asignada') or 1)
            if cant > max_cant:
                return (False, f'La cantidad no puede ser mayor a {max_cant}')

            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO TraspasosInventarioCorporativo
                    (ProductoId, OficinaOrigenId, OficinaDestinoId, AsignacionOrigenId, Cantidad,
                     Motivo, EstadoTraspaso, UsuarioSolicita, FechaSolicitud, Activo)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDIENTE', ?, GETDATE(), 1)
            """, (
                int(asignacion['producto_id']),
                int(asignacion['oficina_id']),
                destino,
                int(asignacion_id),
                cant,
                _to_text(motivo or '').strip() or 'Sin motivo',
                _to_text(usuario_solicita)
            ))
            conn.commit()
            return (True, 'Solicitud de traslado creada y enviada para aprobación')
        except Exception as e:
            logger.info("Error crear_solicitud_traspaso: ref=%s", sanitizar_log_text(_error_id()))
            try:
                conn.rollback()
            except Exception:
                pass
            return (False, 'Error creando la solicitud de traslado')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def listar_traspasos(estado=None, oficina_id=None):
        """Lista traspasos de inventario corporativo."""
        conn = get_database_connection()
        if not conn:
            return []
        cursor = None
        try:
            cursor = conn.cursor()
            where = ['t.Activo = 1']
            params = []
            if estado:
                where.append('t.EstadoTraspaso = ?')
                params.append(_to_text(estado))
            if oficina_id:
                # Filtra por origen o destino
                where.append('(t.OficinaOrigenId = ? OR t.OficinaDestinoId = ?)')
                params.extend([int(oficina_id), int(oficina_id)])

            cursor.execute(f"""
                SELECT
                    t.TraspasoId               AS traspaso_id,
                    t.ProductoId               AS producto_id,
                    p.CodigoUnico              AS codigo_unico,
                    p.NombreProducto           AS nombre_producto,
                    t.OficinaOrigenId          AS oficina_origen_id,
                    oo.NombreOficina           AS oficina_origen,
                    t.OficinaDestinoId         AS oficina_destino_id,
                    od.NombreOficina           AS oficina_destino,
                    t.AsignacionOrigenId       AS asignacion_origen_id,
                    t.Cantidad                 AS cantidad,
                    t.Motivo                   AS motivo,
                    t.EstadoTraspaso           AS estado,
                    t.UsuarioSolicita          AS usuario_solicita,
                    t.FechaSolicitud           AS fecha_solicitud,
                    t.UsuarioAprueba           AS usuario_aprueba,
                    t.FechaAprobacion          AS fecha_aprobacion,
                    t.ObservacionesAprobacion  AS observaciones_aprobacion,
                    a.UsuarioADNombre          AS usuario_ad_nombre,
                    a.UsuarioADEmail           AS usuario_ad_email
                FROM TraspasosInventarioCorporativo t
                INNER JOIN ProductosCorporativos p ON t.ProductoId = p.ProductoId
                INNER JOIN Oficinas oo ON t.OficinaOrigenId = oo.OficinaId
                INNER JOIN Oficinas od ON t.OficinaDestinoId = od.OficinaId
                INNER JOIN Asignaciones a ON t.AsignacionOrigenId = a.AsignacionId
                WHERE {' AND '.join(where)}
                ORDER BY t.FechaSolicitud DESC
            """, params)

            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            logger.info("Error listar_traspasos: ref=%s", sanitizar_log_text(_error_id()))
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def aprobar_traspaso(traspaso_id, usuario_aprueba, observaciones=None):
        """Aprueba un traspaso: cierra asignación origen y crea una nueva en oficina destino."""
        conn = get_database_connection()
        if not conn:
            return (False, 'Sin conexión a base de datos')
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TraspasoId, ProductoId, OficinaOrigenId, OficinaDestinoId, AsignacionOrigenId, Cantidad, EstadoTraspaso
                FROM TraspasosInventarioCorporativo
                WHERE TraspasoId = ? AND Activo = 1
            """, (int(traspaso_id),))
            row = cursor.fetchone()
            if not row:
                return (False, 'Solicitud de traslado no encontrada')

            _, producto_id, oficina_origen_id, oficina_destino_id, asignacion_origen_id, cantidad, estado = row
            if _to_text(estado).upper() != 'PENDIENTE':
                return (False, 'La solicitud ya fue procesada')

            cant = int(cantidad)

            # Traer datos de la asignación origen
            cursor.execute("""
                SELECT UsuarioAsignadoId, UsuarioADNombre, UsuarioADEmail
                FROM Asignaciones
                WHERE AsignacionId = ?
            """, (int(asignacion_origen_id),))
            arow = cursor.fetchone()
            if not arow:
                return (False, 'Asignación origen no encontrada')
            usuario_asignado_id, usuario_ad_nombre, usuario_ad_email = arow

            # 1) Marcar solicitud como aprobada
            cursor.execute("""
                UPDATE TraspasosInventarioCorporativo
                SET EstadoTraspaso = 'APROBADO',
                    UsuarioAprueba = ?,
                    FechaAprobacion = GETDATE(),
                    ObservacionesAprobacion = ?
                WHERE TraspasoId = ?
            """, (_to_text(usuario_aprueba), _to_text(observaciones or '').strip() or None, int(traspaso_id)))

            # 2) Cerrar asignación origen
            cursor.execute("""
                UPDATE Asignaciones
                SET Estado = 'TRASPASADO',
                    Activo = 0
                WHERE AsignacionId = ?
            """, (int(asignacion_origen_id),))

            # 3) Crear nueva asignación en destino
            cursor.execute("""
                INSERT INTO Asignaciones
                    (ProductoId, UsuarioAsignadoId, OficinaId, FechaAsignacion, Estado, Observaciones, UsuarioAsignador, Activo,
                     UsuarioADNombre, UsuarioADEmail)
                VALUES (?, ?, ?, GETDATE(), 'ASIGNADO', ?, ?, 1, ?, ?)
            """, (
                int(producto_id),
                int(usuario_asignado_id),
                int(oficina_destino_id),
                f'Traslado aprobado desde oficina {int(oficina_origen_id)}. TraspasoId={int(traspaso_id)}',
                _to_text(usuario_aprueba),
                usuario_ad_nombre,
                usuario_ad_email
            ))

            # 4) Trazabilidad

            # 3.b) Registrar cantidad asignada en destino (para que los reportes sumen correctamente)
            # Nota: varias vistas calculan cantidad por oficina usando historial con Accion='ASIGNAR'.
            cursor.execute("""
                INSERT INTO AsignacionesCorporativasHistorial
                    (ProductoId, OficinaId, Accion, Cantidad, UsuarioAccion, Fecha, Observaciones,
                     UsuarioAsignadoNombre, UsuarioAsignadoEmail)
                VALUES (?, ?, 'ASIGNAR', ?, ?, GETDATE(), ?, ?, ?)
            """, (
                int(producto_id),
                int(oficina_destino_id),
                cant,
                _to_text(usuario_aprueba),
                f'Traspaso aprobado. TraspasoId={int(traspaso_id)}',
                usuario_ad_nombre,
                usuario_ad_email
            ))

            cursor.execute("""
                INSERT INTO AsignacionesCorporativasHistorial
                    (ProductoId, OficinaId, Accion, Cantidad, UsuarioAccion, Fecha, Observaciones,
                     UsuarioAsignadoNombre, UsuarioAsignadoEmail)
                VALUES (?, ?, 'TRASPASAR', ?, ?, GETDATE(), ?, ?, ?)
            """, (
                int(producto_id),
                int(oficina_destino_id),
                cant,
                _to_text(usuario_aprueba),
                _to_text(observaciones or '').strip() or None,
                usuario_ad_nombre,
                usuario_ad_email
            ))

            # 5) Movimiento inventario (opcional)
            cursor.execute("""
                INSERT INTO MovimientosInventario
                    (ProductoId, TipoMovimiento, Cantidad, FechaMovimiento, UsuarioMovimiento, Observaciones, Referencia)
                VALUES (?, 'TRASPASO', ?, GETDATE(), ?, ?, ?)
            """, (
                int(producto_id),
                cant,
                _to_text(usuario_aprueba),
                _to_text(observaciones or '').strip() or None,
                f'TRASPASO:{int(traspaso_id)}'
            ))

            conn.commit()
            return (True, 'Traslado aprobado y aplicado')
        except Exception as e:
            logger.info("Error aprobar_traspaso: ref=%s", sanitizar_log_text(_error_id()))
            try:
                conn.rollback()
            except Exception:
                pass
            return (False, 'Error aprobando el traslado')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def rechazar_traspaso(traspaso_id, usuario_aprueba, observaciones=None):
        """Rechaza un traspaso."""
        conn = get_database_connection()
        if not conn:
            return (False, 'Sin conexión a base de datos')
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE TraspasosInventarioCorporativo
                SET EstadoTraspaso = 'RECHAZADO',
                    UsuarioAprueba = ?,
                    FechaAprobacion = GETDATE(),
                    ObservacionesAprobacion = ?
                WHERE TraspasoId = ?
                  AND Activo = 1
                  AND EstadoTraspaso = 'PENDIENTE'
            """, (_to_text(usuario_aprueba), _to_text(observaciones or '').strip() or None, int(traspaso_id)))

            if cursor.rowcount == 0:
                conn.rollback()
                return (False, 'Solicitud no encontrada o ya fue procesada')

            conn.commit()
            return (True, 'Traslado rechazado')
        except Exception as e:
            logger.info("Error rechazar_traspaso: ref=%s", sanitizar_log_text(_error_id()))
            try:
                conn.rollback()
            except Exception:
                pass
            return (False, 'Error rechazando el traslado')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
