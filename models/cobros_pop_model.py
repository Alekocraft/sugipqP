# models/cobros_pop_model.py

"""Modelo para manejo de estado de cobros POP por oficina y mes."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from datetime import datetime

from database import get_database_connection


class CobroPOPMensualModel:
    """CRUD del estado de cobro mensual por oficina.

    Nota:
      - El monto del cobro se calcula desde SolicitudesMaterial.
      - Esta tabla solo persiste el estado (PENDIENTE/CANCELADO) y auditoría básica.
    """

    ESTADOS_VALIDOS = {"PENDIENTE", "CANCELADO"}

    @staticmethod
    def obtener_estados_por_periodo(periodo: str) -> Dict[int, Dict]:
        """Retorna dict {OficinaId: {estado, fecha_cambio, usuario_cambio}} para el periodo."""
        conn = get_database_connection()
        if conn is None:
            return {}
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT OficinaId, Estado, FechaCambio, UsuarioCambio
                FROM dbo.CobrosPOPMensual
                WHERE Periodo = ?
                """,
                (periodo,),
            )
            rows = cur.fetchall() or []
            out: Dict[int, Dict] = {}
            for oficina_id, estado, fecha, usuario in rows:
                out[int(oficina_id)] = {
                    "estado": (estado or "PENDIENTE").upper(),
                    "fecha_cambio": fecha,
                    "usuario_cambio": usuario,
                }
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

    @staticmethod
    def set_estado(periodo: str, oficina_id: int, estado: str, usuario_cambio: str) -> Tuple[bool, str]:
        """Upsert de estado para oficina/periodo."""
        estado_norm = (estado or "").strip().upper()
        if estado_norm not in CobroPOPMensualModel.ESTADOS_VALIDOS:
            return False, "Estado inválido"

        conn = get_database_connection()
        if conn is None:
            return False, "Error de conexión"
        cur = conn.cursor()
        try:
            cur.execute(
                """
                IF EXISTS (SELECT 1 FROM dbo.CobrosPOPMensual WHERE OficinaId = ? AND Periodo = ?)
                BEGIN
                    UPDATE dbo.CobrosPOPMensual
                    SET Estado = ?, FechaCambio = GETDATE(), UsuarioCambio = ?
                    WHERE OficinaId = ? AND Periodo = ?;
                END
                ELSE
                BEGIN
                    INSERT INTO dbo.CobrosPOPMensual (OficinaId, Periodo, Estado, FechaCambio, UsuarioCambio)
                    VALUES (?, ?, ?, GETDATE(), ?);
                END
                """,
                (
                    oficina_id, periodo,
                    estado_norm, usuario_cambio, oficina_id, periodo,
                    oficina_id, periodo, estado_norm, usuario_cambio,
                ),
            )
            conn.commit()
            return True, "OK"
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False, "Error al actualizar estado (verifique que exista dbo.CobrosPOPMensual y que la migración esté aplicada)"
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def set_estado_masivo(periodo: str, oficina_ids: List[int], estado: str, usuario_cambio: str) -> Tuple[bool, str]:
        """Upsert masivo para múltiples oficinas."""
        estado_norm = (estado or "").strip().upper()
        if estado_norm not in CobroPOPMensualModel.ESTADOS_VALIDOS:
            return False, "Estado inválido"

        oficina_ids = [int(x) for x in (oficina_ids or []) if str(x).strip().isdigit()]
        if not oficina_ids:
            return True, "Sin oficinas para actualizar"

        conn = get_database_connection()
        if conn is None:
            return False, "Error de conexión"
        cur = conn.cursor()
        try:
            for oficina_id in oficina_ids:
                cur.execute(
                    """
                    IF EXISTS (SELECT 1 FROM dbo.CobrosPOPMensual WHERE OficinaId = ? AND Periodo = ?)
                    BEGIN
                        UPDATE dbo.CobrosPOPMensual
                        SET Estado = ?, FechaCambio = GETDATE(), UsuarioCambio = ?
                        WHERE OficinaId = ? AND Periodo = ?;
                    END
                    ELSE
                    BEGIN
                        INSERT INTO dbo.CobrosPOPMensual (OficinaId, Periodo, Estado, FechaCambio, UsuarioCambio)
                        VALUES (?, ?, ?, GETDATE(), ?);
                    END
                    """,
                    (
                        oficina_id, periodo,
                        estado_norm, usuario_cambio, oficina_id, periodo,
                        oficina_id, periodo, estado_norm, usuario_cambio,
                    ),
                )
            conn.commit()
            return True, "OK"
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False, "Error al actualizar estados (verifique migración de dbo.CobrosPOPMensual)"
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass


from decimal import Decimal, ROUND_HALF_UP
from typing import Any


def _add_months(periodo: str, months: int) -> str:
    y = int(periodo[:4])
    m = int(periodo[5:7])
    idx = (y * 12 + (m - 1)) + int(months)
    ny = idx // 12
    nm = (idx % 12) + 1
    return f"{ny:04d}-{nm:02d}"


def _money2(x: Decimal) -> Decimal:
    return x.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


class CobroPOPDiferidoSolicitudModel:
    """Gestión de diferidos por solicitud para cobros POP."""

    @staticmethod
    def _tables_exist(cur) -> bool:
        try:
            cur.execute(
                """
                SELECT CASE WHEN OBJECT_ID('dbo.CobrosPOPDiferidoSolicitudPlan','U') IS NOT NULL
                          AND OBJECT_ID('dbo.CobrosPOPDiferidoSolicitudCuota','U') IS NOT NULL
                       THEN 1 ELSE 0 END
                """
            )
            row = cur.fetchone()
            return bool(row and row[0] == 1)
        except Exception:
            return False

    @staticmethod
    def obtener_resumen_cuotas_periodo(periodo: str, oficina_id: Optional[int] = None) -> Dict[int, Dict]:
        conn = get_database_connection()
        if conn is None:
            return {}
        cur = conn.cursor()
        try:
            if not CobroPOPDiferidoSolicitudModel._tables_exist(cur):
                return {}

            params = [periodo]
            where_of = ''
            if oficina_id is not None:
                where_of = ' AND p.OficinaId = ?'
                params.append(int(oficina_id))

            cur.execute(f"""
                SELECT
                    p.OficinaId,
                    p.SolicitudId,
                    p.PlanId,
                    p.PeriodoOrigen,
                    p.NumeroCuotas,
                    c.CuotaId,
                    c.Periodo,
                    c.NumeroCuota,
                    CAST(c.ValorCuota AS DECIMAL(18,2)) AS ValorCuota,
                    CAST(c.Pagado AS INT) AS Pagado
                FROM dbo.CobrosPOPDiferidoSolicitudCuota c
                INNER JOIN dbo.CobrosPOPDiferidoSolicitudPlan p ON c.PlanId = p.PlanId
                WHERE c.Periodo = ?
                  AND p.Activo = 1
                  {where_of}
                ORDER BY p.OficinaId, p.SolicitudId, p.PlanId, c.NumeroCuota
            """, params)
            rows = cur.fetchall() or []
            out: Dict[int, Dict] = {}
            for oid, solicitud_id, plan_id, periodo_origen, numero_cuotas, cuota_id, cperiodo, numero_cuota, valor_cuota, pagado in rows:
                d = out.setdefault(int(oid), {
                    'total_cuota_mes': Decimal('0.00'),
                    'cuotas_mes': [],
                })
                vc = Decimal(str(valor_cuota or 0))
                d['total_cuota_mes'] += vc
                d['cuotas_mes'].append({
                    'cuota_id': int(cuota_id),
                    'plan_id': int(plan_id),
                    'solicitud_id': int(solicitud_id),
                    'periodo_origen': periodo_origen,
                    'numero_cuota': int(numero_cuota),
                    'numero_cuotas': int(numero_cuotas),
                    'periodo': cperiodo,
                    'valor_cuota': float(vc),
                    'pagado': bool(pagado),
                })
            for oid, d in out.items():
                cuotas = d.get('cuotas_mes') or []
                d['estado_cuota_mes'] = 'PAGADO' if cuotas and all(q.get('pagado') for q in cuotas) else ('PENDIENTE' if cuotas else None)
                d['total_cuota_mes'] = float(_money2(d['total_cuota_mes']))
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

    @staticmethod
    def obtener_planes_y_cuotas_oficinas(oficina_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        oficina_ids = [int(x) for x in (oficina_ids or []) if str(x).strip().isdigit()]
        if not oficina_ids:
            return {}
        conn = get_database_connection()
        if conn is None:
            return {}
        cur = conn.cursor()
        try:
            if not CobroPOPDiferidoSolicitudModel._tables_exist(cur):
                return {}
            placeholders = ','.join(['?'] * len(oficina_ids))
            cur.execute(f"""
                SELECT
                    p.PlanId,
                    p.OficinaId,
                    p.SolicitudId,
                    p.PeriodoOrigen,
                    CAST(p.TotalDiferido AS DECIMAL(18,2)) AS TotalDiferido,
                    p.NumeroCuotas,
                    CAST(p.ValorCuota AS DECIMAL(18,2)) AS ValorCuota,
                    p.PeriodoInicio,
                    p.PeriodoFin,
                    CAST(p.Activo AS INT) AS Activo,
                    p.FechaCrea,
                    p.UsuarioCrea
                FROM dbo.CobrosPOPDiferidoSolicitudPlan p
                WHERE p.Activo = 1
                  AND p.OficinaId IN ({placeholders})
                ORDER BY p.OficinaId, p.SolicitudId, p.PeriodoOrigen
            """, oficina_ids)
            plan_rows = cur.fetchall() or []
            out: Dict[int, Dict[str, Any]] = {int(oid): {'planes': []} for oid in oficina_ids}
            plan_ids = []
            plan_index = {}
            for plan_id, oid, solicitud_id, periodo_origen, total_diferido, numero_cuotas, valor_cuota, periodo_inicio, periodo_fin, activo, fecha_crea, usuario_crea in plan_rows:
                p = {
                    'plan_id': int(plan_id),
                    'oficina_id': int(oid),
                    'solicitud_id': int(solicitud_id),
                    'periodo_origen': periodo_origen,
                    'total_diferido': float(Decimal(str(total_diferido or 0))),
                    'numero_cuotas': int(numero_cuotas or 0),
                    'valor_cuota': float(Decimal(str(valor_cuota or 0))),
                    'periodo_inicio': periodo_inicio,
                    'periodo_fin': periodo_fin,
                    'activo': bool(activo),
                    'fecha_crea': fecha_crea,
                    'usuario_crea': usuario_crea,
                    'cuotas': [],
                }
                out.setdefault(int(oid), {'planes': []})['planes'].append(p)
                plan_ids.append(int(plan_id))
                plan_index[int(plan_id)] = p
            if not plan_ids:
                return out
            placeholders = ','.join(['?'] * len(plan_ids))
            cur.execute(f"""
                SELECT CuotaId, PlanId, Periodo, NumeroCuota,
                       CAST(ValorCuota AS DECIMAL(18,2)) AS ValorCuota,
                       CAST(Pagado AS INT) AS Pagado,
                       FechaPago, UsuarioPago
                FROM dbo.CobrosPOPDiferidoSolicitudCuota
                WHERE PlanId IN ({placeholders})
                ORDER BY PlanId, NumeroCuota
            """, plan_ids)
            cuota_rows = cur.fetchall() or []
            for cuota_id, plan_id, periodo, numero_cuota, valor_cuota, pagado, fecha_pago, usuario_pago in cuota_rows:
                p = plan_index.get(int(plan_id))
                if not p:
                    continue
                p['cuotas'].append({
                    'cuota_id': int(cuota_id),
                    'periodo': periodo,
                    'numero_cuota': int(numero_cuota or 0),
                    'valor_cuota': float(Decimal(str(valor_cuota or 0))),
                    'pagado': bool(pagado),
                    'fecha_pago': fecha_pago,
                    'usuario_pago': usuario_pago,
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

    @staticmethod
    def existe_plan_activo_solicitud(solicitud_id: int, periodo_origen: str) -> bool:
        conn = get_database_connection()
        if conn is None:
            return False
        cur = conn.cursor()
        try:
            if not CobroPOPDiferidoSolicitudModel._tables_exist(cur):
                return False
            cur.execute("""
                SELECT TOP 1 1
                FROM dbo.CobrosPOPDiferidoSolicitudPlan
                WHERE SolicitudId = ? AND PeriodoOrigen = ? AND Activo = 1
            """, (int(solicitud_id), periodo_origen))
            return cur.fetchone() is not None
        except Exception:
            return False
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def crear_plan(solicitud_id: int, oficina_id: int, periodo_origen: str, total_diferido: float, numero_cuotas: int, periodo_inicio: str, usuario: str):
        try:
            numero_cuotas = int(numero_cuotas)
            if numero_cuotas < 2 or numero_cuotas > 120:
                return False, 'Número de cuotas inválido'
        except Exception:
            return False, 'Número de cuotas inválido'

        total = Decimal(str(total_diferido or 0))
        if total <= 0:
            return False, 'Total diferido inválido'

        base = _money2(total / Decimal(numero_cuotas))
        periodo_fin = _add_months(periodo_inicio, numero_cuotas - 1)

        conn = get_database_connection()
        if conn is None:
            return False, 'Error de conexión'
        cur = conn.cursor()
        try:
            if not CobroPOPDiferidoSolicitudModel._tables_exist(cur):
                return False, 'Tablas de diferidos por solicitud no existen (ejecute migración)'
            cur.execute("""
                SELECT TOP 1 PlanId
                FROM dbo.CobrosPOPDiferidoSolicitudPlan
                WHERE SolicitudId = ? AND PeriodoOrigen = ? AND Activo = 1
            """, (int(solicitud_id), periodo_origen))
            if cur.fetchone() is not None:
                return False, 'Ya existe un diferido activo para esa solicitud en ese periodo'

            cur.execute("""
                INSERT INTO dbo.CobrosPOPDiferidoSolicitudPlan
                    (SolicitudId, OficinaId, PeriodoOrigen, TotalDiferido, NumeroCuotas, ValorCuota, PeriodoInicio, PeriodoFin, Activo, FechaCrea, UsuarioCrea)
                OUTPUT INSERTED.PlanId
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, 1, GETDATE(), ?)
            """, (int(solicitud_id), int(oficina_id), periodo_origen, float(_money2(total)), int(numero_cuotas), float(base), periodo_inicio, periodo_fin, usuario))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return False, 'No se pudo crear el diferido'
            plan_id = int(row[0])
            suma_prev = base * Decimal(numero_cuotas - 1)
            ultima = _money2(total - suma_prev)
            for i in range(1, numero_cuotas + 1):
                p = _add_months(periodo_inicio, i - 1)
                valor = ultima if i == numero_cuotas else base
                cur.execute("""
                    INSERT INTO dbo.CobrosPOPDiferidoSolicitudCuota
                        (PlanId, Periodo, NumeroCuota, ValorCuota, Pagado)
                    VALUES (?, ?, ?, ?, 0)
                """, (plan_id, p, i, float(valor)))
            conn.commit()
            return True, f'OK|{plan_id}|{float(base):.2f}'
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            return False, f'No se pudo crear el diferido por solicitud: {e}'
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def set_pago_cuota(cuota_id: int, pagado: bool, usuario: str):
        conn = get_database_connection()
        if conn is None:
            return False, 'Error de conexión'
        cur = conn.cursor()
        try:
            if not CobroPOPDiferidoSolicitudModel._tables_exist(cur):
                return False, 'Tablas de diferidos por solicitud no existen (ejecute migración)'
            if pagado:
                cur.execute("""
                    UPDATE dbo.CobrosPOPDiferidoSolicitudCuota
                    SET Pagado = 1, FechaPago = GETDATE(), UsuarioPago = ?
                    WHERE CuotaId = ?
                """, (usuario, int(cuota_id)))
            else:
                cur.execute("""
                    UPDATE dbo.CobrosPOPDiferidoSolicitudCuota
                    SET Pagado = 0, FechaPago = NULL, UsuarioPago = NULL
                    WHERE CuotaId = ?
                """, (int(cuota_id),))
            conn.commit()
            return True, 'OK'
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False, 'No se pudo actualizar el pago de la cuota'
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def set_pago_mes(oficina_id: int, periodo: str, pagado: bool, usuario: str):
        conn = get_database_connection()
        if conn is None:
            return False, 'Error de conexión'
        cur = conn.cursor()
        try:
            if not CobroPOPDiferidoSolicitudModel._tables_exist(cur):
                return False, 'Tablas de diferidos por solicitud no existen (ejecute migración)'
            if pagado:
                cur.execute("""
                    UPDATE c
                    SET c.Pagado = 1, c.FechaPago = GETDATE(), c.UsuarioPago = ?
                    FROM dbo.CobrosPOPDiferidoSolicitudCuota c
                    INNER JOIN dbo.CobrosPOPDiferidoSolicitudPlan p ON c.PlanId = p.PlanId
                    WHERE p.OficinaId = ? AND c.Periodo = ? AND p.Activo = 1
                """, (usuario, int(oficina_id), periodo))
            else:
                cur.execute("""
                    UPDATE c
                    SET c.Pagado = 0, c.FechaPago = NULL, c.UsuarioPago = NULL
                    FROM dbo.CobrosPOPDiferidoSolicitudCuota c
                    INNER JOIN dbo.CobrosPOPDiferidoSolicitudPlan p ON c.PlanId = p.PlanId
                    WHERE p.OficinaId = ? AND c.Periodo = ? AND p.Activo = 1
                """, (int(oficina_id), periodo))
            conn.commit()
            return True, 'OK'
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False, 'No se pudo actualizar el pago del mes'
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
