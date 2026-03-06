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
