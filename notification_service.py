# services/notification_service.py
"""
Servicio para enviar notificaciones por email.
Incluye:
- Notificaciones de asignación de inventario
- Notificaciones con confirmación de recepción
- Sistema de tokens para confirmaciones
"""

from __future__ import annotations

# Compatibilidad: este proyecto puede ejecutarse con Python < 3.10.
# Evitamos evaluar anotaciones como `str | None` en tiempo de ejecución.

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
import os
import html
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

from utils.helpers import sanitizar_log_text

from email.mime.base import MIMEBase
from email import encoders
from email.mime.image import MIMEImage  # <-- Para PNG/JPG (Outlook)

logger = logging.getLogger(__name__)


class NotificationService:
    """
    Servicio de notificaciones por correo electrónico.
    """

    # ==========================
    # Branding (Qualitas)
    # ==========================
    BRAND = {
        "blue": "#0098B1",
        "gray": "#D9D9D9",
        "purple": "#A73493",
        "company": "Qualitas Colombia",
        "app_name": "Sistema de Gestión de Inventarios",
        "logo_cid": "qualitas_logo",
    }

    # Configuración SMTP (se conserva tal como la tienes)
    SMTP_CONFIG = {
        "server": os.getenv("SMTP_SERVER"),
        "port": int(os.getenv("SMTP_PORT", 25)),
        "use_tls": os.getenv("SMTP_USE_TLS", "False").lower() == "true",
        "from_email": os.getenv("SMTP_FROM_EMAIL", "gestiondeInventarios@qualitascolombia.com.co"),
        "username": os.getenv("SMTP_USERNAME", ""),
        "password": os.getenv("SMTP_PASSWORD", ""),
    }

    @staticmethod
    def _truthy_env(name: str, default: str = "false") -> bool:
        return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "si")


    @staticmethod
    def _include_logo() -> bool:
        """Define si el email debe incluir logo inline.

        Por defecto: NO (EMAIL_INCLUDE_LOGO=false).
        Si se habilita, se adjunta el logo como CID y se muestra en el header.
        """
        return NotificationService._truthy_env("EMAIL_INCLUDE_LOGO", "false")

    @staticmethod
    def notifications_enabled() -> bool:
        """Indica si las notificaciones están activas y hay config SMTP mínima."""
        if os.getenv("NOTIFICATIONS_ENABLED", "true").strip().lower() in ("0", "false", "no", "n"):
            return False
        cfg = getattr(NotificationService, "SMTP_CONFIG", {}) or {}
        return bool(cfg.get("server")) and bool(cfg.get("port")) and bool(cfg.get("from_email"))

    @staticmethod
    def _mask_email(email: str) -> str:
        """Enmascara emails para logs (evita info sensible)."""
        try:
            e = (email or "").strip()
            if "@" not in e:
                return e[:2] + "***" if len(e) > 2 else "***"
            user, dom = e.split("@", 1)
            if len(user) <= 2:
                user_m = user[:1] + "***"
            else:
                user_m = user[:2] + "***" + user[-1:]
            # dominio: mostrar solo últimos 6 chars aprox
            dom_m = dom
            if len(dom) > 8:
                dom_m = dom[:2] + "***" + dom[-6:]
            return f"{user_m}@{dom_m}"
        except Exception:
            return "***"

    @staticmethod
    def _unique_emails(emails: List[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for e in emails or []:
            e2 = (e or "").strip().lower()
            if not e2 or e2 in seen:
                continue
            seen.add(e2)
            out.append(e.strip())
        return out

    @staticmethod
    def _obtener_aprobadores_activos() -> List[Dict[str, str]]:
        """Lee aprobadores activos desde la tabla Aprobadores."""
        try:
            from database import get_database_connection
            conn = get_database_connection()
            if not conn:
                return []
            cur = conn.cursor()
            cur.execute(
                """
                SELECT NombreAprobador, Email
                FROM Aprobadores
                WHERE Activo = 1
                  AND Email IS NOT NULL
                  AND LTRIM(RTRIM(Email)) <> ''
                ORDER BY AprobadorId
                """
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            aprobadores: List[Dict[str, str]] = []
            for r in rows or []:
                aprobadores.append({
                    "nombre": (r[0] or "Aprobador").strip() if len(r) > 0 else "Aprobador",
                    "email": (r[1] or "").strip() if len(r) > 1 else "",
                })
            return [a for a in aprobadores if a.get("email")]
        except Exception:
            logger.exception("Error leyendo aprobadores activos")
            return []

    @staticmethod
    def _send_bulk_general(recipients: List[Dict[str, str]], subject: str, html_body: str, text_body: str) -> bool:
        """Envía un mismo correo a varios destinatarios (personaliza nombre/preheader)."""
        ok_any = False
        for r in recipients or []:
            email = (r.get("email") or "").strip()
            if not email:
                continue
            nombre = (r.get("nombre") or "Usuario").strip()
            ok = NotificationService.enviar_notificacion_general(
                destinatario_email=email,
                destinatario_nombre=nombre,
                asunto=subject,
                mensaje_html=html_body,
                mensaje_texto=text_body,
            )
            ok_any = ok_any or bool(ok)
        return ok_any

    @staticmethod
    def _details_row(label: str, value: str) -> str:
        left = "padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e5e7eb;font-weight:700;width:170px;vertical-align:top;"
        right = "padding:10px 12px;border-bottom:1px solid #e5e7eb;vertical-align:top;"
        return f"<tr><td style=\"{left}\">{NotificationService._escape_html(label)}</td><td style=\"{right}\">{value}</td></tr>"

    @staticmethod
    def _details_table(rows: List[str]) -> str:
        if not rows:
            return ""
        return (
            "<div style=\"border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;margin:14px 0;\">"
            "<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" style=\"border-collapse:collapse;font-size:14px;\">"
            + "".join(rows) +
            "</table></div>"
        )
    @staticmethod
    def _resolve_logo_path() -> Optional[str]:
        """
        Outlook-friendly:
        - Preferir PNG/JPG/JPEG (Outlook los renderiza bien con CID)
        - SVG solo como ÚLTIMO fallback (Outlook puede no mostrarlo)
        Orden:
          1) EMAIL_LOGO_PATH (si existe)
          2) Ruta absoluta del usuario (PNG primero)
          3) Ruta relativa del proyecto static/images (PNG/JPG primero)
          4) SVG fallback
        """
        forced = os.getenv("EMAIL_LOGO_PATH", "").strip()
        if forced and os.path.exists(forced):
            return forced

        # Rutas absolutas sugeridas (preferir PNG)
        # Preferida (según tu ruta real actual)
        abs_png = r"C:\Users\sinventarios\source\repos\sugipq\static\images\qualitas_logo.png"

        # Fallbacks por si el nombre del archivo cambia
        abs_png_alt = r"C:\Users\sinventarios\source\repos\sugipq\static\images\Qualitas_Logo.png"
        abs_jpg = r"C:\Users\sinventarios\source\repos\sugipq\static\images\Qualitas_Logo.jpg"
        abs_jpeg = r"C:\Users\sinventarios\source\repos\sugipq\static\images\Qualitas_Logo.jpeg"
        abs_svg = r"C:\Users\sinventarios\source\repos\sugipq\static\images\Qualitas_Logo.svg"


        for p in (abs_png, abs_png_alt, abs_jpg, abs_jpeg, abs_svg):
            if os.path.exists(p):
                return p

        # Ruta relativa al proyecto
        try:
            root = Path(__file__).resolve().parent.parent
            base = root / "static" / "images"
            rel_candidates = [

                base / "qualitas_logo.png",

                base / "qualitas_logo.jpg",

                base / "qualitas_logo.jpeg",

                base / "Qualitas_Logo.png",

                base / "Qualitas_Logo.jpg",

                base / "Qualitas_Logo.jpeg",

                base / "Qualitas_Logo.svg",  # fallback

            ]
            for c in rel_candidates:
                if c.exists():
                    return str(c)
        except Exception:
            pass

        return None

    @staticmethod
    def _attach_inline_logo(msg_related: MIMEMultipart) -> bool:
        """
        Adjunta el logo como inline (CID).
        - Outlook: PNG/JPG/JPEG OK.
        - SVG: puede NO verse en Outlook (solo fallback).
        """
        logo_path = NotificationService._resolve_logo_path()
        if not logo_path:
            logger.warning(
                "Logo para emails no encontrado. "
                "Recomendado para Outlook: qualitas_logo.png en static/images/ y/o EMAIL_LOGO_PATH."
            )
            return False

        try:
            ext = os.path.splitext(logo_path)[1].lower()

            with open(logo_path, "rb") as f:
                data = f.read()

            cid = "<%s>" % NotificationService.BRAND["logo_cid"]

            # Preferido para Outlook
            if ext in (".png", ".jpg", ".jpeg"):
                img = MIMEImage(data)
                img.add_header("Content-ID", cid)
                img.add_header("Content-Disposition", "inline", filename=os.path.basename(logo_path))
                msg_related.attach(img)
                return True

            # Fallback SVG (NO recomendado para Outlook)
            if ext == ".svg":
                part = MIMEBase("image", "svg+xml")
                part.set_payload(data)
                encoders.encode_base64(part)
                part.add_header("Content-ID", cid)
                part.add_header("Content-Disposition", "inline", filename=os.path.basename(logo_path))
                part.add_header("Content-Type", "image/svg+xml")
                msg_related.attach(part)

                logger.warning(
                    "Logo SVG embebido. Outlook puede no renderizarlo. "
                    "Recomendado: exportar a Qualitas_Logo.png y usar EMAIL_LOGO_PATH al PNG."
                )
                return True

            logger.warning("Extensión de logo no soportada: %s. Use PNG/JPG/JPEG.", sanitizar_log_text(ext))
            return False

        except Exception:
            logger.exception("Error adjuntando logo inline")
            return False


    @staticmethod
    def _wrap_html(title: str, body_html: str, preheader: str = "", include_logo: bool = False) -> str:
        """Plantilla corporativa compatible con clientes de correo (Outlook-friendly).

        - Suave (tarjeta, bordes redondeados, sombras sutiles).
        - Corporativa (morado/azul/gris).
        - Logo opcional (por defecto deshabilitado).
        """
        blue = NotificationService.BRAND["blue"]
        gray = NotificationService.BRAND["gray"]
        purple = NotificationService.BRAND["purple"]
        app_name = NotificationService.BRAND["app_name"]
        company = NotificationService.BRAND["company"]
        logo_cid = NotificationService.BRAND["logo_cid"]

        preheader_html = ""
        if preheader:
            preheader_html = """
            <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
                %s
            </div>
            """ % preheader

        # Header (logo opcional). Si no hay logo, se usa un badge simple.
        if include_logo:
            right_block = """                    <img src="cid:%s" alt="Qualitas" width="160" height="40"
                         style="display:block;border:0;outline:none;text-decoration:none;-ms-interpolation-mode:bicubic;">
            """ % logo_cid
        else:
            right_block = """                    <div style="width:34px;height:34px;border-radius:999px;background:%s;
                                color:#ffffff;font-weight:900;font-size:16px;line-height:34px;text-align:center;">
                      Q
                    </div>
            """ % purple

        return """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
  <title>%s</title>
  <style>
    /* Algunos clientes lo ignoran, pero ayuda a los que sí lo respetan */
    .details { width: 100%%; border-collapse: collapse; font-size: 14px; }
    .details td { padding: 8px 10px; border-bottom: 1px solid #e5e7eb; vertical-align: top; }
    .details td:first-child { font-weight: 700; background: #f8fafc; width: 170px; }
  </style>
</head>

<body style="margin:0;padding:0;background:%s;font-family:Arial,Helvetica,sans-serif;color:#111827;">
  %s

  <table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="background:%s;padding:28px 0;">
    <tr>
      <td align="center">

        <table role="presentation" width="640" cellpadding="0" cellspacing="0"
               style="background:#ffffff;border-radius:14px;overflow:hidden;
                      border:1px solid rgba(15,23,42,0.10);
                      box-shadow:0 6px 18px rgba(0,0,0,0.06);">

          <!-- Franja morada superior -->
          <tr>
            <td style="background:%s;height:6px;line-height:6px;font-size:0;">&nbsp;</td>
          </tr>

          <!-- Header azul -->
          <tr>
            <td style="background:%s;padding:18px 22px;">
              <table role="presentation" width="100%%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="vertical-align:middle;">
                    <div style="color:#ffffff;font-size:13px;font-weight:800;letter-spacing:0.2px;margin:0;">
                      %s
                    </div>
                    <div style="color:#e6f7fb;font-size:12px;margin-top:4px;">
                      %s
                    </div>
                  </td>

                  <td align="right" style="vertical-align:middle;">
%s
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Título -->
          <tr>
            <td style="padding:18px 22px 0 22px;">
              <div style="font-size:18px;font-weight:900;color:#0f172a;margin:0;">
                %s
              </div>
              <div style="height:4px;width:72px;background:%s;border-radius:4px;margin-top:10px;"></div>
            </td>
          </tr>

          <!-- Cuerpo -->
          <tr>
            <td style="padding:14px 22px 22px 22px;font-size:14px;line-height:1.65;">
              %s
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:14px 22px;background:#f7fafc;color:#6b7280;font-size:12px;line-height:1.5;">
              Mensaje automático — por favor no responder.<br>
              © %s %s
            </td>
          </tr>

        </table>

      </td>
    </tr>
  </table>
</body>
</html>
""" % (
            title,
            gray,
            preheader_html,
            gray,
            purple,
            blue,
            company,
            app_name,
            right_block,
            title,
            purple,
            body_html,
            datetime.now().year,
            company,
        )


    @staticmethod
    def _build_related_message(to_email: str, subject: str, plain_text: str, inner_html: str, preheader: str = "") -> MIMEMultipart:
        msg = MIMEMultipart("related")
        alt = MIMEMultipart("alternative")
        msg.attach(alt)

        msg["From"] = NotificationService.SMTP_CONFIG["from_email"]
        msg["To"] = to_email
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject

        if plain_text:
            alt.attach(MIMEText(plain_text, "plain", "utf-8"))

        include_logo = NotificationService._include_logo()
        html = NotificationService._wrap_html(subject, inner_html, preheader=preheader, include_logo=include_logo)
        alt.attach(MIMEText(html, "html", "utf-8"))

        # Logo opcional (por defecto deshabilitado)
        if include_logo:
            NotificationService._attach_inline_logo(msg)

        return msg

    @staticmethod
    def _connect_smtp():
        try:
            server = NotificationService.SMTP_CONFIG["server"]
            port = NotificationService.SMTP_CONFIG["port"]
            use_tls = NotificationService.SMTP_CONFIG["use_tls"]

            logger.info("Conectando SMTP: %s:%s", sanitizar_log_text(server), sanitizar_log_text(port))
            smtp = smtplib.SMTP(server, port, timeout=10)
            smtp.ehlo()

            if use_tls:
                smtp.starttls()
                smtp.ehlo()

            if NotificationService.SMTP_CONFIG["username"] and NotificationService.SMTP_CONFIG["password"]:
                smtp.login(NotificationService.SMTP_CONFIG["username"], NotificationService.SMTP_CONFIG["password"])

            logger.info("Conexión SMTP exitosa")
            return smtp

        except Exception:
            logger.exception("Error conectando al SMTP")
            return None

    @staticmethod
    def _send_email_smtp(msg):
        smtp = None
        try:
            smtp = NotificationService._connect_smtp()
            if not smtp:
                logger.error("No se pudo conectar al servidor SMTP")
                return False

            smtp.send_message(msg)
            logger.info("Email enviado exitosamente a %s", sanitizar_log_text(NotificationService._mask_email(msg.get("To") or "")))
            return True

        except Exception:
            logger.exception("Error enviando email")
            return False

        finally:
            if smtp:
                try:
                    smtp.quit()
                except Exception:
                    pass

    @staticmethod
    def enviar_notificacion_asignacion_con_confirmacion(
        destinatario_email,
        destinatario_nombre,
        producto_info,
        cantidad,
        oficina_nombre,
        asignador_nombre,
        token_confirmacion,
        base_url
    ):
        try:
            # En producción, si base_url viene vacío, usamos APP_BASE_URL
            base_url = (base_url or os.getenv("APP_BASE_URL", "")).strip().rstrip("/")
            if not base_url:
                logger.error("APP_BASE_URL/base_url no configurado; no se puede construir el link de confirmación")
                return False

            if not destinatario_email:
                logger.error("Email del destinatario es requerido")
                return False
            if not token_confirmacion:
                logger.error("Token de confirmación es requerido")
                return False

            confirmacion_url = "%s/confirmacion/verificar/%s" % (base_url, token_confirmacion)

            producto_info = producto_info or {}
            producto_nombre = producto_info.get("nombre", "Producto de inventario")
            producto_codigo = producto_info.get("codigo_unico", "N/A")
            producto_categoria = producto_info.get("categoria", "General")

            subject = "📦 Asignación de Inventario - %s" % producto_nombre

            btn_color = NotificationService.BRAND["blue"]
            badge_color = NotificationService.BRAND["purple"]

            inner_html = """
<p>Estimado/a <strong>%s</strong>,</p>
<p>Se le ha asignado un producto del inventario corporativo. Por favor confirme la recepción:</p>

<div style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:10px;padding:14px;margin:16px 0;">
  <div style="font-weight:800;margin-bottom:8px;">📋 Detalles de la asignación</div>
  <table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="font-size:14px;">
    <tr><td style="padding:4px 0;"><b>Producto:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Código:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Categoría:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Cantidad:</b></td><td style="padding:4px 0;">%s unidad(es)</td></tr>
    <tr><td style="padding:4px 0;"><b>Oficina destino:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Asignado por:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Fecha:</b></td><td style="padding:4px 0;">%s</td></tr>
  </table>
</div>

<div style="padding:10px 12px;border-left:4px solid %s;background:#fbf5fb;border-radius:8px;margin:16px 0;">
  <b>Importante:</b> el enlace de confirmación es válido por <b>8 días</b>.
</div>

<div style="text-align:center;margin:18px 0;">
  <a href="%s"
     style="display:inline-block;background:%s;color:#ffffff;text-decoration:none;
            padding:12px 18px;border-radius:10px;font-weight:800;">
    ✅ Confirmar recepción
  </a>
</div>

<p style="margin-top:10px;">Si el botón no funciona, copie y pegue este enlace en su navegador:</p>
<p style="word-break:break-all;"><small>%s</small></p>

<p>Si no ha recibido el producto o hay alguna inconsistencia, contacte al área de inventarios.</p>
""" % (
                destinatario_nombre,
                producto_nombre,
                producto_codigo,
                producto_categoria,
                cantidad,
                oficina_nombre,
                asignador_nombre,
                datetime.now().strftime("%d/%m/%Y %H:%M"),
                badge_color,
                confirmacion_url,
                btn_color,
                confirmacion_url,
            )

            text_content = """
ASIGNACIÓN DE INVENTARIO

Estimado/a %s,

Producto: %s
Código: %s
Categoría: %s
Cantidad: %s unidad(es)
Oficina destino: %s
Asignado por: %s
Fecha: %s

IMPORTANTE: Debe confirmar la recepción (válido por 8 días)
Enlace:
%s

--
Mensaje automático. No responder.
""" % (
                destinatario_nombre,
                producto_nombre,
                producto_codigo,
                producto_categoria,
                cantidad,
                oficina_nombre,
                asignador_nombre,
                datetime.now().strftime("%d/%m/%Y %H:%M"),
                confirmacion_url,
            )

            msg = NotificationService._build_related_message(
                to_email=destinatario_email,
                subject=subject,
                plain_text=text_content,
                inner_html=inner_html,
                preheader="Asignación: %s" % producto_nombre
            )

            return NotificationService._send_email_smtp(msg)

        except Exception:
            logger.exception("Error en enviar_notificacion_asignacion_con_confirmacion")
            return False

    @staticmethod
    def enviar_notificacion_asignacion_simple(
        destinatario_email,
        destinatario_nombre,
        producto_info,
        cantidad,
        oficina_nombre,
        asignador_nombre
    ):
        try:
            if not destinatario_email:
                logger.error("Email del destinatario es requerido")
                return False

            producto_info = producto_info or {}
            producto_nombre = producto_info.get("nombre", "Producto de inventario")
            producto_codigo = producto_info.get("codigo_unico", "N/A")
            producto_categoria = producto_info.get("categoria", "General")

            subject = "📦 Asignación de Inventario - %s" % producto_nombre

            inner_html = """
<p>Estimado/a <strong>%s</strong>,</p>
<p>Se le ha asignado un producto del inventario corporativo:</p>

<div style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:10px;padding:14px;margin:16px 0;">
  <div style="font-weight:800;margin-bottom:8px;">📋 Detalles de la asignación</div>
  <table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="font-size:14px;">
    <tr><td style="padding:4px 0;"><b>Producto:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Código:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Categoría:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Cantidad:</b></td><td style="padding:4px 0;">%s unidad(es)</td></tr>
    <tr><td style="padding:4px 0;"><b>Oficina destino:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Asignado por:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Fecha:</b></td><td style="padding:4px 0;">%s</td></tr>
  </table>
</div>

<p>Si existe algún error o discrepancia, por favor contacte al área de inventarios.</p>
""" % (
                destinatario_nombre,
                producto_nombre,
                producto_codigo,
                producto_categoria,
                cantidad,
                oficina_nombre,
                asignador_nombre,
                datetime.now().strftime("%d/%m/%Y %H:%M"),
            )

            text_content = """
ASIGNACIÓN DE INVENTARIO

Estimado/a %s,

Producto: %s
Código: %s
Categoría: %s
Cantidad: %s unidad(es)
Oficina destino: %s
Asignado por: %s
Fecha: %s

Si existe algún error o discrepancia, contacte al área de inventarios.

--
Mensaje automático. No responder.
""" % (
                destinatario_nombre,
                producto_nombre,
                producto_codigo,
                producto_categoria,
                cantidad,
                oficina_nombre,
                asignador_nombre,
                datetime.now().strftime("%d/%m/%Y %H:%M"),
            )

            msg = NotificationService._build_related_message(
                to_email=destinatario_email,
                subject=subject,
                plain_text=text_content,
                inner_html=inner_html,
                preheader="Asignación: %s" % producto_nombre
            )

            return NotificationService._send_email_smtp(msg)

        except Exception:
            logger.exception("Error en enviar_notificacion_asignacion_simple")
            return False

    @staticmethod
    def enviar_notificacion_confirmacion_exitosa(
        destinatario_email,
        destinatario_nombre,
        producto_info,
        asignador_nombre
    ):
        try:
            if not destinatario_email:
                logger.error("Email del destinatario es requerido")
                return False

            producto_info = producto_info or {}
            producto_nombre = producto_info.get("nombre", "Producto de inventario")
            producto_codigo = producto_info.get("codigo_unico", "N/A")

            subject = "✅ Confirmación de Recepción - %s" % producto_nombre

            inner_html = """
<p>Estimado/a <strong>%s</strong>,</p>
<p>La recepción del producto asignado fue <strong>confirmada exitosamente</strong>.</p>

<div style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:10px;padding:14px;margin:16px 0;">
  <div style="font-weight:800;margin-bottom:8px;">📋 Detalles</div>
  <table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="font-size:14px;">
    <tr><td style="padding:4px 0;"><b>Producto:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Código:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Asignador:</b></td><td style="padding:4px 0;">%s</td></tr>
    <tr><td style="padding:4px 0;"><b>Fecha confirmación:</b></td><td style="padding:4px 0;">%s</td></tr>
  </table>
</div>

<p><strong>Estado:</strong> Confirmado.</p>
""" % (
                destinatario_nombre,
                producto_nombre,
                producto_codigo,
                asignador_nombre,
                datetime.now().strftime("%d/%m/%Y %H:%M"),
            )

            text_content = """
CONFIRMACIÓN DE RECEPCIÓN EXITOSA

Destinatario: %s
Producto: %s
Código: %s
Asignador: %s
Fecha confirmación: %s

Estado: Confirmado

--
Mensaje automático. No responder.
""" % (
                destinatario_nombre,
                producto_nombre,
                producto_codigo,
                asignador_nombre,
                datetime.now().strftime("%d/%m/%Y %H:%M"),
            )

            msg = NotificationService._build_related_message(
                to_email=destinatario_email,
                subject=subject,
                plain_text=text_content,
                inner_html=inner_html,
                preheader="Confirmación: %s" % producto_nombre
            )

            return NotificationService._send_email_smtp(msg)

        except Exception:
            logger.exception("Error en enviar_notificacion_confirmacion_exitosa")
            return False

    @staticmethod
    def enviar_notificacion_general(
        destinatario_email,
        destinatario_nombre,
        asunto,
        mensaje_html,
        mensaje_texto=None
    ):
        try:
            if not destinatario_email:
                logger.error("Email del destinatario es requerido")
                return False

            inner_html = mensaje_html or "<p></p>"

            msg = NotificationService._build_related_message(
                to_email=destinatario_email,
                subject=asunto,
                plain_text=(mensaje_texto or ""),
                inner_html=inner_html,
                preheader=(destinatario_nombre or "")
            )

            return NotificationService._send_email_smtp(msg)

        except Exception:
            logger.exception("Error en enviar_notificacion_general")
            return False

    @staticmethod
    def test_conexion_smtp():
        try:
            smtp = NotificationService._connect_smtp()
            if smtp:
                smtp.quit()
                return {
                    "success": True,
                    "message": "Conexión SMTP exitosa",
                    "config": {
                        "server": NotificationService.SMTP_CONFIG["server"],
                        "port": NotificationService.SMTP_CONFIG["port"],
                        "use_tls": NotificationService.SMTP_CONFIG["use_tls"],
                        "from_email": NotificationService.SMTP_CONFIG["from_email"],
                    },
                }

            return {
                "success": False,
                "message": "No se pudo conectar al servidor SMTP",
                "config": NotificationService.SMTP_CONFIG,
            }

        except Exception as e:
            return {
                "success": False,
                "message": "Error: %s" % 'Error interno',
                "config": NotificationService.SMTP_CONFIG,
            }



    # ==============================
    # Utilidades internas (HTML)
    # ==============================

    @staticmethod
    def _escape_html(value) -> str:
        """Escapa valores para ser usados en HTML."""
        try:
            return html.escape("" if value is None else str(value), quote=True)
        except Exception:
            return ""

    # ==============================
    # Notificaciones adicionales
    # ==============================

    @staticmethod
    def notificar_cambio_estado_solicitud(
        solicitud_info: dict,
        estado_anterior: str,
        estado_nuevo: str,
        usuario_gestion: Optional[str] = None,
        observaciones: Optional[str] = None,
    ) -> bool:
        """Notifica cambios de estado de una solicitud.
        Siempre notifica:
        - Solicitante (mensaje tipo "Tu solicitud fue ...")
        - Aprobadores activos (mensaje tipo "Solicitud gestionada ...")
        """
        info = solicitud_info or {}
        sid = info.get("id", "N/A")
        material = info.get("material_nombre") or info.get("material") or ""
        cantidad = info.get("cantidad_solicitada") or info.get("cantidad") or ""
        oficina = info.get("oficina_nombre") or ""
        solicitante_nombre = info.get("usuario_solicitante", "Usuario")
        solicitante_email = info.get("email_solicitante") or ""

        base_url = os.getenv("APP_BASE_URL", "").strip().rstrip("/")
        link_solicitudes = f"{base_url}/solicitudes" if base_url else ""

        # Construir tabla de detalle
        rows = [
            NotificationService._details_row("Solicitud", f"#{NotificationService._escape_html(sid)}"),
        ]
        if material:
            rows.append(NotificationService._details_row("Material", NotificationService._escape_html(material)))
        if cantidad:
            rows.append(NotificationService._details_row("Cantidad", NotificationService._escape_html(cantidad)))
        if oficina:
            rows.append(NotificationService._details_row("Oficina", NotificationService._escape_html(oficina)))
        rows.append(NotificationService._details_row("Estado anterior", NotificationService._escape_html(estado_anterior)))
        rows.append(NotificationService._details_row("Estado nuevo", NotificationService._escape_html(estado_nuevo)))
        if usuario_gestion:
            rows.append(NotificationService._details_row("Gestionado por", NotificationService._escape_html(usuario_gestion)))
        if observaciones:
            rows.append(NotificationService._details_row("Observaciones", NotificationService._escape_html(observaciones)))

        table = NotificationService._details_table(rows)

        # CTA
        cta = ""
        if link_solicitudes:
            cta = f"""<div style='text-align:center;margin:16px 0;'>
                <a href='{NotificationService._escape_html(link_solicitudes)}'
                   style='display:inline-block;background:{NotificationService.BRAND["blue"]};color:#fff;text-decoration:none;
                          padding:12px 18px;border-radius:10px;font-weight:800;'>
                   Ver solicitud
                </a>
            </div>"""

        # 1) Solicitante
        ok_solicitante = False
        if solicitante_email:
            subject_s = f"📌 Tu solicitud #{sid} fue {estado_nuevo}"
            html_s = (
                f"<p>Hola <b>{NotificationService._escape_html(solicitante_nombre)}</b>,</p>"
                f"<p>Tu solicitud ha sido gestionada y cambió de estado.</p>"
                f"{table}"
                f"{cta}"
            )
            txt_s = "\n".join([
                f"Hola {solicitante_nombre}, tu solicitud #{sid} fue {estado_nuevo}.",
                f"Estado anterior: {estado_anterior}",
                f"Estado nuevo: {estado_nuevo}",
                f"Gestionado por: {usuario_gestion}" if usuario_gestion else "",
                f"Observaciones: {observaciones}" if observaciones else "",
                f"Link: {link_solicitudes}" if link_solicitudes else "",
            ]).strip()
            ok_solicitante = NotificationService.enviar_notificacion_general(
                solicitante_email, solicitante_nombre, subject_s, html_s, txt_s
            )

        # 2) Aprobadores
        aprobadores = NotificationService._obtener_aprobadores_activos()
        recipients_aprob = [{"nombre": a.get("nombre", "Aprobador"), "email": a.get("email", "")} for a in aprobadores if a.get("email")]
        ok_aprob = False
        if recipients_aprob:
            subject_a = f"📌 Solicitud #{sid} gestionada: {estado_nuevo}"
            html_a = (
                f"<p>Hola,</p>"
                f"<p>Se gestionó una solicitud.</p>"
                f"<div style='margin:10px 0;color:#374151;'><b>Solicitante:</b> {NotificationService._escape_html(solicitante_nombre)}"
                + (f" &lt;{NotificationService._escape_html(solicitante_email)}&gt;" if solicitante_email else "")
                + "</div>"
                f"{table}"
                f"{cta}"
            )
            txt_a = "\n".join([
                f"Solicitud #{sid} gestionada: {estado_nuevo}",
                f"Solicitante: {solicitante_nombre} ({solicitante_email})" if solicitante_email else f"Solicitante: {solicitante_nombre}",
                f"Estado anterior: {estado_anterior}",
                f"Estado nuevo: {estado_nuevo}",
                f"Gestionado por: {usuario_gestion}" if usuario_gestion else "",
                f"Observaciones: {observaciones}" if observaciones else "",
                f"Link: {link_solicitudes}" if link_solicitudes else "",
            ]).strip()
            ok_aprob = NotificationService._send_bulk_general(recipients_aprob, subject_a, html_a, txt_a)

        return bool(ok_solicitante or ok_aprob)



    @staticmethod
    def notificar_novedad_registrada(solicitud_info: dict, novedad_info: Optional[dict] = None) -> bool:
        """Notifica registro/gestión de novedades.
        - Solicitante: "Se registró una novedad en tu solicitud"
        - Aprobadores: "Novedad registrada en solicitud"
        """
        info = solicitud_info or {}
        sid = info.get("id", "N/A")
        material = info.get("material_nombre") or info.get("material") or ""
        solicitante_nombre = info.get("usuario_solicitante", "Usuario")
        solicitante_email = info.get("email_solicitante") or ""

        ninfo = novedad_info or {}
        tipo = ninfo.get("tipo") or ninfo.get("tipo_novedad") or "Novedad"
        descripcion = ninfo.get("descripcion") or ""
        cantidad_afectada = ninfo.get("cantidad_afectada")
        usuario_registra = ninfo.get("usuario_registra") or ninfo.get("usuario") or ""

        base_url = os.getenv("APP_BASE_URL", "").strip().rstrip("/")
        link_solicitudes = f"{base_url}/solicitudes" if base_url else ""

        rows = [
            NotificationService._details_row("Solicitud", f"#{NotificationService._escape_html(sid)}"),
        ]
        if material:
            rows.append(NotificationService._details_row("Material", NotificationService._escape_html(material)))
        rows.append(NotificationService._details_row("Tipo", NotificationService._escape_html(tipo)))
        if cantidad_afectada is not None:
            rows.append(NotificationService._details_row("Cantidad afectada", NotificationService._escape_html(cantidad_afectada)))
        if usuario_registra:
            rows.append(NotificationService._details_row("Registrado por", NotificationService._escape_html(usuario_registra)))
        if descripcion:
            rows.append(NotificationService._details_row("Descripción", NotificationService._escape_html(descripcion)))

        table = NotificationService._details_table(rows)

        cta = ""
        if link_solicitudes:
            cta = f"""<div style='text-align:center;margin:16px 0;'>
                <a href='{NotificationService._escape_html(link_solicitudes)}'
                   style='display:inline-block;background:{NotificationService.BRAND["purple"]};color:#fff;text-decoration:none;
                          padding:12px 18px;border-radius:10px;font-weight:800;'>
                   Ver solicitud
                </a>
            </div>"""

        ok_solicitante = False
        if solicitante_email:
            subject_s = f"⚠️ Se registró una novedad en tu solicitud #{sid}"
            html_s = (
                f"<p>Hola <b>{NotificationService._escape_html(solicitante_nombre)}</b>,</p>"
                f"<p>Se registró una novedad asociada a tu solicitud.</p>"
                f"{table}"
                f"{cta}"
            )
            txt_s = "\n".join([
                f"Hola {solicitante_nombre}, se registró una novedad en tu solicitud #{sid}.",
                f"Tipo: {tipo}",
                f"Cantidad afectada: {cantidad_afectada}" if cantidad_afectada is not None else "",
                f"Registrado por: {usuario_registra}" if usuario_registra else "",
                f"Descripción: {descripcion}" if descripcion else "",
                f"Link: {link_solicitudes}" if link_solicitudes else "",
            ]).strip()
            ok_solicitante = NotificationService.enviar_notificacion_general(
                solicitante_email, solicitante_nombre, subject_s, html_s, txt_s
            )

        aprobadores = NotificationService._obtener_aprobadores_activos()
        recipients_aprob = [{"nombre": a.get("nombre", "Aprobador"), "email": a.get("email", "")} for a in aprobadores if a.get("email")]
        ok_aprob = False
        if recipients_aprob:
            subject_a = f"⚠️ Novedad registrada - Solicitud #{sid}"
            html_a = (
                f"<p>Hola,</p>"
                f"<p>Se registró una novedad en una solicitud.</p>"
                f"<div style='margin:10px 0;color:#374151;'><b>Solicitante:</b> {NotificationService._escape_html(solicitante_nombre)}"
                + (f" &lt;{NotificationService._escape_html(solicitante_email)}&gt;" if solicitante_email else "")
                + "</div>"
                f"{table}"
                f"{cta}"
            )
            txt_a = "\n".join([
                f"Novedad registrada - Solicitud #{sid}",
                f"Solicitante: {solicitante_nombre} ({solicitante_email})" if solicitante_email else f"Solicitante: {solicitante_nombre}",
                f"Tipo: {tipo}",
                f"Cantidad afectada: {cantidad_afectada}" if cantidad_afectada is not None else "",
                f"Registrado por: {usuario_registra}" if usuario_registra else "",
                f"Descripción: {descripcion}" if descripcion else "",
                f"Link: {link_solicitudes}" if link_solicitudes else "",
            ]).strip()
            ok_aprob = NotificationService._send_bulk_general(recipients_aprob, subject_a, html_a, txt_a)

        return bool(ok_solicitante or ok_aprob)



    @staticmethod
    def notificar_prestamo_creado(prestamo_info: dict) -> bool:
        """Notifica al solicitante cuando se registra un préstamo."""
        info = prestamo_info or {}
        email = info.get("email_solicitante")
        if not email:
            return False

        nombre = info.get("solicitante_nombre", "Usuario")
        pid = info.get("id", "N/A")
        material = info.get("material") or ""
        cantidad = info.get("cantidad")
        oficina = info.get("oficina_nombre") or ""
        evento = info.get("evento") or ""
        fecha_prevista = info.get("fecha_prevista") or ""

        subject = f"📌 Préstamo #{pid} registrado"

        rows = []
        rows.append(f"<tr><td><b>Préstamo</b></td><td>#{NotificationService._escape_html(pid)}</td></tr>")
        if material:
            rows.append(f"<tr><td><b>Material</b></td><td>{NotificationService._escape_html(material)}</td></tr>")
        if cantidad is not None:
            rows.append(f"<tr><td><b>Cantidad</b></td><td>{NotificationService._escape_html(cantidad)}</td></tr>")
        if oficina:
            rows.append(f"<tr><td><b>Oficina</b></td><td>{NotificationService._escape_html(oficina)}</td></tr>")
        if evento:
            rows.append(f"<tr><td><b>Evento</b></td><td>{NotificationService._escape_html(evento)}</td></tr>")
        if fecha_prevista:
            rows.append(f"<tr><td><b>Fecha prevista</b></td><td>{NotificationService._escape_html(fecha_prevista)}</td></tr>")

        table = "<table class='details'>" + "".join(rows) + "</table>"

        html_body = (
            f"<p>Hola <b>{NotificationService._escape_html(nombre)}</b>,</p>"
            f"<p>Tu préstamo fue registrado en el sistema.</p>"
            f"{table}"
        )

        txt_lines = [f"Hola {nombre}, tu préstamo #{pid} fue registrado."]
        if material:
            txt_lines.append(f"Material: {material}")
        if cantidad is not None:
            txt_lines.append(f"Cantidad: {cantidad}")
        if oficina:
            txt_lines.append(f"Oficina: {oficina}")
        if evento:
            txt_lines.append(f"Evento: {evento}")
        if fecha_prevista:
            txt_lines.append(f"Fecha prevista: {fecha_prevista}")

        return NotificationService.enviar_notificacion_general(
            email, nombre, subject, html_body, "\n".join(txt_lines)
        )

    @staticmethod
    def notificar_cambio_estado_prestamo(
        prestamo_info: dict,
        estado_nuevo: str,
        usuario_responsable: Optional[str] = None,
        comentario: Optional[str] = None,
    ) -> bool:
        """Notifica al solicitante el cambio de estado de un préstamo."""
        info = prestamo_info or {}
        email = info.get("email_solicitante")
        if not email:
            return False

        nombre = info.get("solicitante_nombre", "Usuario")
        pid = info.get("id", "N/A")
        material = info.get("material") or ""
        cantidad = info.get("cantidad")
        oficina = info.get("oficina_nombre") or ""

        subject = f"📌 Préstamo #{pid} - {estado_nuevo}"

        rows = []
        rows.append(f"<tr><td><b>Préstamo</b></td><td>#{NotificationService._escape_html(pid)}</td></tr>")
        if material:
            rows.append(f"<tr><td><b>Material</b></td><td>{NotificationService._escape_html(material)}</td></tr>")
        if cantidad is not None:
            rows.append(f"<tr><td><b>Cantidad</b></td><td>{NotificationService._escape_html(cantidad)}</td></tr>")
        if oficina:
            rows.append(f"<tr><td><b>Oficina</b></td><td>{NotificationService._escape_html(oficina)}</td></tr>")
        rows.append(f"<tr><td><b>Estado nuevo</b></td><td>{NotificationService._escape_html(estado_nuevo)}</td></tr>")
        if usuario_responsable:
            rows.append(f"<tr><td><b>Gestionado por</b></td><td>{NotificationService._escape_html(usuario_responsable)}</td></tr>")
        if comentario:
            rows.append(f"<tr><td><b>Observaciones</b></td><td>{NotificationService._escape_html(comentario)}</td></tr>")

        table = "<table class='details'>" + "".join(rows) + "</table>"

        html_body = (
            f"<p>Hola <b>{NotificationService._escape_html(nombre)}</b>,</p>"
            f"<p>Tu préstamo ha cambiado de estado.</p>"
            f"{table}"
        )

        txt_lines = [
            f"Hola {nombre}, tu préstamo #{pid} cambió de estado.",
            f"Estado nuevo: {estado_nuevo}",
        ]
        if material:
            txt_lines.insert(1, f"Material: {material}")
        if cantidad is not None:
            txt_lines.insert(2, f"Cantidad: {cantidad}")
        if oficina:
            txt_lines.append(f"Oficina: {oficina}")
        if usuario_responsable:
            txt_lines.append(f"Gestionado por: {usuario_responsable}")
        if comentario:
            txt_lines.append(f"Observaciones: {comentario}")

        return NotificationService.enviar_notificacion_general(
            email, nombre, subject, html_body, "\n".join(txt_lines)
        )
    # Compatibilidad (por si otros módulos lo llaman)
    @staticmethod
    def notificar_solicitud_creada(solicitud_info: dict) -> bool:
        """Notifica creación de solicitud.
        - Al solicitante: confirmación (Tu solicitud fue creada)
        - A aprobadores: aviso de nueva solicitud pendiente
        """
        info = solicitud_info or {}
        sid = info.get("id", "N/A")
        material = info.get("material_nombre") or info.get("material") or ""
        cantidad = info.get("cantidad_solicitada") or info.get("cantidad") or ""
        oficina = info.get("oficina_nombre") or ""
        solicitante_nombre = info.get("usuario_solicitante", "Usuario")
        solicitante_email = info.get("email_solicitante") or ""

        base_url = os.getenv("APP_BASE_URL", "").strip().rstrip("/")
        link_solicitudes = f"{base_url}/solicitudes" if base_url else ""

        # 1) Email al solicitante
        ok_solicitante = False
        if solicitante_email:
            subject_s = f"✅ Tu solicitud #{sid} fue creada"
            rows_s = [
                NotificationService._details_row("Solicitud", f"#{NotificationService._escape_html(sid)}"),
            ]
            if material:
                rows_s.append(NotificationService._details_row("Material", NotificationService._escape_html(material)))
            if cantidad:
                rows_s.append(NotificationService._details_row("Cantidad", NotificationService._escape_html(cantidad)))
            if oficina:
                rows_s.append(NotificationService._details_row("Oficina", NotificationService._escape_html(oficina)))

            table_s = NotificationService._details_table(rows_s)

            cta = ""
            if link_solicitudes:
                cta = f"""<div style='text-align:center;margin:16px 0;'>
                    <a href='{NotificationService._escape_html(link_solicitudes)}'
                       style='display:inline-block;background:{NotificationService.BRAND["blue"]};color:#fff;text-decoration:none;
                              padding:12px 18px;border-radius:10px;font-weight:800;'>
                       Ver mis solicitudes
                    </a>
                </div>"""

            html_s = (
                f"<p>Hola <b>{NotificationService._escape_html(solicitante_nombre)}</b>,</p>"
                f"<p>Tu solicitud fue creada exitosamente y quedó registrada en el sistema.</p>"
                f"{table_s}"
                f"{cta}"
                f"<p style='margin-top:10px;color:#6b7280;'>Te notificaremos cuando sea gestionada (aprobación, rechazo, aprobación parcial, devolución o novedades).</p>"
            )
            txt_s = "\n".join(
                [f"Hola {solicitante_nombre}, tu solicitud #{sid} fue creada.",
                 f"Material: {material}" if material else "",
                 f"Cantidad: {cantidad}" if cantidad else "",
                 f"Oficina: {oficina}" if oficina else "",
                 f"Link: {link_solicitudes}" if link_solicitudes else ""]
            ).strip()
            ok_solicitante = NotificationService.enviar_notificacion_general(
                solicitante_email, solicitante_nombre, subject_s, html_s, txt_s
            )

        # 2) Email a aprobadores activos
        aprobadores = NotificationService._obtener_aprobadores_activos()
        emails_aprob = NotificationService._unique_emails([a.get("email", "") for a in aprobadores])
        recipients_aprob = [{"nombre": a.get("nombre", "Aprobador"), "email": a.get("email", "")} for a in aprobadores if a.get("email")]

        ok_aprobadores = False
        if recipients_aprob:
            subject_a = f"📝 Nueva solicitud #{sid} pendiente de aprobación"
            rows_a = [
                NotificationService._details_row("Solicitud", f"#{NotificationService._escape_html(sid)}"),
                NotificationService._details_row("Solicitante", f"{NotificationService._escape_html(solicitante_nombre)}" + (f" &lt;{NotificationService._escape_html(solicitante_email)}&gt;" if solicitante_email else "")),
            ]
            if material:
                rows_a.append(NotificationService._details_row("Material", NotificationService._escape_html(material)))
            if cantidad:
                rows_a.append(NotificationService._details_row("Cantidad", NotificationService._escape_html(cantidad)))
            if oficina:
                rows_a.append(NotificationService._details_row("Oficina", NotificationService._escape_html(oficina)))

            table_a = NotificationService._details_table(rows_a)

            cta_a = ""
            if link_solicitudes:
                cta_a = f"""<div style='text-align:center;margin:16px 0;'>
                    <a href='{NotificationService._escape_html(link_solicitudes)}'
                       style='display:inline-block;background:{NotificationService.BRAND["purple"]};color:#fff;text-decoration:none;
                              padding:12px 18px;border-radius:10px;font-weight:800;'>
                       Revisar y gestionar
                    </a>
                </div>"""

            html_a = (
                f"<p>Hola,</p>"
                f"<p>Se creó una nueva solicitud que requiere gestión.</p>"
                f"{table_a}"
                f"{cta_a}"
            )
            txt_a = "\n".join([
                f"Nueva solicitud #{sid} pendiente de aprobación.",
                f"Solicitante: {solicitante_nombre} ({solicitante_email})" if solicitante_email else f"Solicitante: {solicitante_nombre}",
                f"Material: {material}" if material else "",
                f"Cantidad: {cantidad}" if cantidad else "",
                f"Oficina: {oficina}" if oficina else "",
                f"Link: {link_solicitudes}" if link_solicitudes else "",
            ]).strip()

            ok_aprobadores = NotificationService._send_bulk_general(recipients_aprob, subject_a, html_a, txt_a)

        return bool(ok_solicitante or ok_aprobadores)


def servicio_notificaciones_disponible() -> bool:
    if os.getenv("NOTIFICATIONS_ENABLED", "true").strip().lower() in ("0", "false", "no", "n"):
        return False

    cfg = getattr(NotificationService, "SMTP_CONFIG", {}) or {}
    return bool(cfg.get("server")) and bool(cfg.get("port")) and bool(cfg.get("from_email"))


def notificar_solicitud(solicitud_info: dict) -> bool:
    return NotificationService.notificar_solicitud_creada(solicitud_info)
