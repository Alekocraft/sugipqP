# database.py

import os
import logging
import pyodbc
from urllib.parse import urlsplit, parse_qs, unquote_plus

logger = logging.getLogger(__name__)


def _truthy_env(name: str, default: str = "false") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in ("1", "true", "yes", "y", "si")


def _parse_database_url(db_url: str) -> dict:
    """
    Parsea DATABASE_URL tipo:
    mssql+pyodbc://@localhost\\SQLEXPRESS/SistemaGestionInventarios_PROD?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes
    Devuelve dict con server, database, driver, trusted (si se puede inferir).
    """
    try:
        s = urlsplit(db_url)
        netloc = s.netloc or ""
        host = netloc.rsplit("@", 1)[-1] if "@" in netloc else netloc

        database = (s.path or "").lstrip("/") or ""
        q = parse_qs(s.query)

        driver = q.get("driver", [None])[0]
        if driver:
            driver = unquote_plus(driver).strip()
            if not driver.startswith("{"):
                driver = "{" + driver + "}"

        trusted_raw = (q.get("trusted_connection", [None])[0] or q.get("Trusted_Connection", [None])[0])
        trusted = None
        if trusted_raw is not None:
            trusted = str(trusted_raw).strip().lower() in ("yes", "true", "1")

        return {"server": host, "database": database, "driver": driver, "trusted": trusted}
    except Exception:
        return {}


class Database:
    """
    Conexión SQL Server con pyodbc.

    Prioridad de configuración:
    1) DB_SERVER / DB_NAME / DB_DRIVER (recomendado)
    2) DATABASE_URL (fallback)
    3) Defaults según FLASK_ENV
    """

    def __init__(self):
        flask_env = (os.getenv("FLASK_ENV") or "").strip().lower()
        default_db = "SistemaGestionInventarios_PROD" if flask_env == "production" else "SistemaGestionInventariost"

        # Defaults base
        server = os.getenv("DB_SERVER", "localhost\\SQLEXPRESS")
        database = os.getenv("DB_NAME", default_db)
        driver = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
        trusted = _truthy_env("DB_TRUSTED_CONNECTION", "true")

        # Fallback desde DATABASE_URL si DB_* no está completo
        db_url = (os.getenv("DATABASE_URL") or "").strip()
        if db_url and (not os.getenv("DB_NAME") or not os.getenv("DB_SERVER")):
            parsed = _parse_database_url(db_url)
            server = parsed.get("server") or server
            database = parsed.get("database") or database
            driver = parsed.get("driver") or driver
            if parsed.get("trusted") is not None:
                trusted = bool(parsed["trusted"])

        self.server = server
        self.database = database
        self.driver = driver
        self.trusted = trusted

        self.username = os.getenv("DB_USERNAME", "")
        self.password = os.getenv("DB_PASSWORD", "")

        # Opcionales
        self.encrypt = _truthy_env("DB_ENCRYPT", "false")
        self.trust_server_cert = _truthy_env("DB_TRUST_SERVER_CERTIFICATE", "false")

    def get_connection(self):
        """Devuelve una conexión pyodbc o None si falla."""
        try:
            parts = [
                f"DRIVER={self.driver}",
                f"SERVER={self.server}",
                f"DATABASE={self.database}",
            ]

            if self.trusted:
                parts.append("Trusted_Connection=yes")
            else:
                if not self.username or not self.password:
                    logger.error("DB_USERNAME/DB_PASSWORD requeridos cuando DB_TRUSTED_CONNECTION=false")
                    return None
                parts.append(f"UID={self.username}")
                parts.append(f"PWD={self.password}")

            if self.encrypt:
                parts.append("Encrypt=yes")
                if self.trust_server_cert:
                    parts.append("TrustServerCertificate=yes")

            conn_str = ";".join(parts) + ";"
            conn = pyodbc.connect(conn_str, timeout=10)

            logger.info(
                "Conexión a BD OK - Servidor=%s | BD=%s | Trusted=%s",
                self.server, self.database, self.trusted
            )
            return conn

        except Exception as e:
            logger.exception("Error conectando a SQL Server (Servidor=%s | BD=%s)", self.server, self.database)
            return None


db = Database()


def get_database_connection():
    return db.get_connection()
