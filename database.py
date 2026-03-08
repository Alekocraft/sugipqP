# utils/database.py

import os
import pyodbc
import logging

# Configuración de logging
logger = logging.getLogger(__name__)


def _truthy_env(name: str, default: str = "false") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in ("1", "true", "yes", "y", "si")


class Database:
    """
    Clase para manejar la conexión a SQL Server usando pyodbc.

    Variables de entorno recomendadas:
    - DB_SERVER:  servidor/instancia (ej: TU_SERVIDOR\INSTANCIA o localhost\SQLEXPRESS)
    - DB_NAME:    nombre BD (ej: SistemaGestionInventarios_PROD)
    - DB_DRIVER:  driver ODBC (ej: {ODBC Driver 17 for SQL Server})

    Autenticación:
    - Por defecto usa Windows Auth (Trusted Connection).
      DB_TRUSTED_CONNECTION=true
    - Para SQL Auth:
      DB_TRUSTED_CONNECTION=false, DB_USERNAME, DB_PASSWORD
    """

    def __init__(self):
        flask_env = (os.getenv("FLASK_ENV") or "").strip().lower()
        default_db = "SistemaGestionInventarios_PROD" if flask_env == "production" else "SistemaGestionInventariost"

        self.server = os.getenv("DB_SERVER", "localhost\\SQLEXPRESS")
        self.database = os.getenv("DB_NAME", default_db)
        self.driver = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")

        self.trusted = _truthy_env("DB_TRUSTED_CONNECTION", "true")
        self.username = os.getenv("DB_USERNAME", "")
        self.password = os.getenv("DB_PASSWORD", "")

        # Opcional (solo si tu SQL/infra lo requiere):
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
                    logger.error("DB_USERNAME/DB_PASSWORD son requeridos cuando DB_TRUSTED_CONNECTION=false")
                    return None
                parts.append(f"UID={self.username}")
                parts.append(f"PWD={self.password}")

            if self.encrypt:
                parts.append("Encrypt=yes")
                if self.trust_server_cert:
                    parts.append("TrustServerCertificate=yes")

            conn_str = ";".join(parts) + ";"
            conn = pyodbc.connect(conn_str)
            logger.info(
                "Conexión a BD OK - Servidor: %s - BD: %s - Trusted: %s",
                self.server, self.database, self.trusted
            )
            return conn

        except pyodbc.InterfaceError as e:
            logger.error("Error de interfaz ODBC al conectar a la BD: %s", type(e).__name__)
            return None

        except pyodbc.OperationalError as e:
            logger.error("BD OperationalError: %s | args=%s", str(e), getattr(e, "args", None))
            logger.error(
                "BD Config -> server=%s db=%s driver=%s trusted=%s encrypt=%s trust_cert=%s",
                self.server, self.database, self.driver, self.trusted, self.encrypt, self.trust_server_cert
            )
            return None


        except Exception as e:
            logger.error("Error inesperado al conectar a la BD: %s", type(e).__name__)
            return None


# Instancia global
db = Database()


def get_database_connection():
    """Mantiene compatibilidad con imports existentes."""
    return db.get_connection()
