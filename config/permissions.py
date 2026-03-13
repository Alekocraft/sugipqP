# -*- coding: utf-8 -*-
"""config/permissions.py

Definición central de permisos por rol.

Convenciones:
- modules: módulos visibles / accesibles en UI (dashboard y navegación).
- actions: acciones permitidas por sub-módulo (usadas por validación en rutas y templates).
- office_filter:
    - 'all' => sin filtro por oficina (ve todo)
    - cualquier otro valor => se considera "solo su oficina" (filtro por session.oficina_id)
      *NOTA*: PermissionManager actualmente interpreta cualquier valor != 'all' como 'own'.
"""

from __future__ import annotations

from copy import deepcopy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_office_key(role_key: str) -> str:
    """Retorna el filtro de oficina configurado para el rol.

    - 'all' => puede ver todas las oficinas
    - Cualquier otro string => se usa como filtro por oficina (p.ej. 'CALI')
    """
    role = (role_key or "").strip().lower()
    cfg = ROLE_PERMISSIONS.get(role)
    if not cfg:
        # Fallback: si es una oficina no registrada, aplica filtro por defecto
        if role.startswith("oficina_"):
            return "OFFICE_ONLY"
        return "all"
    return cfg.get("office_filter", "all")


# ---------------------------------------------------------------------------
# Plantillas base por tipo de rol
# ---------------------------------------------------------------------------

# Administrador: acceso total (incluye gestión de usuarios).
ADMIN_PERMS = {
    "modules": [
        "dashboard",
        "material_pop",
        "inventario_corporativo",
        "prestamo_material",
        "reportes",
        "solicitudes",
        "oficinas",
        "novedades",
        "usuarios",
        "aprobadores",
    ],
    "actions": {
        "materiales": ["view", "create", "edit", "delete"],
        "solicitudes": [
            "view",
            "create",
            "edit",
            "delete",
            "approve",
            "reject",
            "partial_approve",
            "return",
        ],
        "oficinas": ["view", "create", "edit", "delete"],
        "aprobadores": ["view", "create", "edit", "delete"],
        "prestamos": [
            "view",
            "view_all",
            "view_own",
            "create",
            "approve",
            "reject",
            "return",
            "manage_materials",
        ],
        "reportes": [
            "view_all",
            "view_own",
            "cobros_view",
            "cobros_cancel",
            "cobros_export",
        ],
        "inventario_corporativo": [
            "view",
            "create",
            "edit",
            "delete",
            "assign",
            "manage_sedes",
            "manage_oficinas",
            "manage_returns",
            "manage_transfers",
            "create_return",
            "create_transfer",
            "request_return",
            "request_transfer",
            "view_reports",
        ],
        "usuarios": ["view", "create", "edit", "delete"],
        "novedades": ["create", "view", "manage", "approve", "reject", "return"],
    },
    "office_filter": "all",
}

# Aprobador: puede ver reportes globales
APPROVER_LIKE_PERMS = {
    "modules": [
        "dashboard",
        "material_pop",
        "inventario_corporativo",
        "prestamo_material",
        "reportes",
        "solicitudes",
        "oficinas",
        "novedades",
        "aprobadores",
    ],
    "actions": {
        "materiales": ["view"],
        "solicitudes": ["view", "create", "approve", "reject", "partial_approve", "return"],
        "oficinas": ["view"],
        "aprobadores": ["view"],
        "prestamos": [
            "view",
            "view_all",
            "view_own",
            "create",
            "approve",
            "reject",
            "return",
            "manage_materials",
        ],
        "reportes": ["view_all"],
        "inventario_corporativo": [
            "view",
            "create",
            "edit",
            "delete",
            "assign",
            "manage_sedes",
            "manage_oficinas",
            "manage_returns",
            "manage_transfers",
            "create_return",
            "create_transfer",
            "view_reports",
        ],
        "novedades": ["create", "view", "manage", "approve", "reject", "return"],
    },
    "office_filter": "all",
}

# Líder de inventario: mismo acceso global a reportes + cobros POP + create/edit Material POP
LIDER_INVENTARIO_PERMS = deepcopy(APPROVER_LIKE_PERMS)
LIDER_INVENTARIO_PERMS.setdefault("actions", {}).setdefault("reportes", [])
for _perm in ("cobros_view", "cobros_cancel", "cobros_export"):
    if _perm not in LIDER_INVENTARIO_PERMS["actions"]["reportes"]:
        LIDER_INVENTARIO_PERMS["actions"]["reportes"].append(_perm)

LIDER_INVENTARIO_PERMS.setdefault("actions", {}).setdefault("materiales", [])
for _perm in ("create", "edit"):
    if _perm not in LIDER_INVENTARIO_PERMS["actions"]["materiales"]:
        LIDER_INVENTARIO_PERMS["actions"]["materiales"].append(_perm)

# Tesorería: reportes, pero sin acceso global especial al reporte de asignaciones-persona
TREASURY_PERMS = {
    "modules": ["dashboard", "reportes"],
    "actions": {
        "reportes": ["view_own", "cobros_view", "cobros_cancel", "cobros_export"]
    },
    "office_filter": "all",
}

# Oficinas: solo ven lo suyo
OFFICE_BASE_PERMS = {
    "modules": [
        "dashboard",
        "material_pop",
        "inventario_corporativo",
        "prestamo_material",
        "reportes",
        "solicitudes",
        "novedades",
        "oficinas",
        "aprobadores",
    ],
    "actions": {
        "materiales": [],
        "solicitudes": ["view", "create", "return"],
        "novedades": ["create", "view", "return"],
        "reportes": ["view_own"],
        "oficinas": ["view"],
        "aprobadores": ["view"],
        "prestamos": ["view_own", "create"],
        "inventario_corporativo": [
            "view",
            "return",
            "transfer",
            "request_return",
            "request_transfer",
            "view_reports",
        ],
    },
    "office_filter": "OFFICE_ONLY",
}


# ---------------------------------------------------------------------------
# ROLE_PERMISSIONS final
# ---------------------------------------------------------------------------

ROLE_PERMISSIONS = {
    "administrador": deepcopy(ADMIN_PERMS),
    "aprobador": deepcopy(APPROVER_LIKE_PERMS),
    "lider_inventario": deepcopy(LIDER_INVENTARIO_PERMS),
    "tesoreria": deepcopy(TREASURY_PERMS),
}

# Oficinas conocidas
OFFICE_FILTERS = {
    "oficina_pepe_sierra": "PEPE SIERRA",
    "oficina_polo_club": "POLO CLUB",
    "oficina_nogal": "NOGAL",
    "oficina_tunja": "TUNJA",
    "oficina_cartagena": "CARTAGENA",
    "oficina_morato": "MORATO",
    "oficina_medellin": "MEDELLÍN",
    "oficina_cedritos": "CEDRITOS",
    "oficina_coq": "COQ",
    "oficina_cali": "CALI",
    "oficina_lourdes": "LOURDES",
    "oficina_pereira": "PEREIRA",
    "oficina_bucaramanga": "BUCARAMANGA",
    "oficina_neiva": "NEIVA",
    "oficina_kennedy": "KENNEDY",
    "oficina_barranquilla": "BARRANQUILLA",
    "oficina_usaquen": "USAQUEN",
}

# Roles corporativos con comportamiento office-like
OFFICE_LIKE_ROLES = {
    "gerencia_talento_humano": "COQ",
    "gerencia_comercial": "COQ",
    "comunicaciones": "COQ",
    "presidencia": "COQ",
}

for role_key, office_name in {**OFFICE_FILTERS, **OFFICE_LIKE_ROLES}.items():
    cfg = deepcopy(OFFICE_BASE_PERMS)
    cfg["office_filter"] = office_name
    ROLE_PERMISSIONS[role_key] = cfg