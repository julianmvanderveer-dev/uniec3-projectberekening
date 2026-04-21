import os

class Config:
    SECRET_KEY           = os.environ.get("SECRET_KEY", "uniec3-projectberekening-dev")
    MAX_CONTENT_LENGTH   = 50 * 1024 * 1024
    TOOL_NAAM            = "Uniec3 Projectberekening Samensteller"

    # Prijsinstelling (excl. BTW, in euro)
    PRIJS_EXCL_BTW: float = 100.00  # → incl. 21% BTW = € 121,00
    BTW_PCT:        float = 21.0

    # Admin-pagina wachtwoord (overschrijf via ADMIN_KEY env var)
    ADMIN_KEY: str = os.environ.get("ADMIN_KEY", "admin")
