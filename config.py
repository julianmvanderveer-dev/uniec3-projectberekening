class Config:
    SECRET_KEY = "uniec3-projectberekening"
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # max 50 MB upload

    # Weergave
    TOOL_NAAM = "Uniec3 Projectberekening Samensteller"
    TOOL_VERSIE = "1.0"
    BEDRIJF = ""       # optioneel, bijv. "Mijn Bedrijf BV"
    CONTACT = ""       # optioneel, bijv. "info@mijnbedrijf.nl"
