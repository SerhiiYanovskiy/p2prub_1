import os
from dotenv import load_dotenv

load_dotenv()


from pathlib import Path
REPORT_DELIVERY = "telegram"

BASE_DIR = Path(__file__).resolve().parent
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
XLSX_PATH = os.getenv(
    "XLSX_PATH",
    "1.xlsx"
).strip()
MAX_MESSAGE_LEN = 3500
RULES_PDF_PATH = str(BASE_DIR / "умови використання програми.pdf")


AUDIO_DIR = "audio"
AUDIO_EXT = "ogg"

TTS_VOICE = "uk-UA-PolinaNeural"
TTS_RATE = "+0%"
TTS_VOLUME = "+0%"
TTS_OUTPUT_FORMAT = "ogg-24khz-16bit-mono-opus"

PREGENERATE_AUDIO_ON_START = False




DB_PATH = str(BASE_DIR / "bot.sqlite3")


TTS_TIMEOUT_SEC = 25

REPORT_TZ = "Europe/Kyiv"
REPORT_WEEKDAY = 0
REPORT_HOUR = 8
REPORT_MINUTE = 0

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_USER = "familylegalfamilylegal@gmail.com"
SMTP_PASSWORD = os.getenv("APP_PASSWORD", "")
SMTP_USE_SSL = True
SMTP_FROM = "familylegalfamilylegal@gmail.com"
SMTP_TIMEOUT_SEC = 30

EMAIL_FROM = "familylegalfamilylegal@gmail.com"

SENDGRID_API_KEY = ""




REPORT_TO = [
    "serhiiyanovskyi05021992@gmail.com",
    "lishchukirisha94@gmail.com",
    "krasovskaya07@gmail.com"

]


REPORT_SUBJECT = "Weekly bot report - user actions"


REPORT_ENABLE = True
REPORT_TZ = "Europe/Kyiv"
REPORT_MAX_ROWS_PER_FILE = 1000000
REPORT_MAX_ROWS_PER_SHEET = 1000000
REPORT_OUT_DIR = str(BASE_DIR / "reports")
REPORT_SUBJECT = "Weekly bot report"


ADMIN_USER_IDS = [544723767]











