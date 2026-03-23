import base64
import io
import json
import logging
import mimetypes
import os
from dataclasses import dataclass
from typing import Optional

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai
from google.genai import types


# ============================================================
# CONFIGURACION
# ============================================================
# Rellena estos valores en Codespaces.
# Tambien puedes moverlos a variables de entorno.

SERVICE_ACCOUNT_FILE = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    "service_account.json",
)

DRIVE_FOLDER_ID = os.getenv(
    "GOOGLE_DRIVE_FOLDER_ID",
    "1u8qfvAnrBLku8VN-399b-0flaAF7XUZc",
)

SPREADSHEET_ID = os.getenv(
    "GOOGLE_SHEETS_SPREADSHEET_ID",
    "19BKG6k0h_axJ9v8xh08faGTgfhxqnryRQtX3s-HdH6s",
)

SHEET_NAME = os.getenv(
    "GOOGLE_SHEETS_TAB_NAME",
    "data",
)

GEMINI_API_KEY = os.getenv(
    "GEMINI_API_KEY",
    "AIzaSyBkpH2QgQ4xzF7XM9Hp1S-bh_139eZuO_A",
)

GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3-flash-preview",
)

# Spotify es opcional.
SPOTIFY_CLIENT_ID = os.getenv(
    "SPOTIFY_CLIENT_ID",
    "",
)
SPOTIFY_CLIENT_SECRET = os.getenv(
    "SPOTIFY_CLIENT_SECRET",
    "",
)
SPOTIFY_MARKET = os.getenv(
    "SPOTIFY_MARKET",
    "MX",
)

# Si es True, evita repetir archivos ya procesados.
SKIP_ALREADY_REGISTERED = True

# Puedes filtrar por extensiones si quieres.
ALLOWED_MIME_PREFIXES = ("image/",)

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# MODELOS DE DATOS
# ============================================================
@dataclass
class ImageAnalysis:
    file_id: str
    file_name: str
    description: str
    main_sentiment: str
    other_sentiments: str
    spotify_track_name: str = ""
    spotify_artist: str = ""
    spotify_url: str = ""
    spotify_reason: str = ""


# ============================================================
# GOOGLE AUTH
# ============================================================
class GoogleClients:
    def __init__(self, service_account_file: str):
        scopes = [
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes,
        )
        self.drive = build("drive", "v3", credentials=creds)
        self.sheets = build("sheets", "v4", credentials=creds)


# ============================================================
# DRIVE
# ============================================================
def list_images_in_folder(drive_service, folder_id: str):
    files = []
    page_token = None

    query = (
        f"'{folder_id}' in parents and trashed = false"
    )

    while True:
        response = (
            drive_service.files()
            .list(
                q=query,
                fields=(
                    "nextPageToken, "
                    "files(id, name, mimeType, createdTime)"
                ),
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
                pageSize=100,
            )
            .execute()
        )

        for item in response.get("files", []):
            mime_type = item.get("mimeType", "")
            if mime_type.startswith(ALLOWED_MIME_PREFIXES):
                files.append(item)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    files.sort(key=lambda x: x.get("name", "").lower())
    return files


def download_drive_file_bytes(drive_service, file_id: str):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    return fh.getvalue()


# ============================================================
# SHEETS
# ============================================================
def ensure_sheet_header(sheets_service):
    header = [[
        "file_id",
        "file_name",
        "description",
        "main_sentiment",
        "other_sentiments",
        "spotify_track_name",
        "spotify_artist",
        "spotify_url",
        "spotify_reason",
    ]]

    range_name = f"{SHEET_NAME}!A1:I1"
    (
        sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            body={"values": header},
        )
        .execute()
    )


def get_already_processed_file_ids(sheets_service):
    if not SKIP_ALREADY_REGISTERED:
        return set()

    range_name = f"{SHEET_NAME}!A2:A"
    response = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
        )
        .execute()
    )
    rows = response.get("values", [])
    return {row[0] for row in rows if row}


def append_result_row(sheets_service, result: ImageAnalysis):
    values = [[
        result.file_id,
        result.file_name,
        result.description,
        result.main_sentiment,
        result.other_sentiments,
        result.spotify_track_name,
        result.spotify_artist,
        result.spotify_url,
        result.spotify_reason,
    ]]

    (
        sheets_service.spreadsheets()
        .values()
        .append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:I",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        )
        .execute()
    )


# ============================================================
# GEMINI
# ============================================================
def guess_mime_type(file_name: str) -> str:
    mime_type, _ = mimetypes.guess_type(file_name)
    return mime_type or "image/jpeg"


def build_gemini_client(api_key: str):
    return genai.Client(api_key=api_key)


def analyze_image_with_gemini(
    client,
    image_bytes: bytes,
    file_name: str,
) -> ImageAnalysis:
    mime_type = guess_mime_type(file_name)

    prompt = """
Analiza esta imagen y responde SOLO JSON valido.
No uses markdown ni bloques de codigo.

Quiero este esquema exacto:
{
  "description": "descripcion clara en espanol",
  "main_sentiment": "sentimiento principal en una o dos palabras",
  "other_sentiments": ["sentimiento 1", "sentimiento 2"],
  "music_query": "busqueda corta para Spotify"
}

Reglas:
- La descripcion debe ser breve pero concreta.
- El sentimiento debe inferirse por la escena.
- other_sentiments debe tener 1 a 3 elementos.
- music_query debe servir para buscar una cancion.
- Si la emocion no es clara, usa "neutral".
""".strip()

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[
            types.Part.from_bytes(
                data=image_bytes,
                mime_type=mime_type,
            ),
            prompt,
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
        ),
    )

    raw_text = response.text.strip()
    data = json.loads(raw_text)

    other = data.get("other_sentiments", [])
    if isinstance(other, list):
        other_text = ", ".join(str(x) for x in other)
    else:
        other_text = str(other)

    return ImageAnalysis(
        file_id="",
        file_name=file_name,
        description=str(data.get("description", "")).strip(),
        main_sentiment=str(
            data.get("main_sentiment", "neutral")
        ).strip(),
        other_sentiments=other_text,
        spotify_reason=str(data.get("music_query", "")).strip(),
    )


# ============================================================
# SPOTIFY
# ============================================================
def get_spotify_access_token(
    client_id: str,
    client_secret: str,
) -> Optional[str]:
    if not client_id or not client_secret:
        return None

    token_url = "https://accounts.spotify.com/api/token"
    basic = base64.b64encode(
        f"{client_id}:{client_secret}".encode("utf-8")
    ).decode("utf-8")

    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "client_credentials"}

    response = requests.post(
        token_url,
        headers=headers,
        data=data,
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    return payload["access_token"]


def suggest_spotify_track(
    access_token: Optional[str],
    query: str,
) -> dict:
    if not access_token or not query:
        return {
            "name": "",
            "artist": "",
            "url": "",
        }

    url = "https://api.spotify.com/v1/search"
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    params = {
        "q": query,
        "type": "track",
        "limit": 1,
        "market": SPOTIFY_MARKET,
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    items = data.get("tracks", {}).get("items", [])
    if not items:
        return {
            "name": "",
            "artist": "",
            "url": "",
        }

    track = items[0]
    artists = track.get("artists", [])
    artist_names = ", ".join(
        artist.get("name", "") for artist in artists
    )

    return {
        "name": track.get("name", ""),
        "artist": artist_names,
        "url": track.get("external_urls", {}).get(
            "spotify", ""
        ),
    }


# ============================================================
# PIPELINE
# ============================================================
def process_drive_folder():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            "No encuentro el archivo service_account.json"
        )

    if not GEMINI_API_KEY or "PON_AQUI" in GEMINI_API_KEY:
        raise ValueError("Falta GEMINI_API_KEY")

    clients = GoogleClients(SERVICE_ACCOUNT_FILE)
    gemini_client = build_gemini_client(GEMINI_API_KEY)

    spotify_token = None
    if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
        logger.info("Obteniendo token de Spotify...")
        spotify_token = get_spotify_access_token(
            SPOTIFY_CLIENT_ID,
            SPOTIFY_CLIENT_SECRET,
        )
    else:
        logger.info("Spotify omitido. Sin credenciales.")

    logger.info("Preparando encabezados en Sheets...")
    ensure_sheet_header(clients.sheets)

    processed_ids = get_already_processed_file_ids(clients.sheets)
    logger.info("Archivos ya registrados: %s", len(processed_ids))

    logger.info("Buscando imagenes en Drive...")
    files = list_images_in_folder(clients.drive, DRIVE_FOLDER_ID)
    logger.info("Imagenes encontradas: %s", len(files))

    for idx, file_item in enumerate(files, start=1):
        file_id = file_item["id"]
        file_name = file_item["name"]

        if file_id in processed_ids:
            logger.info(
                "[%s/%s] Saltando ya procesado: %s",
                idx,
                len(files),
                file_name,
            )
            continue

        logger.info(
            "[%s/%s] Descargando: %s",
            idx,
            len(files),
            file_name,
        )
        image_bytes = download_drive_file_bytes(
            clients.drive,
            file_id,
        )

        logger.info("Analizando con Gemini...")
        result = analyze_image_with_gemini(
            gemini_client,
            image_bytes,
            file_name,
        )
        result.file_id = file_id

        logger.info("Buscando cancion en Spotify...")
        song = suggest_spotify_track(
            spotify_token,
            result.spotify_reason,
        )
        result.spotify_track_name = song["name"]
        result.spotify_artist = song["artist"]
        result.spotify_url = song["url"]

        logger.info("Guardando fila en Sheets...")
        append_result_row(clients.sheets, result)

    logger.info("Proceso terminado.")


if __name__ == "__main__":
    process_drive_folder()
