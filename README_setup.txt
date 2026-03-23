1) Activa estas APIs en Google Cloud:
   - Google Drive API
   - Google Sheets API
   - Gemini API

2) Crea una service account y descarga el JSON.
   Guarda el archivo como service_account.json
   en la raiz del proyecto.

3) Comparte con el correo de la service account:
   - la carpeta de Google Drive
   - el Google Sheet

4) Crea una pestaña llamada:
   Resultados

5) Variables opcionales de entorno:
   GOOGLE_SERVICE_ACCOUNT_FILE=service_account.json
   GOOGLE_DRIVE_FOLDER_ID=...
   GOOGLE_SHEETS_SPREADSHEET_ID=...
   GOOGLE_SHEETS_TAB_NAME=Resultados
   GEMINI_API_KEY=...
   GEMINI_MODEL=gemini-3-flash-preview
   SPOTIFY_CLIENT_ID=...
   SPOTIFY_CLIENT_SECRET=...
   SPOTIFY_MARKET=MX

6) Instala dependencias:
   pip install -r requirements.txt

7) Ejecuta:
   python main.py
