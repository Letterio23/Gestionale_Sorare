import os
import requests
import json
import time
import gspread

# --- 1. CONFIGURAZIONE DAI SEGRETI ---
SORARE_API_KEY = os.environ.get("SORARE_API_KEY")
USER_SLUG = os.environ.get("USER_SLUG")
GSPREAD_CREDENTIALS_JSON = os.environ.get("GSPREAD_CREDENTIALS")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- Costanti di Configurazione ---
API_URL = "https://api.sorare.com/graphql"
MAIN_SHEET_NAME = "Foglio1"
BATCH_SIZE = 15

# --- Funzioni Helper (le stesse che usiamo già) ---
def sorare_graphql_fetch(query, variables={}):
    # ... (implementazione della funzione, la aggiungiamo dopo)
    pass

def send_telegram_notification(text):
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        print("Token o Chat ID di Telegram non configurati.")
        return
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(api_url, json=payload)
        print("Notifica Telegram inviata.")
    except Exception as e:
        print(f"Errore invio notifica Telegram: {e}")

# ... (qui andranno le altre funzioni tradotte)

# --- TRADUZIONE DI updateFloorPrices_V3 ---
def update_floor_prices():
    print("--- INIZIO AGGIORNAMENTO FLOOR PRICES ---")
    start_time = time.time()
    
    # 1. Connessione a Google Sheets
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
        print("Connessione a Google Sheets riuscita.")
    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile connettersi a Google Sheets: {e}")
        return

    # 2. Ottieni la lista degli slug unici dal foglio
    try:
        header_row = sheet.row_values(1)
        slug_col_index = header_row.index("Player API Slug") + 1
        all_slugs = sheet.col_values(slug_col_index)[1:] # [1:] per saltare l'header
        slugs_to_process = sorted(list(set(filter(None, all_slugs)))) # Filtra, Rendi unici, Ordina
        print(f"Trovati {len(slugs_to_process)} slug unici da processare.")
    except (ValueError, gspread.exceptions.GSpreadException) as e:
        print(f"ERRORE: Impossibile leggere gli slug dal foglio. Assicurati che la colonna 'Player API Slug' esista. Dettagli: {e}")
        return

    # 3. Ottieni i tassi di cambio (semplificato per ora)
    # In una versione completa, qui andrebbe la logica a cascata
    eth_rate_response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur")
    rates = {"eth_to_eur": eth_rate_response.json()["ethereum"]["eur"]}
    print(f"Tasso ETH/EUR ottenuto: {rates['eth_to_eur']}")

    # 4. Processa gli slug in lotti
    floor_prices_map = {}
    for i in range(0, len(slugs_to_process), BATCH_SIZE):
        batch_slugs = slugs_to_process[i:i + BATCH_SIZE]
        print(f"Processo lotto {i//BATCH_SIZE + 1}/{(len(slugs_to_process) + BATCH_SIZE - 1)//BATCH_SIZE}...")
        
        # Qui andrebbe la logica per costruire e fare la chiamata GraphQL per il lotto
        # Per ora, simuliamo dei dati
        for slug in batch_slugs:
            floor_prices_map[slug] = {
                "FLOOR CLASSIC LIMITED": 1.0, # Esempio
                "FLOOR IN SEASON LIMITED": 1.5, # Esempio
                # ... altri campi
            }
        time.sleep(1) # Pausa di cortesia
    
    # 5. Aggiorna il foglio Google con i nuovi dati
    try:
        print("Aggiornamento del foglio Google in corso...")
        all_sheet_data = sheet.get_all_records()
        updates = []
        
        # Prepara la lista delle colonne da aggiornare
        target_columns = ["FLOOR CLASSIC LIMITED", "FLOOR IN SEASON LIMITED"] # Esempio
        
        for index, row in enumerate(all_sheet_data):
            slug_in_row = row.get("Player API Slug")
            if slug_in_row in floor_prices_map:
                prices = floor_prices_map[slug_in_row]
                for col_name in target_columns:
                    if col_name in prices:
                        updates.append({
                            'range': gspread.utils.rowcol_to_a1(index + 2, header_row.index(col_name) + 1),
                            'values': [[prices[col_name]]],
                        })
        
        if updates:
            sheet.batch_update(updates)
        print("Aggiornamento del foglio completato.")

    except Exception as e:
        print(f"ERRORE durante l'aggiornamento del foglio: {e}")

    # 6. Notifica finale
    execution_time = time.time() - start_time
    message = (
        f"✅ <b>Floor Prices Aggiornati (da GitHub)</b>\n\n"
        f"⏱️ Tempo: {execution_time:.2f}s"
    )
    send_telegram_notification(message)


if __name__ == "__main__":
    # Questa parte serve per decidere quale funzione eseguire
    # In futuro, potremmo passare un argomento dal workflow
    update_floor_prices()
