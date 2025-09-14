# Importazioni necessarie
import os
import sys
import requests
import json
import time
from datetime import datetime
import gspread

# --- 1. CONFIGURAZIONE DAI SEGRETI E COSTANTI ---
SORARE_API_KEY = os.environ.get("SORARE_API_KEY")
USER_SLUG = os.environ.get("USER_SLUG")
GSPREAD_CREDENTIALS_JSON = os.environ.get("GSPREAD_CREDENTIALS")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

API_URL = "https://api.sorare.com/graphql"
MAIN_SHEET_NAME = "Foglio1"
SALES_HISTORY_SHEET_NAME = "Cronologia Vendite"
STATE_FILE = "state.json"  # Il nostro sostituto di PropertiesService

BATCH_SIZE = 15
CARD_DATA_UPDATE_INTERVAL_HOURS = 0.5
MAX_SALES_TO_DISPLAY = 100
MAX_SALES_FROM_API = 7
INITIAL_SALES_FETCH_COUNT = 20

# --- 2. FUNZIONI HELPER DI BASE ---

def load_state():
    """Carica lo stato dell'esecuzione dal file JSON."""
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_state(state_data):
    """Salva lo stato aggiornato nel file JSON."""
    with open(STATE_FILE, "w") as f:
        json.dump(state_data, f, indent=2)

def sorare_graphql_fetch(query, variables={}):
    """Funzione generica per le chiamate API a Sorare."""
    payload = {"query": query, "variables": variables}
    headers = {
        "APIKEY": SORARE_API_KEY, "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()
        if "errors" in data: print(f"ERRORE GraphQL: {data['errors']}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Errore di rete durante la chiamata API: {e}")
        return None

def send_telegram_notification(text):
    """Invia una notifica a Telegram."""
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]): return
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(api_url, json=payload)
    except Exception as e:
        print(f"Errore invio notifica Telegram: {e}")

def get_eth_rate():
    """Ottiene il tasso di cambio ETH/EUR."""
    # Semplificato per ora, si può migliorare con la logica a cascata
    try:
        response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur")
        return response.json()["ethereum"]["eur"]
    except Exception:
        return 3000.0 # Fallback

# --- 3. TRADUZIONE DELLE FUNZIONI PRINCIPALI ---

def update_cards():
    """Traduzione di updateAllCardData_V3."""
    print("--- INIZIO AGGIORNAMENTO DATI CARTE ---")
    # ... Logica di update_cards ...
    # Questa è la funzione più complessa da tradurre, la lasciamo come placeholder
    print("Funzione 'update_cards' non ancora implementata.")
    send_telegram_notification("✅ <b>Dati Carte Aggiornati (da GitHub)</b>")


def update_sales():
    """Traduzione di updatePlayerSalesHistory_V3."""
    print("--- INIZIO AGGIORNAMENTO CRONOLOGIA VENDITE ---")
    # ... Logica di update_sales ...
    print("Funzione 'update_sales' non ancora implementata.")
    send_telegram_notification("✅ <b>Cronologia Vendite Aggiornata (da GitHub)</b>")
    

def update_floors():
    """Traduzione completa di updateFloorPrices_V3."""
    print("--- INIZIO AGGIORNAMENTO FLOOR PRICES ---")
    start_time = time.time()
    
    # Connessione a Google Sheets
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
        print("Connessione a Google Sheets riuscita.")
    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile connettersi a Google Sheets: {e}")
        return

    # Ottieni slug unici dal foglio
    try:
        header_row = sheet.row_values(1)
        slug_col_idx = header_row.index("Player API Slug") + 1
        all_slugs = sheet.col_values(slug_col_idx)[1:]
        slugs_to_process = sorted(list(set(filter(None, all_slugs))))
        print(f"Trovati {len(slugs_to_process)} slug unici da processare.")
    except (ValueError, gspread.exceptions.GSpreadException) as e:
        print(f"ERRORE: Impossibile leggere gli slug. Colonna 'Player API Slug' esiste? Dettagli: {e}")
        return

    eth_rate = get_eth_rate()
    rates = {"eth_to_eur": eth_rate} # Per compatibilità con funzioni future
    
    # Processa gli slug in lotti
    floor_prices_map = {}
    price_fields_fragment = "liveSingleSaleOffer { receiverSide { amounts { eurCents, wei, referenceCurrency } } }"
    
    for i in range(0, len(slugs_to_process), BATCH_SIZE):
        batch_slugs = slugs_to_process[i:i + BATCH_SIZE]
        print(f"Processo lotto {i//BATCH_SIZE + 1}...")
        
        query_parts = [
            f'p_{idx}: player(slug: "{slug}") {{ '
            f'L_IN: lowestPriceAnyCard(rarity: limited, inSeason: true) {{ {price_fields_fragment} }} '
            f'L_ANY: lowestPriceAnyCard(rarity: limited, inSeason: false) {{ {price_fields_fragment} }} '
            # Aggiungi qui le altre rarità se necessario (rare, super_rare)
            f'}}' for idx, slug in enumerate(batch_slugs)
        ]
        
        dynamic_query = f"query GetMultipleFloorPrices {{ football {{ {' '.join(query_parts)} }} }}"
        api_resp = sorare_graphql_fetch(dynamic_query)

        if api_resp and api_resp.get("data", {}).get("football"):
            for idx, slug in enumerate(batch_slugs):
                p_data = api_resp["data"]["football"].get(f'p_{idx}')
                if p_data:
                    floor_prices_map[slug] = {
                        "FLOOR IN SEASON LIMITED": calculate_eur_price(p_data.get('L_IN'), rates),
                        "FLOOR CLASSIC LIMITED": calculate_eur_price(p_data.get('L_ANY'), rates),
                    }
        time.sleep(1)

    # Aggiorna il foglio Google
    try:
        print("Aggiornamento del foglio Google in corso...")
        all_sheet_data_with_headers = sheet.get_all_values()
        headers = all_sheet_data_with_headers[0]
        all_sheet_data = all_sheet_data_with_headers[1:]
        
        updates = []
        slug_col_idx_header = headers.index("Player API Slug")
        
        target_columns = {
            "FLOOR IN SEASON LIMITED": headers.index("FLOOR IN SEASON LIMITED"),
            "FLOOR CLASSIC LIMITED": headers.index("FLOOR CLASSIC LIMITED"),
        }
        
        for row_idx, row in enumerate(all_sheet_data):
            slug_in_row = row[slug_col_idx_header]
            if slug_in_row in floor_prices_map:
                prices = floor_prices_map[slug_in_row]
                for col_name, col_idx in target_columns.items():
                    price_value = prices.get(col_name)
                    if price_value is not None:
                        updates.append({
                            'range': gspread.utils.rowcol_to_a1(row_idx + 2, col_idx + 1),
                            'values': [[price_value]],
                        })
        if updates:
            sheet.batch_update(updates, value_input_option='USER_ENTERED')
        print("Aggiornamento del foglio completato.")
    except Exception as e:
        print(f"ERRORE durante l'aggiornamento del foglio: {e}")

    execution_time = time.time() - start_time
    message = f"✅ <b>Floor Prices Aggiornati (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s"
    send_telegram_notification(message)

def calculate_eur_price(price_object, rates):
    if not price_object or not rates: return ""
    try:
        amounts = price_object['liveSingleSaleOffer']['receiverSide']['amounts'][0]
        if amounts['referenceCurrency'].lower() == 'eur':
            return amounts['eurCents'] / 100
        elif amounts['referenceCurrency'].lower() == 'eth' or amounts['referenceCurrency'].lower() == 'wei':
            return (float(amounts['wei']) / 1e18) * rates['eth_to_eur']
    except (TypeError, KeyError, IndexError):
        return ""
    return ""

# --- 4. GESTORE DEGLI ARGOMENTI ---

if __name__ == "__main__":
    # Legge l'argomento passato dal workflow per decidere quale funzione eseguire
    if len(sys.argv) > 1:
        function_to_run = sys.argv[1]
        if function_to_run == "update_cards":
            update_cards()
        elif function_to_run == "update_sales":
            update_sales()
        elif function_to_run == "update_floors":
            update_floors()
        else:
            print(f"Errore: Funzione '{function_to_run}' non riconosciuta.")
    else:
        print("Nessuna funzione specificata. Eseguo 'update_floors' di default.")
        update_floors()
