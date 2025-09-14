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
STATE_FILE = "state.json"

BATCH_SIZE = 15
CARD_DATA_UPDATE_INTERVAL_HOURS = 0.5
MAX_SALES_TO_DISPLAY = 100
MAX_SALES_FROM_API = 7
INITIAL_SALES_FETCH_COUNT = 20

# --- 2. QUERY GRAPHQL ---
ALL_CARDS_QUERY = """
    query AllCardsFromUser($userSlug: String!, $rarities: [Rarity!], $cursor: String) {
        user(slug: $userSlug) {
            cards(rarities: $rarities, after: $cursor) {
                nodes {
                    slug
                    rarity
                    player {
                        displayName
                        slug
                    }
                }
                pageInfo { endCursor, hasNextPage }
            }
        }
    }
"""

# --- 3. FUNZIONI HELPER DI BASE ---

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
        if "errors" in data: 
            print(f"ERRORE GraphQL: {data['errors']}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Errore di rete durante la chiamata API: {e}")
        return None

def send_telegram_notification(text):
    """Invia una notifica a Telegram."""
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]): 
        print("Token o Chat ID di Telegram non configurati.")
        return
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(api_url, json=payload, timeout=10)
        print("Notifica Telegram inviata.")
    except Exception as e:
        print(f"Errore invio notifica Telegram: {e}")

def get_eth_rate():
    """Ottiene il tasso di cambio ETH/EUR."""
    try:
        response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur", timeout=5)
        response.raise_for_status()
        return response.json()["ethereum"]["eur"]
    except Exception:
        print("Attenzione: impossibile ottenere il tasso da CoinGecko, uso un valore di fallback.")
        return 3000.0

def calculate_eur_price(price_object, rates):
    """Calcola il prezzo in EUR da un oggetto 'amounts' dell'API."""
    if not price_object or not rates: return ""
    try:
        # L'API a volte restituisce una lista, a volte no.
        amounts = price_object.get('liveSingleSaleOffer', {}).get('receiverSide', {}).get('amounts', [])
        if not amounts: return ""
        
        # Gestisce sia il caso in cui amounts sia una lista che un singolo oggetto
        amounts_data = amounts[0] if isinstance(amounts, list) else amounts
        
        currency = amounts_data.get('referenceCurrency', '').lower()
        
        if currency == 'eur':
            return amounts_data.get('eurCents', 0) / 100
        elif currency in ['eth', 'wei']:
            return (float(amounts_data.get('wei', 0)) / 1e18) * rates.get('eth_to_eur', 3000)
    except (TypeError, KeyError, IndexError, AttributeError):
        return ""
    return ""

# --- 4. FUNZIONI PRINCIPALI DEL GESTIONALE ---

def initial_setup():
    """Popola il foglio principale con tutte le carte dell'utente."""
    print("--- INIZIO PRIMO AGGIORNAMENTO COMPLETO ---")
    
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        try:
            sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
            sheet.clear()
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=MAIN_SHEET_NAME, rows="1000", cols="50")
        
        print(f"Foglio '{MAIN_SHEET_NAME}' preparato.")
    except Exception as e:
        print(f"ERRORE CRITICO durante l'accesso a Google Sheets: {e}")
        return

    print("Recupero di tutte le carte dall'API di Sorare...")
    all_cards = []
    cursor = None
    has_next_page = True
    while has_next_page:
        variables = {"userSlug": USER_SLUG, "rarities": ["limited", "rare", "super_rare", "unique"], "cursor": cursor}
        data = sorare_graphql_fetch(ALL_CARDS_QUERY, variables)
        
        if not data or not data.get("data") or not data["data"].get("user") or not data["data"]["user"].get("cards"):
            print("Risposta API non valida o vuota durante il recupero delle carte. Interruzione.")
            break
        
        cards_data = data["data"]["user"]["cards"]
        all_cards.extend(cards_data.get("nodes", []))
        page_info = cards_data.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
        print(f"Recuperate {len(all_cards)} carte finora...")
        if has_next_page: time.sleep(1)

    print(f"Recupero completato. Trovate {len(all_cards)} carte in totale.")

    headers = ["Slug", "Rarity", "Player Name", "Player API Slug", "FLOOR CLASSIC LIMITED", "FLOOR IN SEASON LIMITED"] # Aggiungi altre colonne necessarie
    data_to_write = [headers]
    for card in all_cards:
        player = card.get("player") or {}
        data_to_write.append([
            card.get("slug", ""),
            card.get("rarity", ""),
            player.get("displayName", ""),
            player.get("slug", "")
        ])

    if len(data_to_write) > 1:
        sheet.update('A1', data_to_write, value_input_option='USER_ENTERED')
        print(f"Il foglio '{MAIN_SHEET_NAME}' è stato popolato con {len(all_cards)} carte.")
    else:
        print("Nessuna carta trovata da scrivere.")

    send_telegram_notification(f"✅ <b>Primo Avvio Completato (GitHub)</b>\n\nIl foglio contiene {len(all_cards)} carte.")

def update_cards():
    """Placeholder per la traduzione di updateAllCardData_V3."""
    print("--- INIZIO AGGIORNAMENTO DATI CARTE ---")
    print("Funzione 'update_cards' non ancora implementata.")
    send_telegram_notification("✅ <b>Dati Carte Aggiornati (da GitHub) - Placeholder</b>")

def update_sales():
    """Placeholder per la traduzione di updatePlayerSalesHistory_V3."""
    print("--- INIZIO AGGIORNAMENTO CRONOLOGIA VENDITE ---")
    print("Funzione 'update_sales' non ancora implementata.")
    send_telegram_notification("✅ <b>Cronologia Vendite Aggiornata (da GitHub) - Placeholder</b>")

def update_floors():
    """Traduzione completa di updateFloorPrices_V3."""
    print("--- INIZIO AGGIORNAMENTO FLOOR PRICES ---")
    start_time = time.time()
    
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
        print("Connessione a Google Sheets riuscita.")
    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile connettersi a Google Sheets: {e}")
        return

    try:
        header_row = sheet.row_values(1)
        slug_col_idx = header_row.index("Player API Slug") + 1
        all_slugs = sheet.col_values(slug_col_idx)[1:]
        slugs_to_process = sorted(list(set(filter(None, all_slugs))))
        if not slugs_to_process:
            print("Nessun slug trovato nel foglio. Eseguire prima 'initial_setup'.")
            return
        print(f"Trovati {len(slugs_to_process)} slug unici da processare.")
    except (ValueError, gspread.exceptions.GSpreadException) as e:
        print(f"ERRORE: Impossibile leggere gli slug. Colonna 'Player API Slug' esiste? Dettagli: {e}")
        return

    rates = {"eth_to_eur": get_eth_rate()}
    print(f"Tasso ETH/EUR ottenuto: {rates['eth_to_eur']}")
    
    floor_prices_map = {}
    price_fields_fragment = "liveSingleSaleOffer { receiverSide { amounts { eurCents, wei, referenceCurrency } } }"
    
    for i in range(0, len(slugs_to_process), BATCH_SIZE):
        batch_slugs = slugs_to_process[i:i + BATCH_SIZE]
        print(f"Processo lotto {i//BATCH_SIZE + 1}/{ -(-len(slugs_to_process) // BATCH_SIZE) }...") # Calcolo corretto per il totale lotti
        
        query_parts = [
            f'p_{idx}: player(slug: "{slug}") {{ '
            f'L_IN: lowestPriceAnyCard(rarity: limited, inSeason: true) {{ {price_fields_fragment} }} '
            f'L_ANY: lowestPriceAnyCard(rarity: limited, inSeason: false) {{ {price_fields_fragment} }} '
            # Aggiungi qui RARE e SUPER_RARE se necessario
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

    try:
        print("Aggiornamento del foglio Google in corso...")
        all_sheet_values = sheet.get_all_values()
        headers = all_sheet_values[0]
        
        slug_col_idx_header = headers.index("Player API Slug")
        target_columns_indices = {
            "FLOOR IN SEASON LIMITED": headers.index("FLOOR IN SEASON LIMITED"),
            "FLOOR CLASSIC LIMITED": headers.index("FLOOR CLASSIC LIMITED"),
        }
        
        # Copia i dati per modificarli localmente
        updated_sheet_values = [list(row) for row in all_sheet_values]

        for row_idx, row in enumerate(updated_sheet_values[1:], start=1):
            slug_in_row = row[slug_col_idx_header]
            if slug_in_row in floor_prices_map:
                prices = floor_prices_map[slug_in_row]
                for col_name, col_idx in target_columns_indices.items():
                    price_value = prices.get(col_name)
                    if price_value is not None:
                        updated_sheet_values[row_idx][col_idx] = price_value
        
        # Scrive tutti i dati in un'unica operazione
        sheet.update('A1', updated_sheet_values, value_input_option='USER_ENTERED')
        print("Aggiornamento del foglio completato.")
    except Exception as e:
        print(f"ERRORE durante l'aggiornamento del foglio: {e}")

    execution_time = time.time() - start_time
    message = f"✅ <b>Floor Prices Aggiornati (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s"
    send_telegram_notification(message)

# --- 5. GESTORE DEGLI ARGOMENTI ---

if __name__ == "__main__":
    if len(sys.argv) > 1:
        function_to_run = sys.argv[1]
        print(f"Eseguo la funzione: {function_to_run}")
        
        if function_to_run == "initial_setup":
            initial_setup()
        elif function_to_run == "update_cards":
            update_cards()
        elif function_to_run == "update_sales":
            update_sales()
        elif function_to_run == "update_floors":
            update_floors()
        else:
            print(f"Errore: Funzione '{function_to_run}' non riconosciuta.")
    else:
        print("Nessuna funzione specificata. Terminazione.")
