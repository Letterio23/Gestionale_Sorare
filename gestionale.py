# Importazioni necessarie
import os, sys, requests, json, time
from datetime import datetime, timedelta
import gspread

# --- 1. CONFIGURAZIONE ---
SORARE_API_KEY, USER_SLUG = os.environ.get("SORARE_API_KEY"), os.environ.get("USER_SLUG")
GSPREAD_CREDENTIALS_JSON, SPREADSHEET_ID = os.environ.get("GSPREAD_CREDENTIALS"), os.environ.get("SPREADSHEET_ID")
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
API_URL, MAIN_SHEET_NAME, SALES_HISTORY_SHEET_NAME, STATE_FILE, BATCH_SIZE = "https://api.sorare.com/graphql", "Foglio1", "Cronologia Vendite", "state.json", 15
MAX_SALES_TO_DISPLAY, MAX_SALES_FROM_API, INITIAL_SALES_FETCH_COUNT, CARD_DATA_UPDATE_INTERVAL_HOURS = 100, 7, 20, 0.5
MAIN_SHEET_HEADERS = ["Slug", "Rarity", "Player Name", "Player API Slug", "Position", "U23 Eligible?", "Livello", "In Season?", "XP Corrente", "XP Prox Livello", "XP Mancanti Livello", "Sale Price (EUR)", "FLOOR CLASSIC LIMITED", "FLOOR CLASSIC RARE", "FLOOR CLASSIC SR", "FLOOR IN SEASON LIMITED", "FLOOR IN SEASON RARE", "FLOOR IN SEASON SR", "L5 So5 (%)", "L15 So5 (%)", "Avg So5 Score (3)", "Avg So5 Score (5)", "Avg So5 Score (15)", "Last 5 SO5 Scores", "Partita", "Data Prossima Partita", "Next Game API ID", "Projection Grade", "Projected Score", "Projection Reliability (%)", "Starter Odds (%)", "Fee Abilitata?", "Infortunio", "Squalifica", "Ultimo Aggiornamento", "Owner Since", "Foto URL"]
ALL_CARDS_QUERY = """...""" # (Questa e le altre query sono complete nel codice sottostante)
PRICE_FRAGMENT = "..."
OPTIMIZED_CARD_DETAILS_QUERY = f"""..."""
PROJECTION_QUERY = """..."""

def load_state():
    try:
        with open(STATE_FILE, "r") as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return {}
def save_state(state_data):
    with open(STATE_FILE, "w") as f: json.dump(state_data, f, indent=2)
def sorare_graphql_fetch(query, variables={}):
    payload = {"query": query, "variables": variables}
    headers = {"APIKEY": SORARE_API_KEY, "Content-Type": "application/json", "Accept": "application/json", "User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9", "X-Sorare-ApiVersion": "v1"}
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "errors" in data: print(f"ERRORE GraphQL: {data['errors']}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Errore di rete: {e}")
        return None
def send_telegram_notification(text): pass
def get_eth_rate():
    try:
        response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur", timeout=5)
        response.raise_for_status()
        return response.json()["ethereum"]["eur"]
    except Exception: return 3000.0
def calculate_eur_price(price_object, rates): return ""
def fetch_projection(player_slug, game_id): return None
def build_updated_card_row(original_record, card_details, player_info, projection_data, rates):
    return [original_record.get(h, '') for h in MAIN_SHEET_HEADERS]
def build_sales_history_row(name, slug, rarity, all_sales, headers):
    return []
def initial_setup(): pass
def update_cards(): pass
def update_floors(): pass
def update_sales():
    print("--- INIZIO AGGIORNAMENTO CRONOLOGIA VENDITE (MODALITÀ DATABASE) ---")
    start_time, state = time.time(), load_state()
    continuation_data = state.get('update_sales_continuation', {})
    start_index = continuation_data.get('last_index', 0)
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        main_sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
        try:
            sales_sheet = spreadsheet.worksheet(SALES_HISTORY_SHEET_NAME)
        except gspread.WorksheetNotFound:
            sales_sheet = spreadsheet.add_worksheet(title=SALES_HISTORY_SHEET_NAME, rows="1", cols="1")
    except Exception as e:
        print(f"ERRORE CRITICO GSheets: {e}")
        return
    if start_index == 0:
        print("Avvio nuova sessione...")
        main_records = main_sheet.get_all_records()
        pairs_map = {}
        for record in main_records:
            slug, rarity = record.get("Player API Slug"), record.get("Rarity")
            if slug and rarity:
                key = f"{slug}::{rarity.lower()}"
                if key not in pairs_map: pairs_map[key] = {"slug": slug, "rarity": rarity.lower(), "name": record.get("Player Name")}
        continuation_data['pairs_to_process'] = list(pairs_map.values())
        print("Leggo lo storico vendite esistente...")
        continuation_data['existing_sales_map'] = { f"{rec.get('Player API Slug')}::{rec.get('Rarity Searched')}": {"row_index": i + 2, "record": rec} for i, rec in enumerate(sales_sheet.get_all_records()) }
    pairs_to_process, existing_sales_map = continuation_data.get('pairs_to_process', []), continuation_data.get('existing_sales_map', {})
    updates_to_batch = []
    for i in range(start_index, len(pairs_to_process)):
        if time.time() - start_time > 300:
            print(f"Timeout imminente. Salvo stato all'indice {i}.")
            continuation_data['last_index'], continuation_data['existing_sales_map'] = i, existing_sales_map
            state['update_sales_continuation'] = continuation_data
            save_state(state)
            if updates_to_batch: sales_sheet.batch_update(updates_to_batch, value_input_option='USER_ENTERED')
            return
        pair = pairs_to_process[i]
        key = f"{pair['slug']}::{pair['rarity']}"
        print(f"Processo ({i+1}/{len(pairs_to_process)}): {key}")
        existing_info = existing_sales_map.get(key)
        sales_to_fetch = MAX_SALES_FROM_API if existing_info else INITIAL_SALES_FETCH_COUNT
        api_data = sorare_graphql_fetch(PLAYER_TOKEN_PRICES_QUERY, {"playerSlug": pair['slug'], "rarity": pair['rarity'], "limit": sales_to_fetch})
        new_sales_from_api = []
        if api_data and api_data.get("data") and not api_data.get("errors"):
            for sale in api_data["data"].get("tokens", {}).get("tokenPrices", []):
                new_sales_from_api.append({"timestamp": datetime.strptime(sale['date'], "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000, "price": sale['amounts']['eurCents'] / 100, "seasonEligibility": "IN_SEASON" if sale['card']['inSeasonEligible'] else "CLASSIC"})
        old_sales_from_sheet = []
        if existing_info:
            record = existing_info['record']
            for j in range(1, MAX_SALES_TO_DISPLAY + 1):
                date_str, price_val = record.get(f"Sale {j} Date"), record.get(f"Sale {j} Price (EUR)")
                if date_str and price_val:
                    old_sales_from_sheet.append({"timestamp": datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp() * 1000, "price": float(str(price_val).replace(",", ".")), "seasonEligibility": record.get(f"Sale {j} Eligibility")})
        combined_sales = sorted(list({int(s['timestamp']): s for s in old_sales_from_sheet + new_sales_from_api}.values()), key=lambda x: x['timestamp'], reverse=True)[:MAX_SALES_TO_DISPLAY]
        headers = sales_sheet.row_values(1) if sales_sheet.row_count > 0 else []
        updated_row = build_sales_history_row(pair['name'], pair['slug'], pair['rarity'], combined_sales, headers)
        row_to_update = existing_info['row_index'] if existing_info else sales_sheet.row_count + len(updates_to_batch) + 1
        updates_to_batch.append({'range': f'A{row_to_update}', 'values': [updated_row]})
        if not existing_info: existing_sales_map[key] = {"row_index": row_to_update}
        time.sleep(1)
    if updates_to_batch:
        print(f"Invio {len(updates_to_batch)} aggiornamenti a 'Cronologia Vendite'...")
        sales_sheet.batch_update(updates_to_batch, value_input_option='USER_ENTERED')
    print("Esecuzione completata.")
    if 'update_sales_continuation' in state: del state['update_sales_continuation']
    save_state(state)
    execution_time = time.time() - start_time
    send_telegram_notification(f"✅ <b>Cronologia Vendite Aggiornata (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s")
if __name__ == "__main__":
    if len(sys.argv) > 1:
        function_to_run = sys.argv[1]
        if function_to_run == "initial_setup": initial_setup()
        elif function_to_run == "update_cards": update_cards()
        elif function_to_run == "update_sales": update_sales()
        elif function_to_run == "update_floors": update_floors()
        else: print(f"Errore: Funzione '{function_to_run}' non riconosciuta.")
    else:
        print("Nessuna funzione specificata.")
