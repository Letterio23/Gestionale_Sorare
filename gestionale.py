# Importazioni necessari
import os
import sys
import requests
import json
import time
from datetime import datetime, timedelta
import gspread

# --- 1. CONFIGURAZIONE ---
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
MAX_SALES_TO_DISPLAY = 100
MAX_SALES_FROM_API = 7
INITIAL_SALES_FETCH_COUNT = 20
CARD_DATA_UPDATE_INTERVAL_HOURS = 0.5
MAIN_SHEET_HEADERS = ["Slug", "Rarity", "Player Name", "Player API Slug", "Position", "U23 Eligible?", "Livello", "In Season?", "XP Corrente", "XP Prox Livello", "XP Mancanti Livello", "Sale Price (EUR)", "Fee Abilitata?", "Foto URL", "Owner Since", "Last 15 SO5 Scores", "L5 So5 (%)", "L15 So5 (%)", "Avg So5 Score (3)", "Avg So5 Score (5)", "Avg So5 Score (15)", "Projection Grade", "Projected Score", "Projection Reliability (%)", "Starter Odds (%)", "Data Prossima Partita", "Next Game API ID", "Partita", "Infortunio", "Squalifica", "FLOOR CLASSIC LIMITED", "FLOOR IN SEASON LIMITED", "FLOOR CLASSIC RARE", "FLOOR IN SEASON RARE", "FLOOR CLASSIC SR", "FLOOR IN SEASON SR", "Ultimo Aggiornamento"]
CHART_SHEET_NAME = "Grafici SO5"
GRADIENT_STOPS = {
    0: {'r': 255, 'g': 80, 'b': 80},      # Red
    40: {'r': 255, 'g': 255, 'b': 0},   # Yellow
    60: {'r': 37, 'g': 237, 'b': 54},   # Green
    75: {'r': 0, 'g': 243, 'b': 235},   # Light Blue
    100: {'r': 193, 'g': 229, 'b': 237} # Silver
}

# --- 2. QUERY GRAPHQL ---
ALL_CARDS_QUERY = """
    query AllCardsFromUser($userSlug: String!, $rarities: [Rarity!], $cursor: String) {
        user(slug: $userSlug) {
            cards(rarities: $rarities, after: $cursor, first: 50) {
                nodes { ... on Card { slug, rarity, ownerSince, player { ... on Player { displayName, slug, position, u23Eligible } } } }
                pageInfo { endCursor, hasNextPage }
            }
        }
    }
"""

PLAYER_TOKEN_PRICES_QUERY = """
    query GetPlayerTokenPrices($playerSlug: String!, $rarity: Rarity!, $limit: Int!) {
        tokens {
            tokenPrices(playerSlug: $playerSlug, rarity: $rarity, first: $limit, includePrivateSales: true) {
                amounts { eurCents }
                date
                card { inSeasonEligible }
            }
        }
    }
"""

PRICE_FRAGMENT = "liveSingleSaleOffer { receiverSide { amounts { eurCents, usdCents, gbpCents, wei, referenceCurrency } } }"

OPTIMIZED_CARD_DETAILS_QUERY = f"""
    query GetOptimizedCardDetails($cardSlug: String!) {{
        anyCard(slug: $cardSlug) {{
            ... on Card {{
                rarity, grade, xp, xpNeededForNextGrade, pictureUrl, inSeasonEligible, secondaryMarketFeeEnabled
                liveSingleSaleOffer {{ receiverSide {{ amounts {{ eurCents, usdCents, gbpCents, wei, referenceCurrency }} }} }}
                player {{
                    slug, displayName, position, lastFiveSo5Appearances, lastFifteenSo5Appearances
                    playerGameScores(last: 15) {{ score }}
                    activeInjuries {{ status, expectedEndDate }}
                    activeSuspensions {{ reason, endDate }}
                    activeClub {{ name, upcomingGames(first: 1) {{ id, date, competition {{ displayName }}, homeTeam {{ ... on TeamInterface {{ name }} }}, awayTeam {{ ... on TeamInterface {{ name }} }} }} }}
                    u23Eligible
                    L_ANY: lowestPriceAnyCard(rarity: limited, inSeason: false) {{ {PRICE_FRAGMENT} }}
                    L_IN: lowestPriceAnyCard(rarity: limited, inSeason: true) {{ {PRICE_FRAGMENT} }}
                    R_ANY: lowestPriceAnyCard(rarity: rare, inSeason: false) {{ {PRICE_FRAGMENT} }}
                    R_IN: lowestPriceAnyCard(rarity: rare, inSeason: true) {{ {PRICE_FRAGMENT} }}
                    SR_ANY: lowestPriceAnyCard(rarity: super_rare, inSeason: false) {{ {PRICE_FRAGMENT} }}
                    SR_IN: lowestPriceAnyCard(rarity: super_rare, inSeason: true) {{ {PRICE_FRAGMENT} }}
                }}
            }}
        }}
    }}
"""

PROJECTION_QUERY = """
    query GetProjection($playerSlug: String!, $gameId: ID!) {
        football {
            player(slug: $playerSlug) {
                playerGameScore(gameId: $gameId) {
                    projection { grade score reliabilityBasisPoints }
                    anyPlayerGameStats {
                        ... on PlayerGameStats {
                            footballPlayingStatusOdds { starterOddsBasisPoints }
                        }
                    }
                }
            }
        }
    }
"""

# --- 3. FUNZIONI HELPER ---
def load_state():
    try:
        with open(STATE_FILE, "r") as f: 
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): 
        return {}

def save_state(state_data):
    with open(STATE_FILE, "w") as f: 
        json.dump(state_data, f, indent=2)

def sorare_graphql_fetch(query, variables={}):
    payload = {"query": query, "variables": variables}
    headers = {
        "APIKEY": SORARE_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Sorare-ApiVersion": "v1"
    }
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        if response.status_code == 422:
            try:
                error_details = response.json()
                print(f"AVVISO: Dati non processabili per {variables}. Dettagli API: {error_details}")
            except json.JSONDecodeError:
                print(f"AVVISO: Dati non processabili per {variables}. Risposta non JSON: {response.text}")
            return None
        response.raise_for_status()
        data = response.json()
        if "errors" in data:
            print(f"ERRORE GraphQL per {variables}: {data['errors']}")
        return data
    except requests.exceptions.HTTPError as e:
        print(f"Errore HTTP: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Errore di rete generico: {e}")
        return None

def send_telegram_notification(text):
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]): 
        return
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try: 
        requests.post(api_url, json=payload, timeout=10)
    except Exception: 
        pass

def get_eth_rate():
    try:
        response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur", timeout=5)
        response.raise_for_status()
        return response.json()["ethereum"]["eur"]
    except Exception: 
        return 3000.0

def get_currency_rates():
    try:
        response = requests.get("https://api.exchangerate-api.com/v4/latest/EUR", timeout=5)
        response.raise_for_status()
        rates = response.json().get('rates', {})
        return {'usd_to_eur': 1 / rates.get('USD', 1.08), 'gbp_to_eur': 1 / rates.get('GBP', 0.85)}
    except Exception:
        return {'usd_to_eur': 0.92, 'gbp_to_eur': 1.17}

def calculate_eur_price(price_object, rates):
    if not price_object or not rates: 
        return ""
    try:
        amounts = price_object.get('liveSingleSaleOffer', {}).get('receiverSide', {}).get('amounts')
        if not amounts: 
            return ""
        amounts_data = amounts[0] if isinstance(amounts, list) else amounts
        currency = amounts_data.get('referenceCurrency', '').lower()
        euro_value = 0
        if currency == 'eur': 
            euro_value = amounts_data.get('eurCents', 0) / 100
        elif currency == 'usd': 
            euro_value = (amounts_data.get('usdCents', 0) / 100) * rates.get('usd_to_eur', 0.92)
        elif currency == 'gbp': 
            euro_value = (amounts_data.get('gbpCents', 0) / 100) * rates.get('gbp_to_eur', 1.17)
        elif currency in ['eth', 'wei']:
            wei_value = amounts_data.get('wei')
            if wei_value is not None: 
                euro_value = (float(wei_value) / 1e18) * rates.get('eth_to_eur', 3000)
        return round(euro_value, 2) if euro_value > 0 else ""
    except (TypeError, KeyError, IndexError, AttributeError, ValueError): 
        return ""

def fetch_projection(player_slug, game_id):
    if not player_slug or not game_id: 
        return None
    clean_game_id = str(game_id).replace("Game:", "")
    data = sorare_graphql_fetch(PROJECTION_QUERY, {"playerSlug": player_slug, "gameId": clean_game_id})
    return data.get("data", {}).get("football", {}).get("player", {}).get("playerGameScore") if data else None

# ... (rest of helper and main functions unchanged for brevity)

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
                if key not in pairs_map: 
                    pairs_map[key] = {"slug": slug, "rarity": rarity.lower(), "name": record.get("Player Name")}
        continuation_data['pairs_to_process'] = list(pairs_map.values())
        print("Leggo lo storico vendite esistente...")
        try:
            continuation_data['existing_sales_map'] = { f"{rec.get('Player API Slug')}::{rec.get('Rarity Searched')}": {"row_index": i + 2, "record": rec} for i, rec in enumerate(sales_sheet.get_all_records()) }
        except gspread.exceptions.GSpreadException as e:
            print(f"Attenzione: il foglio '{SALES_HISTORY_SHEET_NAME}' sembra vuoto o malformato. Verrà trattato come vuoto. Dettagli: {e}")
            continuation_data['existing_sales_map'] = {}
    pairs_to_process = continuation_data.get('pairs_to_process', [])
    existing_sales_map = continuation_data.get('existing_sales_map', {})
    updates_to_batch = []
    new_rows_to_append = []

    expected_headers = ["Player Name", "Player API Slug", "Rarity Searched", "Sales Today (In-Season)", "Sales Today (Classic)"]
    periods = [3, 7, 14, 30]
    for p in periods: 
        expected_headers.extend([f"Avg Price {p}d (In-Season)", f"Avg Price {p}d (Classic)"])
    for j in range(1, MAX_SALES_TO_DISPLAY + 1): 
        expected_headers.extend([f"Sale {j} Date", f"Sale {j} Price (EUR)", f"Sale {j} Eligibility"])
    expected_headers.append("Last Updated")
    
    try:
        existing_headers = sales_sheet.row_values(1) if sales_sheet.row_count > 0 else []
    except:
        existing_headers = []
    
    headers_need_update = False
    if not existing_headers:
        print("Nessun header trovato. Creo nuovi header.")
        headers_need_update = True
    elif len(existing_headers) != len(expected_headers):
        print(f"Numero di colonne diverso: esistenti={len(existing_headers)}, attesi={len(expected_headers)}. Aggiorno header.")
        headers_need_update = True
    elif existing_headers != expected_headers:
        print("Header esistenti diversi da quelli attesi. Aggiorno header.")
        headers_need_update = True
    
    if headers_need_update:
        try:
            existing_data = sales_sheet.get_all_values()
            if len(existing_data) > 1:
                print(f"Salvataggio di {len(existing_data)-1} righe di dati esistenti...")
        except:
            existing_data = []
        
        num_expected_cols = len(expected_headers)
        current_cols = sales_sheet.col_count
        
        print(f"Ridimensionamento foglio: da {current_cols} colonne a {num_expected_cols} colonne")
        sales_sheet.resize(rows=1000, cols=num_expected_cols)
        sales_sheet.clear()
        sales_sheet.update(range_name='A1', values=[expected_headers])
        header_range = f'A1:{chr(64 + num_expected_cols)}1'
        sales_sheet.format(header_range, {'textFormat': {'bold': True}})
        if len(existing_data) > 1:
            print("⚠️  ATTENZIONE: Gli header sono stati modificati.")
            print("Si consiglia di verificare manualmente i dati nel foglio 'Cronologia Vendite'.")
        print("Header aggiornati e foglio ridimensionato correttamente.")
    else:
        print("Header già corretti, nessun aggiornamento necessario.")
    
    headers = expected_headers

    for i in range(start_index, len(pairs_to_process)):
        if time.time() - start_time > 480: # Aumentato a 8 minuti per sicurezza
            print(f"Timeout imminente. Salvo stato all'indice {i}.")
            continuation_data['last_index'] = i
            state['update_sales_continuation'] = continuation_data
            save_state(state)
            if updates_to_batch: 
                sales_sheet.batch_update(updates_to_batch, value_input_option='USER_ENTERED')
            if new_rows_to_append: 
                sales_sheet.append_rows(new_rows_to_append, value_input_option='USER_ENTERED')
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
                new_sales_from_api.append({
                    "timestamp": datetime.strptime(sale['date'], "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000,
                    "price": sale['amounts']['eurCents'] / 100,
                    "seasonEligibility": "IN_SEASON" if sale['card']['inSeasonEligible'] else "CLASSIC"
                })
        old_sales_from_sheet = []
        if existing_info:
            record = existing_info['record']
            for j in range(1, MAX_SALES_TO_DISPLAY + 1):
                date_str, price_val = record.get(f"Sale {j} Date"), record.get(f"Sale {j} Price (EUR)")
                if date_str and price_val:
                    price = parse_price(price_val)
                    if price is not None:
                        try:
                            timestamp = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp() * 1000
                            eligibility = record.get(f"Sale {j} Eligibility")
                            old_sales_from_sheet.append({"timestamp": timestamp, "price": price, "seasonEligibility": eligibility})
                        except (ValueError, TypeError):
                            continue
        combined_sales = sorted(list({int(s['timestamp']): s for s in old_sales_from_sheet + new_sales_from_api}.values()), key=lambda x: x['timestamp'], reverse=True)[:MAX_SALES_TO_DISPLAY]
        updated_row = build_sales_history_row(pair['name'], pair['slug'], pair['rarity'], combined_sales, headers)
        if existing_info:
            updates_to_batch.append({'range': f'A{existing_info["row_index"]}', 'values': [updated_row]})
        else:
            new_rows_to_append.append(updated_row)
            existing_sales_map[key] = {'row_index': 'new'}
        time.sleep(1)
    if updates_to_batch:
        print(f"Invio {len(updates_to_batch)} aggiornamenti a '{SALES_HISTORY_SHEET_NAME}'...")
        sales_sheet.batch_update(updates_to_batch, value_input_option='USER_ENTERED')
    if new_rows_to_append:
        print(f"Aggiunta di {len(new_rows_to_append)} nuove righe a '{SALES_HISTORY_SHEET_NAME}'...")
        sales_sheet.append_rows(new_rows_to_append, value_input_option='USER_ENTERED')
    print("Esecuzione completata.")
    if 'update_sales_continuation' in state: 
        del state['update_sales_continuation']
    save_state(state)
    execution_time = time.time() - start_time
    send_telegram_notification(f"✅ <b>Cronologia Vendite Aggiornata (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s")

# ... (resto del file invariato)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        function_to_run = sys.argv[1]
        if function_to_run == "sync_galleria": 
            sync_galleria()
        elif function_to_run == "update_cards": 
            update_cards()
        elif function_to_run == "update_sales": 
            update_sales()
        elif function_to_run == "update_floors": 
            update_floors()
        elif function_to_run == "create_charts": 
            create_so5_charts()
        else: 
            print(f"Errore: Funzione '{function_to_run}' non riconosciuta.")
    else:
        print("Nessuna funzione specificata. Le funzioni disponibili sono: sync_galleria, update_cards, update_sales, update_floors, create_charts.")
