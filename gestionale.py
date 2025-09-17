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
MAIN_SHEET_HEADERS = ["Slug", "Rarity", "Player Name", "Player API Slug", "Position", "U23 Eligible?", "Livello", "In Season?", "XP Corrente", "XP Prox Livello", "XP Mancanti Livello", "Sale Price (EUR)", "FLOOR CLASSIC LIMITED", "FLOOR CLASSIC RARE", "FLOOR CLASSIC SR", "FLOOR IN SEASON LIMITED", "FLOOR IN SEASON RARE", "FLOOR IN SEASON SR", "L5 So5 (%)", "L15 So5 (%)", "Avg So5 Score (3)", "Avg So5 Score (5)", "Avg So5 Score (15)", "Last 5 SO5 Scores", "Partita", "Data Prossima Partita", "Next Game API ID", "Projection Grade", "Projected Score", "Projection Reliability (%)", "Starter Odds (%)", "Fee Abilitata?", "Infortunio", "Squalifica", "Ultimo Aggiornamento", "Owner Since", "Foto URL"]
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
                    anyPlayerGameStats { footballPlayingStatusOdds { starterOddsBasisPoints } }
                }
            }
        }
    }
"""
# --- 3. FUNZIONI HELPER ---
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
        if "errors" in data: print(f"ERRORE GraphQL per {variables}: {data['errors']}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Errore di rete: {e}")
        return None
def send_telegram_notification(text):
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]): return
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try: requests.post(api_url, json=payload, timeout=10)
    except Exception: pass
def get_eth_rate():
    try:
        response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur", timeout=5)
        response.raise_for_status()
        return response.json()["ethereum"]["eur"]
    except Exception: return 3000.0
def get_currency_rates():
    try:
        response = requests.get("https://api.exchangerate-api.com/v4/latest/EUR", timeout=5)
        response.raise_for_status()
        rates = response.json().get('rates', {})
        return {'usd_to_eur': 1 / rates.get('USD', 1.08), 'gbp_to_eur': 1 / rates.get('GBP', 0.85)}
    except Exception:
        return {'usd_to_eur': 0.92, 'gbp_to_eur': 1.17}
def calculate_eur_price(price_object, rates):
    if not price_object or not rates: return ""
    try:
        amounts = price_object.get('liveSingleSaleOffer', {}).get('receiverSide', {}).get('amounts')
        if not amounts: return ""
        amounts_data = amounts[0] if isinstance(amounts, list) else amounts
        currency = amounts_data.get('referenceCurrency', '').lower()
        euro_value = 0
        if currency == 'eur': euro_value = amounts_data.get('eurCents', 0) / 100
        elif currency == 'usd': euro_value = (amounts_data.get('usdCents', 0) / 100) * rates.get('usd_to_eur', 0.92)
        elif currency == 'gbp': euro_value = (amounts_data.get('gbpCents', 0) / 100) * rates.get('gbp_to_eur', 1.17)
        elif currency in ['eth', 'wei']:
            wei_value = amounts_data.get('wei')
            if wei_value is not None: euro_value = (float(wei_value) / 1e18) * rates.get('eth_to_eur', 3000)
        return round(euro_value, 2) if euro_value > 0 else ""
    except (TypeError, KeyError, IndexError, AttributeError, ValueError): return ""
def fetch_projection(player_slug, game_id):
    if not player_slug or not game_id: return None
    clean_game_id = str(game_id).replace("Game:", "")
    data = sorare_graphql_fetch(PROJECTION_QUERY, {"playerSlug": player_slug, "gameId": clean_game_id})
    return data.get("data", {}).get("football", {}).get("player", {}).get("playerGameScore") if data else None
def build_updated_card_row(original_record, card_details, player_info, projection_data, rates):
    record = original_record.copy()
    if not player_info: player_info = card_details.get("player", {})
    record["FLOOR CLASSIC LIMITED"] = calculate_eur_price(player_info.get('L_ANY'), rates)
    record["FLOOR IN SEASON LIMITED"] = calculate_eur_price(player_info.get('L_IN'), rates)
    record["FLOOR CLASSIC RARE"] = calculate_eur_price(player_info.get('R_ANY'), rates)
    record["FLOOR IN SEASON RARE"] = calculate_eur_price(player_info.get('R_IN'), rates)
    record["FLOOR CLASSIC SR"] = calculate_eur_price(player_info.get('SR_ANY'), rates)
    record["FLOOR IN SEASON SR"] = calculate_eur_price(player_info.get('SR_IN'), rates)
    if projection_data:
        proj = projection_data.get('projection')
        if proj:
            record["Projection Grade"], record["Projected Score"] = proj.get('grade', 'G'), proj.get('score')
            if proj.get('reliabilityBasisPoints') is not None: record["Projection Reliability (%)"] = f"{int(proj['reliabilityBasisPoints'] / 100)}%"
        stats = projection_data.get('anyPlayerGameStats')
        if stats and stats.get('footballPlayingStatusOdds') and stats['footballPlayingStatusOdds'].get('starterOddsBasisPoints') is not None:
            record["Starter Odds (%)"] = f"{int(stats['footballPlayingStatusOdds']['starterOddsBasisPoints'] / 100)}%"
    record["Livello"], record["XP Corrente"], record["XP Prox Livello"] = card_details.get("grade"), card_details.get("xp"), card_details.get("xpNeededForNextGrade")
    if record["XP Prox Livello"] is not None and record["XP Corrente"] is not None: record["XP Mancanti Livello"] = record["XP Prox Livello"] - record["XP Corrente"]
    record["In Season?"], record["Fee Abilitata?"] = "Sì" if card_details.get("inSeasonEligible") else "No", "Sì" if card_details.get("secondaryMarketFeeEnabled") else "No"
    record["Foto URL"], record["Sale Price (EUR)"] = card_details.get("pictureUrl", ""), calculate_eur_price(card_details, rates)
    l5, l15 = player_info.get('lastFiveSo5Appearances'), player_info.get('lastFifteenSo5Appearances')
    if l5 is not None: record["L5 So5 (%)"] = f"{int((l5 / 5) * 100)}%"
    if l15 is not None: record["L15 So5 (%)"] = f"{int((l15 / 15) * 100)}%"
    scores = [s.get('score') for s in player_info.get("playerGameScores", []) if s and s.get('score') is not None]
    if scores:
        if len(scores) >= 3: record["Avg So5 Score (3)"] = round(sum(scores[:3]) / 3, 2)
        if len(scores) >= 5: record["Avg So5 Score (5)"] = round(sum(scores[:5]) / 5, 2)
        record["Avg So5 Score (15)"] = round(sum(scores) / len(scores), 2) if scores else ""
        record["Last 5 SO5 Scores"] = ", ".join(map(str, scores[:5]))
    injuries = player_info.get("activeInjuries", [])
    if injuries and injuries[0].get('expectedEndDate'):
        end_date_str = injuries[0]['expectedEndDate']
        if end_date_str:
            end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).strftime('%d/%m/%y')
            record["Infortunio"] = f"{injuries[0].get('status', 'Infortunato')} fino al {end_date}"
    else: record["Infortunio"] = ""
    suspensions = player_info.get("activeSuspensions", [])
    if suspensions and suspensions[0].get('endDate'):
        end_date_str = suspensions[0]['endDate']
        if end_date_str:
            end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).strftime('%d/%m/%y')
            record["Squalifica"] = f"{suspensions[0].get('reason', 'Squalificato')} fino al {end_date}"
    else: record["Squalifica"] = ""
    club = player_info.get("activeClub")
    if club and club.get("upcomingGames"):
        game = club["upcomingGames"][0]
        if game and game.get('date'):
            game_date = datetime.fromisoformat(game['date'].replace("Z", "+00:00")).strftime('%d-%m-%y %H:%M')
            home, away, comp = game.get("homeTeam", {}).get("name", ""), game.get("awayTeam", {}).get("name", ""), game.get("competition", {}).get("displayName", "")
            record["Data Prossima Partita"], record["Next Game API ID"] = game_date, game.get("id", "")
            record["Partita"] = f"🏠 vs {away} [{comp}]" if home == club.get("name") else f"✈️ vs {home} [{comp}]"
        else: record["Partita"], record["Data Prossima Partita"], record["Next Game API ID"] = "Data non disp.", "", ""
    else: record["Partita"], record["Data Prossima Partita"], record["Next Game API ID"] = "Nessuna partita", "", ""
    record["Ultimo Aggiornamento"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return [record.get(header, '') for header in MAIN_SHEET_HEADERS]
def parse_price(price_val):
    if price_val is None or price_val == '':
        return None
    price_str = str(price_val).strip()

    # Check for non-numeric characters that are not separators
    # This is a simple check; a more robust regex might be even better.
    allowed_chars = "0123456789.,-"
    if not all(char in allowed_chars for char in price_str):
        # Attempt to strip non-standard currency symbols if any, like '€'
        price_str = ''.join(filter(lambda x: x in allowed_chars, price_str))

    has_comma = ',' in price_str
    has_dot = '.' in price_str

    # Handle formats like "1.234,56" (Italian) and "1,234.56" (English)
    if has_comma and has_dot:
        if price_str.rfind(',') > price_str.rfind('.'):
            # Italian format: remove dots, replace comma with dot
            price_str = price_str.replace('.', '').replace(',', '.')
        else:
            # English format: remove commas
            price_str = price_str.replace(',', '')
    elif has_comma:
        # Only comma is present, assume it's a decimal separator e.g., "1234,56"
        price_str = price_str.replace(',', '.')

    try:
        return float(price_str)
    except (ValueError, TypeError):
        # If everything fails, return None to indicate a parsing error
        return None

def get_gradient_color(score):
    """Calculates the RGBA string for a score based on a predefined gradient for Chart.js."""
    if score is None:
        return "rgba(200, 200, 200, 1)"  # Grey for DNPs

    try:
        score = max(0, min(100, float(score)))
    except (ValueError, TypeError):
        return "rgba(200, 200, 200, 1)"

    sorted_stops = sorted(GRADIENT_STOPS.keys())

    start_score, end_score = sorted_stops[0], sorted_stops[-1]
    for i in range(len(sorted_stops) - 1):
        if sorted_stops[i] <= score <= sorted_stops[i+1]:
            start_score, end_score = sorted_stops[i], sorted_stops[i+1]
            break

    start_color, end_color = GRADIENT_STOPS[start_score], GRADIENT_STOPS[end_score]

    if score == start_score:
        return f"rgba({start_color['r']}, {start_color['g']}, {start_color['b']}, 1)"
    if score == end_score:
        return f"rgba({end_color['r']}, {end_color['g']}, {end_color['b']}, 1)"

    score_range = float(end_score - start_score)
    percentage = (score - start_score) / score_range if score_range > 0 else 0

    r = start_color['r'] + (end_color['r'] - start_color['r']) * percentage
    g = start_color['g'] + (end_color['g'] - start_color['g']) * percentage
    b = start_color['b'] + (end_color['b'] - start_color['b']) * percentage

    return f"rgba({int(r)}, {int(g)}, {int(b)}, 1)"

def build_sales_history_row(name, slug, rarity, all_sales, headers):
    now_ms = time.time() * 1000
    today_start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    out_row_map = {"Player Name": name, "Player API Slug": slug, "Rarity Searched": rarity}
    sales_today_in_season = len([s for s in all_sales if datetime.fromtimestamp(s['timestamp']/1000) >= today_start_dt and s['seasonEligibility'] == "IN_SEASON"])
    sales_today_classic = len([s for s in all_sales if datetime.fromtimestamp(s['timestamp']/1000) >= today_start_dt and s['seasonEligibility'] != "IN_SEASON"])
    out_row_map["Sales Today (In-Season)"] = sales_today_in_season
    out_row_map["Sales Today (Classic)"] = sales_today_classic
    for p in [3, 7, 14, 30]:
        is_prices, cl_prices = [], []
        for s in all_sales:
            if s['timestamp'] >= now_ms - (p * 86400000):
                if s['seasonEligibility'] == "IN_SEASON": is_prices.append(s['price'])
                else: cl_prices.append(s['price'])
        out_row_map[f"Avg Price {p}d (In-Season)"] = round(sum(is_prices)/len(is_prices), 2) if is_prices else ""
        out_row_map[f"Avg Price {p}d (Classic)"] = round(sum(cl_prices)/len(cl_prices), 2) if cl_prices else ""
    for j in range(MAX_SALES_TO_DISPLAY):
        if j < len(all_sales):
            sale = all_sales[j]
            out_row_map[f"Sale {j+1} Date"] = datetime.fromtimestamp(sale['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
            out_row_map[f"Sale {j+1} Price (EUR)"] = sale['price']
            out_row_map[f"Sale {j+1} Eligibility"] = sale['seasonEligibility']
        else:
            out_row_map[f"Sale {j+1} Date"], out_row_map[f"Sale {j+1} Price (EUR)"], out_row_map[f"Sale {j+1} Eligibility"] = "", "", ""
    out_row_map["Last Updated"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return [out_row_map.get(h, '') for h in headers]
# --- 4. FUNZIONI PRINCIPALI ---
def sync_galleria():
    print("--- INIZIO SINCRONIZZAZIONE GALLERIA ---")
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        try:
            sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
            if not sheet.row_values(1):
                 sheet.update(range_name='A1', values=[MAIN_SHEET_HEADERS])
                 sheet.format(f'A1:{gspread.utils.rowcol_to_a1(1, len(MAIN_SHEET_HEADERS))}', {'textFormat': {'bold': True}})
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=MAIN_SHEET_NAME, rows="1", cols=len(MAIN_SHEET_HEADERS))
            sheet.update(range_name='A1', values=[MAIN_SHEET_HEADERS])
            sheet.format(f'A1:{gspread.utils.rowcol_to_a1(1, len(MAIN_SHEET_HEADERS))}', {'textFormat': {'bold': True}})
            print(f"Foglio '{MAIN_SHEET_NAME}' creato.")
    except Exception as e:
        print(f"ERRORE CRITICO GSheets in sync_galleria: {e}")
        return
    print("Recupero di tutte le carte dall'API di Sorare...")
    api_cards = []
    cursor, has_next_page = None, True
    while has_next_page:
        variables = {"userSlug": USER_SLUG, "rarities": ["limited", "rare", "super_rare", "unique"], "cursor": cursor}
        data = sorare_graphql_fetch(ALL_CARDS_QUERY, variables)
        if not data or "errors" in data or not data.get("data", {}).get("user", {}).get("cards"):
            break
        cards_data = data["data"]["user"]["cards"]
        api_cards.extend(cards_data.get("nodes", []))
        page_info = cards_data.get("pageInfo", {})
        has_next_page, cursor = page_info.get("hasNextPage", False), page_info.get("endCursor")
        if has_next_page: time.sleep(1)
    api_card_slugs = {card['slug'] for card in api_cards}
    print(f"Recupero completato. Trovate {len(api_card_slugs)} carte uniche in totale.")
    print("Leggo le carte presenti nel foglio Google...")
    try:
        sheet_records = sheet.get_all_records()
        sheet_card_slugs = {record['Slug']: {'row_index': i + 2} for i, record in enumerate(sheet_records) if record.get('Slug')}
    except gspread.exceptions.GSpreadException as e:
        print(f"Attenzione: il foglio '{MAIN_SHEET_NAME}' sembra vuoto o malformato. Verrà trattato come vuoto. Dettagli: {e}")
        sheet_card_slugs = {}
    print(f"Trovate {len(sheet_card_slugs)} carte nel foglio.")
    slugs_to_add = api_card_slugs - sheet_card_slugs.keys()
    slugs_to_delete = sheet_card_slugs.keys() - api_card_slugs
    if slugs_to_delete:
        rows_to_delete = sorted([sheet_card_slugs[slug]['row_index'] for slug in slugs_to_delete], reverse=True)
        print(f"Rimozione di {len(rows_to_delete)} righe...")
        for row_index in rows_to_delete:
            try:
                sheet.delete_rows(row_index)
                time.sleep(1.5)
            except Exception as e:
                print(f"Errore durante la rimozione della riga {row_index}: {e}")
    if slugs_to_add:
        new_cards_data = [card for card in api_cards if card['slug'] in slugs_to_add]
        data_to_write = []
        empty_record = {header: "" for header in MAIN_SHEET_HEADERS}
        for card in new_cards_data:
            player = card.get("player") or {}
            record = empty_record.copy()
            record["Slug"], record["Rarity"], record["Owner Since"] = card.get("slug", ""), card.get("rarity", ""), card.get("ownerSince", "")
            record["Player Name"], record["Player API Slug"] = player.get("displayName", ""), player.get("slug", "")
            record["Position"], record["U23 Eligible?"] = player.get("position", ""), "Sì" if player.get("u23Eligible") else "No"
            data_to_write.append([record.get(header, '') for header in MAIN_SHEET_HEADERS])
        if data_to_write:
            print(f"Aggiunta di {len(data_to_write)} nuove carte al foglio...")
            sheet.append_rows(data_to_write, value_input_option='USER_ENTERED')
    message = f"✅ <b>Sincronizzazione Galleria Completata</b>\n\nGalleria: {len(api_card_slugs)} carte\n➕ Aggiunte: {len(slugs_to_add)}\n➖ Rimosse: {len(slugs_to_delete)}"
    print(message)
    send_telegram_notification(message)
def update_cards():
    print("--- INIZIO AGGIORNAMENTO DATI CARTE (OTTIMIZZATO) ---")
    start_time, state = time.time(), load_state()
    continuation_data = state.get('update_cards_continuation', {})
    start_index = continuation_data.get('last_index', 0)
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(MAIN_SHEET_NAME)
        print("Connessione a Google Sheets riuscita.")
    except Exception as e:
        print(f"ERRORE CRITICO GSheets: {e}")
        return
    rates = {"eth_to_eur": get_eth_rate()}
    rates.update(get_currency_rates())
    if start_index == 0:
        print("Avvio nuova sessione...")
        all_sheet_records = sheet.get_all_records()
        cutoff_time = datetime.now() - timedelta(hours=CARD_DATA_UPDATE_INTERVAL_HOURS)
        cards_to_process = []
        for i, record in enumerate(all_sheet_records):
            record['row_index'] = i + 2
            last_update_str = record.get('Ultimo Aggiornamento', '').strip()
            if not last_update_str:
                cards_to_process.append(record)
                continue
            try:
                if datetime.strptime(last_update_str, '%Y-%m-%d %H:%M:%S') < cutoff_time:
                    cards_to_process.append(record)
            except ValueError:
                cards_to_process.append(record)
        print(f"Identificate {len(cards_to_process)} carte da aggiornare.")
        continuation_data['cards_to_process'] = cards_to_process
    else:
        print(f"Ripresa sessione dall'indice {start_index}.")
        cards_to_process = continuation_data.get('cards_to_process', [])
    if not cards_to_process:
        print("Nessuna carta da aggiornare.")
        if 'update_cards_continuation' in state: del state['update_cards_continuation']
        save_state(state)
        return
    for i in range(start_index, len(cards_to_process)):
        if time.time() - start_time > 300:
            print(f"Timeout imminente. Salvo stato all'indice {i}.")
            continuation_data['last_index'] = i
            state['update_cards_continuation'] = continuation_data
            save_state(state)
            return
        card_to_update = cards_to_process[i]
        card_slug = card_to_update.get('Slug')
        if not card_slug: continue
        print(f"Aggiorno carta ({i+1}/{len(cards_to_process)}): {card_slug}")
        details_data = sorare_graphql_fetch(OPTIMIZED_CARD_DETAILS_QUERY, {"cardSlug": card_slug})
        if not details_data or not details_data.get("data", {}).get("anyCard"):
            time.sleep(1)
            continue
        card_details = details_data["data"]["anyCard"]
        player_info = card_details.get("player")
        player_slug = player_info.get("slug") if player_info else None
        upcoming_games = []
        if player_info and player_info.get("activeClub"):
            upcoming_games = player_info.get("activeClub", {}).get("upcomingGames", [])
        game_id = upcoming_games[0].get("id") if upcoming_games else None
        projection_data = fetch_projection(player_slug, game_id)
        updated_row = build_updated_card_row(card_to_update, card_details, player_info, projection_data, rates)
        try:
            sheet.update(range_name=f'A{card_to_update["row_index"]}', values=[updated_row], value_input_option='USER_ENTERED')
        except Exception as e:
            print(f"Errore aggiornamento riga per {card_slug}: {e}")
        time.sleep(1)
    print("Esecuzione completata. Pulizia dello stato.")
    if 'update_cards_continuation' in state: del state['update_cards_continuation']
    save_state(state)
    execution_time = time.time() - start_time
    send_telegram_notification(f"✅ <b>Dati Carte Aggiornati (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s")
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
        try:
            continuation_data['existing_sales_map'] = { f"{rec.get('Player API Slug')}::{rec.get('Rarity Searched')}": {"row_index": i + 2, "record": rec} for i, rec in enumerate(sales_sheet.get_all_records()) }
        except gspread.exceptions.GSpreadException as e:
            print(f"Attenzione: il foglio '{SALES_HISTORY_SHEET_NAME}' sembra vuoto o malformato. Verrà trattato come vuoto. Dettagli: {e}")
            continuation_data['existing_sales_map'] = {}
    pairs_to_process = continuation_data.get('pairs_to_process', [])
    existing_sales_map = continuation_data.get('existing_sales_map', {})
    updates_to_batch = []
    new_rows_to_append = []
    for i in range(start_index, len(pairs_to_process)):
        if time.time() - start_time > 480: # Aumentato a 8 minuti per sicurezza
            print(f"Timeout imminente. Salvo stato all'indice {i}.")
            continuation_data['last_index'] = i
            state['update_sales_continuation'] = continuation_data
            save_state(state)
            if updates_to_batch: sales_sheet.batch_update(updates_to_batch, value_input_option='USER_ENTERED')
            if new_rows_to_append: sales_sheet.append_rows(new_rows_to_append, value_input_option='USER_ENTERED')
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
                    price = parse_price(price_val)
                    if price is not None:
                        try:
                            timestamp = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp() * 1000
                            eligibility = record.get(f"Sale {j} Eligibility")
                            old_sales_from_sheet.append({"timestamp": timestamp, "price": price, "seasonEligibility": eligibility})
                        except (ValueError, TypeError):
                            continue # Skip if date is malformed
        combined_sales = sorted(list({int(s['timestamp']): s for s in old_sales_from_sheet + new_sales_from_api}.values()), key=lambda x: x['timestamp'], reverse=True)[:MAX_SALES_TO_DISPLAY]
        headers = sales_sheet.row_values(1) if sales_sheet.row_count > 0 else []
        if not headers:
             exp_headers = ["Player Name", "Player API Slug", "Rarity Searched", "Sales Today (In-Season)", "Sales Today (Classic)"]
             periods = [3, 7, 14, 30]
             for p in periods: exp_headers.extend([f"Avg Price {p}d (In-Season)", f"Avg Price {p}d (Classic)"])
             for j in range(1, MAX_SALES_TO_DISPLAY + 1): exp_headers.extend([f"Sale {j} Date", f"Sale {j} Price (EUR)", f"Sale {j} Eligibility"])
             exp_headers.append("Last Updated")
             sales_sheet.update('A1', [exp_headers])
             headers = exp_headers
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
    if 'update_sales_continuation' in state: del state['update_sales_continuation']
    save_state(state)
    execution_time = time.time() - start_time
    send_telegram_notification(f"✅ <b>Cronologia Vendite Aggiornata (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s")
def update_floors():
    pass

import urllib.parse

def generate_chart_config(player_name, scores):
    """Generates a Chart.js configuration dictionary for a player's SO5 scores."""
    colors = [get_gradient_color(s) for s in scores]

    chart_config = {
        'type': 'bar',
        'data': {
            'labels': [f'G{i+1}' for i in range(len(scores))],
            'datasets': [{
                'label': 'SO5 Score',
                'data': scores,
                'backgroundColor': colors,
                'borderColor': 'rgba(0, 0, 0, 0.2)',
                'borderWidth': 1
            }]
        },
        'options': {
            'title': {
                'display': True,
                'text': player_name,
                'fontSize': 16
            },
            'legend': {
                'display': False
            },
            'scales': {
                'yAxes': [{
                    'ticks': {
                        'beginAtZero': True,
                        'max': 100,
                        'stepSize': 10
                    }
                }],
                'xAxes': [{
                    'gridLines': {
                        'display': False
                    }
                }]
            }
        }
    }
    return chart_config

def create_so5_charts():
    """Creates a new sheet with QuickChart.io chart images for each player."""
    print("--- INIZIO CREAZIONE GRAFICI SO5 (QuickChart.io) ---")
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        main_sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
    except Exception as e:
        print(f"ERRORE CRITICO GSheets: {e}")
        return

    # Get or create the chart sheet
    try:
        chart_sheet = spreadsheet.worksheet(CHART_SHEET_NAME)
    except gspread.WorksheetNotFound:
        chart_sheet = spreadsheet.add_worksheet(title=CHART_SHEET_NAME, rows=1000, cols=5)
        print(f"Foglio '{CHART_SHEET_NAME}' creato.")

    chart_sheet.clear()
    chart_sheet.update('A1:B1', [['Giocatore', 'Grafico Ultimi 5 Punteggi SO5']])
    chart_sheet.format('A1:B1', {'textFormat': {'bold': True}})
    print("Foglio dei grafici pulito e intestazioni scritte.")

    # Read player data from the main sheet
    all_records = main_sheet.get_all_records()
    players_with_scores = [r for r in all_records if r.get("Last 5 SO5 Scores", "").strip()]
    if not players_with_scores:
        print("Nessun giocatore con punteggi SO5 trovato.")
        return

    print(f"Trovati {len(players_with_scores)} giocatori con punteggi SO5 da processare.")

    # Prepare data for batch update
    update_data = []
    for i, player in enumerate(players_with_scores):
        scores_str = player.get("Last 5 SO5 Scores")
        scores = [s.strip() if s.strip().upper() != 'DNP' else '0' for s in scores_str.split(',') if s.strip()]
        if not scores:
            continue

        player_name = player.get("Player Name")
        # The API gives scores from most recent to least recent. We reverse for the chart to show recent on right.
        reversed_scores = scores[::-1]

        # Generate chart config and URL
        chart_config = generate_chart_config(player_name, reversed_scores)
        config_str = json.dumps(chart_config, separators=(',', ':'))
        encoded_config = urllib.parse.quote(config_str)
        chart_url = f"https://quickchart.io/chart?w=500&h=300&bkg=white&c={encoded_config}"

        # Create the formula for the image
        image_formula = f'=IMAGE("{chart_url}")'

        # Add player name and image formula to the update list
        row_index = i + 2  # +2 because sheet is 1-indexed and we have a header
        update_data.append({'range': f'A{row_index}', 'values': [[player_name]]})
        update_data.append({'range': f'B{row_index}', 'values': [[image_formula]]})

    # Batch write all formulas to the sheet
    if update_data:
        print(f"Scrittura di {len(players_with_scores)} grafici nel foglio...")
        chart_sheet.batch_update(update_data, value_input_option='USER_ENTERED')

    # Adjust column and row sizes
    chart_sheet.set_frozen(rows=1)
    chart_sheet.update_acell('C1', "Nota: I grafici sono immagini generate da QuickChart.io")
    spreadsheet.batch_update({
        "requests": [
            {"updateSheetProperties": {"properties": {"sheetId": chart_sheet.id, "gridProperties": {"rowHeight": 25}}, "fields": "gridProperties.rowHeight"}},
            {"updateSheetProperties": {"properties": {"sheetId": chart_sheet.id, "gridProperties": {"frozenRowCount": 1}},"fields": "gridProperties.frozenRowCount"}},
            {"updateDimensionProperties": {"range": {"sheetId": chart_sheet.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 200}}},
            {"updateDimensionProperties": {"range": {"sheetId": chart_sheet.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 510}}},
            {"updateDimensionProperties": {"range": {"sheetId": chart_sheet.id, "dimension": "ROWS", "startIndex": 1, "endIndex": len(players_with_scores) + 1}, "properties": {"pixelSize": 310}}},
        ]
    })

    print(f"--- CREAZIONE GRAFICI COMPLETATA. {len(players_with_scores)} grafici aggiunti a '{CHART_SHEET_NAME}'. ---")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        function_to_run = sys.argv[1]
        if function_to_run == "sync_galleria": sync_galleria()
        elif function_to_run == "update_cards": update_cards()
        elif function_to_run == "update_sales": update_sales()
        elif function_to_run == "update_floors": update_floors()
        elif function_to_run == "create_charts": create_so5_charts()
        else: print(f"Errore: Funzione '{function_to_run}' non riconosciuta.")
    else:
        print("Nessuna funzione specificata. Le funzioni disponibili sono: sync_galleria, update_cards, update_sales, update_floors, create_charts.")
