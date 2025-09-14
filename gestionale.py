# Importazioni necessarie
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

MAIN_SHEET_HEADERS = [
    "Slug", "Rarity", "Player Name", "Player API Slug", "Position", "U23 Eligible?", "Livello", "In Season?", 
    "XP Corrente", "XP Prox Livello", "XP Mancanti Livello", "Sale Price (EUR)", "FLOOR CLASSIC LIMITED", 
    "FLOOR CLASSIC RARE", "FLOOR CLASSIC SR", "FLOOR IN SEASON LIMITED", "FLOOR IN SEASON RARE", 
    "FLOOR IN SEASON SR", "L5 So5 (%)", "L15 So5 (%)", "Avg So5 Score (3)", "Avg So5 Score (5)", 
    "Avg So5 Score (15)", "Last 5 SO5 Scores", "Partita", "Data Prossima Partita", "Next Game API ID", 
    "Projection Grade", "Projected Score", "Projection Reliability (%)", "Starter Odds (%)", "Fee Abilitata?", 
    "Infortunio", "Squalifica", "Ultimo Aggiornamento", "Owner Since", "Foto URL"
]

# --- 2. QUERY GRAPHQL ---
ALL_CARDS_QUERY = """
    query AllCardsFromUser($userSlug: String!, $rarities: [Rarity!], $cursor: String) {
        user(slug: $userSlug) {
            cards(rarities: $rarities, after: $cursor, first: 50) {
                nodes {
                    ... on Card {
                        slug, rarity, ownerSince
                        player { ... on Player { displayName, slug, position, u23Eligible } }
                    }
                }
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
CARD_DETAILS_QUERY = """
    query GetCardDetails($cardSlug: String!) {
        anyCard(slug: $cardSlug) {
            ... on Card {
                rarity, grade, xp, xpNeededForNextGrade, pictureUrl, inSeasonEligible, secondaryMarketFeeEnabled
                liveSingleSaleOffer { receiverSide { amounts { eurCents, wei, referenceCurrency } } }
                player {
                    slug, displayName, position, lastFiveSo5Appearances, lastFifteenSo5Appearances
                    playerGameScores(last: 15) { score }
                    activeInjuries { status, expectedEndDate }
                    activeSuspensions { reason, endDate }
                    activeClub {
                        name
                        upcomingGames(first: 1) { id, date, competition { displayName }, homeTeam { ... on TeamInterface { name } }, awayTeam { ... on TeamInterface { name } } }
                    }
                    u23Eligible
                }
            }
        }
    }
"""
PRICE_FRAGMENT = "liveSingleSaleOffer { receiverSide { amounts { eurCents, wei, referenceCurrency } } }"
SINGLE_PLAYER_FLOORS_QUERY = f"""
    query GetSinglePlayerFloorPrices($playerSlug: String!) {{
        football {{
            player(slug: $playerSlug) {{
                L_ANY: lowestPriceAnyCard(rarity: limited, inSeason: false) {{ {PRICE_FRAGMENT} }}
                L_IN: lowestPriceAnyCard(rarity: limited, inSeason: true) {{ {PRICE_FRAGMENT} }}
                R_ANY: lowestPriceAnyCard(rarity: rare, inSeason: false) {{ {PRICE_FRAGMENT} }}
                R_IN: lowestPriceAnyCard(rarity: rare, inSeason: true) {{ {PRICE_FRAGMENT} }}
                SR_ANY: lowestPriceAnyCard(rarity: super_rare, inSeason: false) {{ {PRICE_FRAGMENT} }}
                SR_IN: lowestPriceAnyCard(rarity: super_rare, inSeason: true) {{ {PRICE_FRAGMENT} }}
            }}
        }}
    }}
"""
PROJECTION_QUERY = """
    query GetProjection($playerSlug: String!, $gameId: ID!) {
        football {
            player(slug: $playerSlug) {
                playerGameScore(gameId: $gameId) {
                    projection {{ grade score reliabilityBasisPoints }}
                    anyPlayerGameStats {{ ... on PlayerGameStats {{ footballPlayingStatusOdds {{ starterOddsBasisPoints }} }} }}
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
    headers = {
        "APIKEY": SORARE_API_KEY, "Content-Type": "application/json", "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9", "X-Sorare-ApiVersion": "v1"
    }
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "errors" in data: print(f"ERRORE GraphQL per variabili {variables}: {data['errors']}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Errore di rete durante la chiamata API: {e}")
        return None

def send_telegram_notification(text):
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]): return
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(api_url, json=payload, timeout=10)
    except Exception as e:
        print(f"Errore invio notifica Telegram: {e}")

def get_eth_rate():
    try:
        response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur", timeout=5)
        response.raise_for_status()
        return response.json()["ethereum"]["eur"]
    except Exception: return 3000.0

def calculate_eur_price(price_object, rates):
    if not price_object or not rates: return ""
    try:
        amounts = price_object.get('liveSingleSaleOffer', {}).get('receiverSide', {}).get('amounts')
        if not amounts: return ""
        amounts_data = amounts[0] if isinstance(amounts, list) else amounts
        currency = amounts_data.get('referenceCurrency', '').lower()
        if currency == 'eur': return amounts_data.get('eurCents', 0) / 100
        elif currency in ['eth', 'wei']: return (float(amounts_data.get('wei', 0)) / 1e18) * rates.get('eth_to_eur', 3000)
    except (TypeError, KeyError, IndexError, AttributeError): return ""
    return ""

def get_single_player_floors(player_slug, rates):
    if not player_slug: return {}
    data = sorare_graphql_fetch(SINGLE_PLAYER_FLOORS_QUERY, {"playerSlug": player_slug})
    p = data.get("data", {}).get("football", {}).get("player") if data else None
    if not p: return {}
    return {
        "FLOOR CLASSIC LIMITED": calculate_eur_price(p.get('L_ANY'), rates), "FLOOR IN SEASON LIMITED": calculate_eur_price(p.get('L_IN'), rates),
        "FLOOR CLASSIC RARE": calculate_eur_price(p.get('R_ANY'), rates), "FLOOR IN SEASON RARE": calculate_eur_price(p.get('R_IN'), rates),
        "FLOOR CLASSIC SR": calculate_eur_price(p.get('SR_ANY'), rates), "FLOOR IN SEASON SR": calculate_eur_price(p.get('SR_IN'), rates),
    }

def fetch_projection(player_slug, game_id):
    if not player_slug or not game_id: return None
    clean_game_id = str(game_id).replace("Game:", "")
    data = sorare_graphql_fetch(PROJECTION_QUERY, {"playerSlug": player_slug, "gameId": clean_game_id})
    return data.get("data", {}).get("football", {}).get("player", {}).get("playerGameScore") if data else None

def build_updated_card_row(original_record, card_details, floor_prices, projection_data, rates):
    record = original_record.copy()
    player_details = card_details.get("player", {})
    if not player_details: return [original_record.get(h, '') for h in MAIN_SHEET_HEADERS]
    
    record.update(floor_prices)

    if projection_data:
        proj = projection_data.get('projection')
        if proj:
            record["Projection Grade"] = proj.get('grade', 'G')
            record["Projected Score"] = proj.get('score')
            if proj.get('reliabilityBasisPoints') is not None: record["Projection Reliability (%)"] = f"{int(proj['reliabilityBasisPoints'] / 100)}%"
        
        stats = projection_data.get('anyPlayerGameStats')
        if stats and stats.get('footballPlayingStatusOdds') and stats['footballPlayingStatusOdds'].get('starterOddsBasisPoints') is not None:
            record["Starter Odds (%)"] = f"{int(stats['footballPlayingStatusOdds']['starterOddsBasisPoints'] / 100)}%"
    
    record["Livello"] = card_details.get("grade")
    record["XP Corrente"] = card_details.get("xp")
    record["XP Prox Livello"] = card_details.get("xpNeededForNextGrade")
    if record["XP Prox Livello"] is not None and record["XP Corrente"] is not None: record["XP Mancanti Livello"] = record["XP Prox Livello"] - record["XP Corrente"]
    record["In Season?"] = "Sì" if card_details.get("inSeasonEligible") else "No"
    record["Fee Abilitata?"] = "Sì" if card_details.get("secondaryMarketFeeEnabled") else "No"
    record["Foto URL"] = card_details.get("pictureUrl", "")
    record["Sale Price (EUR)"] = calculate_eur_price(card_details, rates)
    
    l5, l15 = player_details.get('lastFiveSo5Appearances'), player_details.get('lastFifteenSo5Appearances')
    if l5 is not None: record["L5 So5 (%)"] = f"{int((l5 / 5) * 100)}%"
    if l15 is not None: record["L15 So5 (%)"] = f"{int((l15 / 15) * 100)}%"
    
    scores = [s.get('score') for s in player_details.get("playerGameScores", []) if s and s.get('score') is not None]
    if scores:
        if len(scores) >= 3: record["Avg So5 Score (3)"] = round(sum(scores[:3]) / 3, 2)
        if len(scores) >= 5: record["Avg So5 Score (5)"] = round(sum(scores[:5]) / 5, 2)
        record["Avg So5 Score (15)"] = round(sum(scores) / len(scores), 2) if scores else ""
        record["Last 5 SO5 Scores"] = ", ".join(map(str, scores[:5]))

    injuries = player_details.get("activeInjuries", [])
    if injuries and injuries[0].get('expectedEndDate'):
        end_date_str = injuries[0].get('expectedEndDate')
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).strftime('%d/%m/%y')
        record["Infortunio"] = f"{injuries[0].get('status', 'Infortunato')} fino al {end_date}"
    else: record["Infortunio"] = ""

    suspensions = player_details.get("activeSuspensions", [])
    if suspensions and suspensions[0].get('endDate'):
        end_date_str = suspensions[0].get('endDate')
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).strftime('%d/%m/%y')
        record["Squalifica"] = f"{suspensions[0].get('reason', 'Squalificato')} fino al {end_date}"
    else: record["Squalifica"] = ""
        
    club = player_details.get("activeClub")
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

# --- 4. FUNZIONI PRINCIPALI ---
def initial_setup():
    print("--- INIZIO PRIMO AGGIORNAMENTO COMPLETO ---")
    
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        try:
            sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
            sheet.clear()
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=MAIN_SHEET_NAME, rows="1000", cols=len(MAIN_SHEET_HEADERS))
        
        sheet.update(range_name='A1', values=[MAIN_SHEET_HEADERS])
        sheet.format(f'A1:{gspread.utils.rowcol_to_a1(1, len(MAIN_SHEET_HEADERS))}', {'textFormat': {'bold': True}})
        print(f"Foglio '{MAIN_SHEET_NAME}' preparato con tutte le intestazioni.")
    except Exception as e:
        print(f"ERRORE CRITICO GSheets: {e}")
        return

    print("Recupero di tutte le carte dall'API di Sorare...")
    all_cards = []
    cursor = None
    has_next_page = True
    while has_next_page:
        variables = {"userSlug": USER_SLUG, "rarities": ["limited", "rare", "super_rare", "unique"], "cursor": cursor}
        data = sorare_graphql_fetch(ALL_CARDS_QUERY, variables)
        if not data or "errors" in data or not data.get("data", {}).get("user", {}).get("cards"):
            print("Risposta API non valida o con errori. Interruzione.")
            break
        
        cards_data = data["data"]["user"]["cards"]
        all_cards.extend(cards_data.get("nodes", []))
        page_info = cards_data.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
        print(f"Recuperate {len(all_cards)} carte finora...")
        if has_next_page: time.sleep(1)

    print(f"Recupero completato. Trovate {len(all_cards)} carte in totale.")
    empty_record = {header: "" for header in MAIN_SHEET_HEADERS}
    data_to_write = []
    for card in all_cards:
        player = card.get("player") or {}
        record = empty_record.copy()
        record["Slug"] = card.get("slug", "")
        record["Rarity"] = card.get("rarity", "")
        record["Owner Since"] = card.get("ownerSince", "")
        record["Player Name"] = player.get("displayName", "")
        record["Player API Slug"] = player.get("slug", "")
        record["Position"] = player.get("position", "")
        record["U23 Eligible?"] = "Sì" if player.get("u23Eligible") else "No"
        data_to_write.append([record[header] for header in MAIN_SHEET_HEADERS])

    if data_to_write:
        sheet.update(range_name='A2', values=data_to_write, value_input_option='USER_ENTERED')
        print(f"Il foglio '{MAIN_SHEET_NAME}' è stato popolato con {len(all_cards)} carte.")
    else:
        print("Nessuna carta trovata da scrivere.")
    send_telegram_notification(f"✅ <b>Primo Avvio Completato (GitHub)</b>\n\nIl foglio contiene {len(all_cards)} carte.")

def update_cards():
    """Traduzione completa di updateAllCardData_V3."""
    print("--- INIZIO AGGIORNAMENTO DATI CARTE ---")
    start_time = time.time()
    state = load_state()
    continuation_data = state.get('update_cards_continuation', {})
    start_index = continuation_data.get('last_index', 0)
    
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
        print("Connessione a Google Sheets riuscita.")
    except Exception as e:
        print(f"ERRORE CRITICO GSheets: {e}")
        return

    rates = {"eth_to_eur": get_eth_rate()}

    if start_index == 0:
        print("Avvio nuova sessione. Identifico le carte da aggiornare.")
        all_sheet_records = sheet.get_all_records()
        api_cards_data = sorare_graphql_fetch(ALL_CARDS_QUERY, {"userSlug": USER_SLUG})
        
        api_cards = api_cards_data.get("data", {}).get("user", {}).get("cards", {}).get("nodes", []) if api_cards_data else []
        api_card_slugs = {card['slug'] for card in api_cards}
        
        cutoff_time = datetime.now() - timedelta(hours=CARD_DATA_UPDATE_INTERVAL_HOURS)
		cards_to_process = []
		for i, record in enumerate(all_sheet_records):
  		  record['row_index'] = i + 2
   		  last_update_str = record.get('Ultimo Aggiornamento')
    
		    # --- MODIFICA CHIAVE QUI ---
		    # Sostituiamo la logica complessa con una semplice istruzione
		    # per forzare l'aggiornamento di ogni carta
		    if True: 
		    # ---------------------------
		        cards_to_process.append(record)
		            except ValueError:
		                cards_to_process.append(record)
        
        print(f"Identificate {len(cards_to_process)} carte da aggiornare.")
        continuation_data['cards_to_process'] = cards_to_process
    else:
        print(f"Ripresa sessione dall'indice {start_index}.")
        cards_to_process = continuation_data.get('cards_to_process', [])

    if not cards_to_process:
        print("Nessuna carta da aggiornare in questa sessione.")
        return
        
    for i in range(start_index, len(cards_to_process)):
        if time.time() - start_time > 300: # Timeout 5 min
            print(f"Timeout imminente. Salvo stato all'indice {i}.")
            continuation_data['last_index'] = i
            state['update_cards_continuation'] = continuation_data
            save_state(state)
            return
            
        card_to_update = cards_to_process[i]
        card_slug = card_to_update.get('Slug')
        if not card_slug: continue
        
        print(f"Aggiorno carta ({i+1}/{len(cards_to_process)}): {card_slug}")

        details_data = sorare_graphql_fetch(CARD_DETAILS_QUERY, {"cardSlug": card_slug})
        if not details_data or not details_data.get("data", {}).get("anyCard"):
            time.sleep(1)
            continue
        
        card_details = details_data["data"]["anyCard"]
        player_info = card_details.get("player", {})
        player_slug = player_info.get("slug")
        upcoming_games = player_info.get("activeClub", {}).get("upcomingGames", [])
        game_id = upcoming_games[0].get("id") if upcoming_games else None

        floor_prices = get_single_player_floors(player_slug, rates)
        time.sleep(1)
        projection_data = fetch_projection(player_slug, game_id)
        
        updated_row = build_updated_card_row(card_to_update, card_details, floor_prices, projection_data, rates)
        
        try:
            sheet.update(range_name=f'A{card_to_update["row_index"]}', values=[updated_row], value_input_option='USER_ENTERED')
        except Exception as e:
            print(f"Errore durante l'aggiornamento della riga per {card_slug}: {e}")
        time.sleep(1.5)

    print("Esecuzione completata. Pulizia dello stato.")
    if 'update_cards_continuation' in state:
        del state['update_cards_continuation']
    save_state(state)
    
    execution_time = time.time() - start_time
    send_telegram_notification(f"✅ <b>Dati Carte Aggiornati (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s")

def update_sales():
    print("--- INIZIO AGGIORNAMENTO CRONOLOGIA VENDITE ---")
    start_time = time.time()
    state = load_state()
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
        print("Avvio nuova sessione. Preparo i dati e il foglio di destinazione.")
        main_records = main_sheet.get_all_records()
        pairs_map = {}
        for record in main_records:
            slug, rarity = record.get("Player API Slug"), record.get("Rarity")
            if slug and rarity:
                key = f"{slug}::{rarity}"
                if key not in pairs_map:
                    pairs_map[key] = {"slug": slug, "rarity": rarity.lower(), "name": record.get("Player Name")}
        pairs_to_process = list(pairs_map.values())
        continuation_data['pairs_to_process'] = pairs_to_process
        exp_headers = ["Player Name", "Player API Slug", "Rarity Searched", "Sales Today (In-Season)", "Sales Today (Classic)"]
        periods = [3, 7, 14, 30]
        for p in periods: exp_headers.extend([f"Avg Price {p}d (In-Season)", f"Avg Price {p}d (Classic)"])
        for j in range(1, MAX_SALES_TO_DISPLAY + 1): exp_headers.extend([f"Sale {j} Date", f"Sale {j} Price (EUR)", f"Sale {j} Eligibility"])
        exp_headers.append("Last Updated")
        sales_sheet.clear()
        sales_sheet.update(range_name='A1', values=[exp_headers])
        sales_sheet.format(f'A1:{gspread.utils.rowcol_to_a1(1, len(exp_headers))}', {'textFormat': {'bold': True}})
        continuation_data['exp_headers'] = exp_headers
        all_rows_to_write = []
    else:
        print(f"Ripresa sessione dall'indice {start_index}.")
        pairs_to_process = continuation_data.get('pairs_to_process', [])
        all_rows_to_write = continuation_data.get('all_rows_to_write', [])
    for i in range(start_index, len(pairs_to_process)):
        if time.time() - start_time > 300: # Timeout
            print(f"Timeout imminente. Salvo stato all'indice {i}.")
            continuation_data['last_index'] = i
            continuation_data['all_rows_to_write'] = all_rows_to_write
            state['update_sales_continuation'] = continuation_data
            save_state(state)
            return
        pair = pairs_to_process[i]
        key = f"{pair['slug']}::{pair['rarity']}"
        print(f"Processo ({i+1}/{len(pairs_to_process)}): {key}")
        sales_to_fetch = INITIAL_SALES_FETCH_COUNT
        api_data = sorare_graphql_fetch(PLAYER_TOKEN_PRICES_QUERY, {"playerSlug": pair['slug'], "rarity": pair['rarity'], "limit": sales_to_fetch})
        all_unique_sales = []
        if api_data and api_data.get("data") and not api_data.get("errors"):
            prices = api_data["data"].get("tokens", {}).get("tokenPrices", [])
            for sale in prices:
                all_unique_sales.append({
                    "timestamp": datetime.strptime(sale['date'], "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000,
                    "price": sale['amounts']['eurCents'] / 100,
                    "seasonEligibility": "IN_SEASON" if sale['card']['inSeasonEligible'] else "CLASSIC"
                })
        all_unique_sales.sort(key=lambda x: x['timestamp'], reverse=True)
        now_ms = time.time() * 1000
        today_start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        out_row = [pair['name'], pair['slug'], pair['rarity']]
        sales_today_in_season = len([s for s in all_unique_sales if datetime.fromtimestamp(s['timestamp']/1000) >= today_start_dt and s['seasonEligibility'] == "IN_SEASON"])
        sales_today_classic = len([s for s in all_unique_sales if datetime.fromtimestamp(s['timestamp']/1000) >= today_start_dt and s['seasonEligibility'] != "IN_SEASON"])
        out_row.extend([sales_today_in_season, sales_today_classic])
        for p in [3, 7, 14, 30]:
            is_prices, cl_prices = [], []
            for s in all_unique_sales:
                if s['timestamp'] >= now_ms - (p * 86400000):
                    if s['seasonEligibility'] == "IN_SEASON": is_prices.append(s['price'])
                    else: cl_prices.append(s['price'])
            out_row.append(sum(is_prices)/len(is_prices) if is_prices else "")
            out_row.append(sum(cl_prices)/len(cl_prices) if cl_prices else "")
        for j in range(MAX_SALES_TO_DISPLAY):
            if j < len(all_unique_sales):
                sale = all_unique_sales[j]
                out_row.extend([datetime.fromtimestamp(sale['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S'), sale['price'], sale['seasonEligibility']])
            else:
                out_row.extend(["", "", ""])
        out_row.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        all_rows_to_write.append(out_row)
        time.sleep(1)
    if all_rows_to_write:
        print("Scrittura di tutte le righe sul foglio 'Cronologia Vendite'...")
        sales_sheet.update(range_name='A2', values=all_rows_to_write, value_input_option='USER_ENTERED')
    print("Esecuzione completata. Pulizia dello stato.")
    if 'update_sales_continuation' in state:
        del state['update_sales_continuation']
    save_state(state)
    execution_time = time.time() - start_time
    send_telegram_notification(f"✅ <b>Cronologia Vendite Aggiornata (GitHub)</b>\n\n⏱️ Tempo: {execution_time:.2f}s")

def update_floors():
    print("--- INIZIO AGGIORNAMENTO FLOOR PRICES ---")
    start_time = time.time()
    try:
        credentials = json.loads(GSPREAD_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
        print("Connessione a Google Sheets riuscita.")
    except Exception as e:
        print(f"ERRORE CRITICO GSheets: {e}")
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
        print(f"Processo lotto {i//BATCH_SIZE + 1}/{ -(-len(slugs_to_process) // BATCH_SIZE) }...")
        query_parts = [
            f'p_{idx}: player(slug: "{slug}") {{ '
            f'L_IN: lowestPriceAnyCard(rarity: limited, inSeason: true) {{ {price_fields_fragment} }} '
            f'L_ANY: lowestPriceAnyCard(rarity: limited, inSeason: false) {{ {price_fields_fragment} }} '
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
        updated_sheet_values = [list(row) for row in all_sheet_values]
        for row_idx, row in enumerate(updated_sheet_values[1:], start=1):
            slug_in_row = row[slug_col_idx_header]
            if slug_in_row in floor_prices_map:
                prices = floor_prices_map[slug_in_row]
                for col_name, col_idx in target_columns_indices.items():
                    price_value = prices.get(col_name)
                    if price_value is not None:
                        updated_sheet_values[row_idx][col_idx] = price_value
        sheet.update(range_name='A1', values=updated_sheet_values, value_input_option='USER_ENTERED')
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
