import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def best_so5_x_xp(sheet, output_sheet_name="Formazione Best SO5 XP"):
    records = sheet.get_all_records()

    portieri = [r for r in records if r.get("Position") == "Goalkeeper"]
    difensori = [r for r in records if r.get("Position") == "Defender"]
    centrocampisti = [r for r in records if r.get("Position") == "Midfielder"]
    attaccanti = [r for r in records if r.get("Position") == "Forward"]

    portieri.sort(key=lambda x: float(x.get("Projected Score") or 0), reverse=True)
    difensori.sort(key=lambda x: float(x.get("Projected Score") or 0), reverse=True)
    centrocampisti.sort(key=lambda x: float(x.get("Projected Score") or 0), reverse=True)
    attaccanti.sort(key=lambda x: float(x.get("Projected Score") or 0), reverse=True)

    formazione = []
    if portieri:
        formazione.append(portieri[0])
    else:
        raise Exception("Nessun portiere disponibile")

    formazione.extend(difensori[:2])
    formazione.extend(centrocampisti[:2])

    if attaccanti:
        formazione.append(attaccanti[0])
    else:
        raise Exception("Nessun attaccante disponibile")

    extra_candidates = difensori[2:] + centrocampisti[2:] + attaccanti[1:]
    if extra_candidates:
        extra_candidates.sort(key=lambda x: float(x.get("Projected Score") or 0), reverse=True)
        formazione.append(extra_candidates[0])
    else:
        raise Exception("Nessun giocatore extra disponibile")

    total_score = sum(float(p.get("Projected Score") or 0) for p in formazione)
    xp = total_score / 5

    header = ["Player Name", "Position", "Projected Score"]
    values = [header]
    for p in formazione:
        values.append([p.get("Player Name"), p.get("Position"), p.get("Projected Score")])
    values.append(["", "Total XP", round(xp, 2)])

    try:
        output_sheet = sheet.spreadsheet.worksheet(output_sheet_name)
        output_sheet.clear()
    except gspread.WorksheetNotFound:
        output_sheet = sheet.spreadsheet.add_worksheet(title=output_sheet_name, rows=10, cols=5)

    output_sheet.update('A1', values)

def main():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds_json = os.environ.get("GSPREAD_CREDENTIALS")
    if not creds_json:
        raise Exception("GSPREAD_CREDENTIALS environment variable is not set")

    creds_dict = eval(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise Exception("SPREADSHEET_ID environment variable is not set")

    spreadsheet = client.open_by_key(spreadsheet_id)
    main_sheet = spreadsheet.sheet1  # Usa il foglio principale o cambia nome se serve

    best_so5_x_xp(main_sheet)

if __name__ == "__main__":
    main()
