# viirs.py
import pandas as pd

def create_viirs_template(date_from, date_to, aoi_wkt=None):
    rows = [{
        "variable": "VIIRS_DNB_monthly",
        "date_from": date_from,
        "date_to": date_to,
        "aoi_wkt": aoi_wkt,
        "download_url": "",
        "notes": "Rellenar con enlaces VNL (NOAA/EOG)."
    }]
    df = pd.DataFrame(rows)
    return df
