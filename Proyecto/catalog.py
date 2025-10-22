def build_filter(collection, date_from, date_to, aoi_wkt=None, cloud=None):
    filt = f"Collection/Name eq '{collection}' and ContentDate/Start ge {date_from}T00:00:00.000Z and ContentDate/Start le {date_to}T23:59:59.999Z"
    if aoi_wkt:
        filt = f"Collection/Name eq '{collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{aoi_wkt}') and ContentDate/Start ge {date_from}T00:00:00.000Z and ContentDate/Start le {date_to}T23:59:59.999Z"
    # NO incluir cloud filter por ahora
    return filt

def query_catalog(filter_expr, top=500, max_pages=50):
    import requests
    items = []
    q = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter={filter_expr}&$count=True&$top={top}"
    headers = {"Accept": "application/json"}
    page = 0
    while q:
        page += 1
        r = requests.get(q, headers=headers, timeout=60)
        r.raise_for_status()
        js = r.json()
        items.extend(js.get("value", []))
        q = js.get("@odata.nextLink")
        if page >= max_pages:
            break
    return items

def items_to_df(items):
    import pandas as pd
    if not items:
        return pd.DataFrame()
    df = pd.json_normalize(items)
    if "Name" in df.columns:
        df["identifier"] = df["Name"].astype(str).str.split(".").str[0]
    if "ContentDate.Start" in df.columns:
        df["content_start"] = pd.to_datetime(df["ContentDate.Start"])
    else:
        df["content_start"] = pd.to_datetime(
            df.get("ContentDate", pd.NA).apply(lambda x: x.get("Start") if isinstance(x, dict) else None)
        )
    return df
