import json, re
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
import pandas as pd

FEEDS = [
    ("PV-Tech", "https://www.pv-tech.org/feed/"),
    ("Energy Storage News", "https://www.energy-storage.news/feed/"),
    ("IEA News", "https://www.iea.org/rss/news.xml"),
    ("IRENA", "https://www.irena.org/rss"),
    ("Bundesnetzagentur", "https://www.bundesnetzagentur.de/SiteGlobals/Functions/RSSFeed/rss_nachrichten.xml?nn=268128"),
    ("EU Energy", "https://ec.europa.eu/newsroom/ener/rss.cfm?type=atom"),
]

REGION_MAP = [
    (re.compile(r"\bGermany|German|Bundesnetzagentur|BNetzA|Fraunhofer|TenneT|50Hertz|Amprion|TransnetBW\b", re.I), ("Germany","DE")),
    (re.compile(r"\bIndia|MNRE|SECI|CEA|CERC|Gujarat|Maharashtra|Rajasthan|Grid-India|POSOCO\b", re.I), ("India","IN")),
    (re.compile(r"\bEurope|European Commission|EU\b", re.I), ("Europe","EU")),
]

CATEGORY_GUESS = [
    (re.compile(r"\bauction|tender|RfS|RFP|bid|awarded\b", re.I), "tender"),
    (re.compile(r"\bpolicy|regulation|consultation|state-aid|EEG|grid code\b", re.I), "policy"),
    (re.compile(r"\bproject|NTP|COD|commission|construction\b", re.I), "project"),
    (re.compile(r"\bprice|tariff|LCOE|module price|battery price\b", re.I), "price"),
    (re.compile(r"\bancillary|capacity market|balancing|curtailment\b", re.I), "grid"),
    (re.compile(r"\bOEM|inverter|module|battery|technology\b", re.I), "tech"),
]

def infer_region_country(text):
    for rx, rc in REGION_MAP:
        if rx.search(text or ""):
            return rc
    try:
        host = urlparse(text).netloc
    except Exception:
        host = ""
    if host.endswith(".de"):
        return ("Germany","DE")
    return ("Global","")

def guess_category(text):
    for rx, cat in CATEGORY_GUESS:
        if rx.search(text or ""):
            return cat
    return "market"

def score_impact(text):
    score = 3
    if re.search(r"\b(GW|[3-9]\d{2}\s*MW)\b", text, re.I):  # >=300 MW
        score = 4
    if re.search(r"\b(>?\s*1000\s*MW|gigawatt)\b", text, re.I):
        score = 5
    if re.search(r"\bpolicy|auction result|capacity market|state-aid\b", text, re.I):
        score = max(score, 4)
    return score

def normalize(src_name, e):
    title = (e.get("title") or "").strip()
    summary = (e.get("summary") or e.get("subtitle") or "").strip()
    link = e.get("link","")
    dt = e.get("published_parsed") or e.get("updated_parsed")
    if dt:
        date_utc = datetime(*dt[:6], tzinfo=timezone.utc).strftime("%Y-%m-%d")
    else:
        date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    text = f"{title} {summary}"
    region, country = infer_region_country(text + " " + link)
    category = guess_category(text)
    impact = score_impact(text)

    item = {
        "date_utc": date_utc,
        "headline": title,
        "summary_80w": re.sub(r"\s+", " ", summary)[:600],
        "region": region,
        "country": country or ("EU" if region=="Europe" else ""),
        "category": category,
        "subtopic": "",
        "capacity_MW": 0,
        "storage_MWh": 0,
        "project_stage": "NA",
        "tariff_or_price_local": "",
        "tariff_or_price_usd": "",
        "entities": [],
        "location": {"state_or_länder":"", "city":"", "grid_zone":""},
        "effective_date": date_utc,
        "impact_score_1to5": impact,
        "tags": ["utility-scale"],
        "source_name": src_name,
        "source_url": link,
        "reliability": "official" if src_name in ("Bundesnetzagentur","IEA","IRENA","EU Energy") else "trade",
        "notes": ""
    }
    if re.search(r"\bhybrid|PV\+BESS|RTC\b", text, re.I): item["tags"].append("PV+BESS")
    if re.search(r"\bwind\b", text, re.I): item["tags"].append("wind")
    if re.search(r"\bPV|solar\b", text, re.I): item["tags"].append("PV")
    if re.search(r"\bbattery|BESS|storage\b", text, re.I): item["tags"].append("storage")
    return item

def main():
    items=[]
    for name,url in FEEDS:
        try:
            feed=feedparser.parse(url)
            for e in feed.entries[:50]:
                items.append(normalize(name,e))
        except Exception as ex:
            print("Feed failed:", name, url, ex)

    seen=set(); dedup=[]
    for it in items:
        k=(it["headline"], it["source_url"])
        if k in seen: continue
        seen.add(k); dedup.append(it)

    df = pd.DataFrame(dedup).sort_values(["date_utc","impact_score_1to5"], ascending=[False, False])
    df.to_csv("docs/dataset.csv", index=False)
    df.to_json("docs/dataset.json", orient="records", indent=2)

if __name__ == "__main__":
    main()

