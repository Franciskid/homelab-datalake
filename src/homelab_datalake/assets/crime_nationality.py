"""Collectors for the crime-by-nationality sources (parsing/crime_nationality.py has the formats and the method).

Each source is all-or-nothing and fails alone: the export then keeps yesterday's block, marked stale. Nothing here is scraped around a
protection: every file is an open-data API or a download the publisher links from its own page (the BKA's file is served after a cookie
redirect the standard library follows). The SSMSI's bilan and Info rapide are in assets/ssmsi_bilan.py: the interieur.gouv.fr HTML pages and the
IR PDFs behind its 503 are not touched; the bilan PDF is asked for once a year from the files host, which serves it.
"""

import json
import time
import urllib.request
from http.cookiejar import CookieJar

from homelab_datalake.parsing import crime_nationality as c

REQUEST_TIMEOUT_SECONDS = 240
ATTEMPTS = 3
USER_AGENT = "fraserv-datalake/1.0 (official statistics)"
FIRST_YEAR = 2007


def request(url: str, *, headers: dict | None = None, data: bytes | None = None, attempts: int = ATTEMPTS) -> bytes:
    if not url.startswith("https://"):
        raise ValueError(f"refusing a non-https source: {url!r}")
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
            req = urllib.request.Request(url, data=data, headers={"User-Agent": USER_AGENT, **(headers or {})})  # noqa: S310
            with opener.open(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return response.read()
        except Exception as error:  # noqa: BLE001 -- retried, then re-raised with its cause
            last = error
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{url} failed {attempts} times") from last


def text(url: str, **kwargs) -> str:
    return request(url, **kwargs).decode("utf-8-sig")


def as_json(url: str, **kwargs) -> dict | list:
    return json.loads(text(url, **kwargs))


def odata_items(url: str) -> list[dict]:
    """Every record of a CBS OData query, following the paging link."""
    out: list[dict] = []
    while url:
        page = as_json(url)
        out.extend(page["value"])
        url = page.get("odata.nextLink")
    return out


def odata_titles(table: str, dimension: str) -> dict[str, str]:
    return {x["Key"].strip(): x["Title"] for x in as_json(f"{c.CBS_ODATA}/{table}/{dimension}")["value"]}


def collect_crime(fetched_at: str, log, only: set[str] | None = None) -> tuple[list[dict], dict[str, dict], list[str]]:
    """Every source once; `only` (source names such as {'it_alleged'}) reruns just those, for a laptop retry."""
    rows: list[dict] = []
    blocks: dict[str, dict] = {}
    failed: list[str] = []

    def land(code: str, label: str, url: str, dims: list[str], parsed: list[dict], *, updated: str | None = None, extra: dict | None = None) -> None:
        if not parsed:
            raise RuntimeError(f"{code}: nothing parsed")
        blocks[f"{c.SOURCE}:{code}"] = c.block(code, label, url, dims, [[r[d] for d in dims] + [r["value"]] for r in parsed], fetched_at, updated=updated, extra=extra)
        rows.extend(c.ledger(code, parsed, dims, url, label, updated))

    def run(code: str, work) -> None:
        if only is not None and code not in only:
            return
        try:
            work()
        except Exception as error:  # noqa: BLE001 -- one publisher down must not stop the others
            log.warning("crime %s failed: %s", code, error)
            failed.append(f"{c.SOURCE}:{code}")

    # -- Switzerland
    def swiss() -> None:
        structure = json.loads(request(c.OFS_NATIONALITIES_URL, headers={"Accept": c.OFS_STRUCTURE_ACCEPT}))
        names = c.ofs_names(structure)
        parsed: list[dict] = []
        unmapped: dict[str, str] = {}
        for i, law in enumerate(c.OFS_LAWS):
            url = f"{c.OFS_BASE}/data/CH1.SUS,DF_SUS_NATIONALITIES_LAWS,1.0.0/{law}......."
            got, miss = c.ofs_sus_rows(text(url, headers={"Accept": c.OFS_DATA_ACCEPT}), names, c.OFS_LAWS[law], population=i == 0)
            parsed += got
            unmapped.update(miss)
        if unmapped:
            log.info("OFS nationalities without a country: %s", unmapped)
        land("ch_sus", "OFS, statistique des condamnations pénales (SUS) et STATPOP : personnes condamnées et population résidente selon la nationalité, la loi, le sexe, l'âge (2024)",
             c.OFS_EXPLORER, c.OFS_SUS_DIMS, parsed, extra={"unmapped": unmapped})

    run("ch_sus", swiss)

    def swiss_persons() -> None:
        parsed = c.ofs_persons_rows(text(c.OFS_PERSONS_URL, headers={"Accept": c.OFS_DATA_ACCEPT}))
        land("ch_persons", "OFS, statistique des condamnations pénales : personnes condamnées selon le Code pénal, le groupe de nationalité et le sexe, depuis 2007",
             c.OFS_EXPLORER.replace("DF_SUS_NATIONALITIES_LAWS", "DF_SUS_PERSONS"), c.OFS_PERSONS_DIMS, parsed)

    run("ch_persons", swiss_persons)

    # -- Germany
    def germany() -> None:
        parsed: list[dict] = []
        unmapped: set[str] = set()
        years = []
        for year in (2024, 2025):
            try:
                got, miss = c.bka_t62_rows(request(c.BKA_FILE.format(year=year)), year)
            except Exception as error:  # noqa: BLE001 -- a year not published yet is not a failure
                log.info("BKA T62 %s not read: %s", year, error)
                continue
            parsed += got
            unmapped.update(miss)
            years.append(year)
        if not years:
            raise RuntimeError("no BKA T62 workbook could be read")
        if unmapped:
            log.info("BKA nationalities without a country: %s", sorted(unmapped))
        land("de_pks_t62", "BKA, Polizeiliche Kriminalstatistik, tableau T62 : suspects selon l'infraction et la nationalité",
             c.BKA_PAGE.format(year=max(years)), c.BKA_DIMS, parsed, extra={"unmapped": sorted(unmapped)})

    run("de_pks_t62", germany)

    # -- Netherlands
    def netherlands() -> None:
        table = c.CBS_SUSPECTS
        titles = odata_titles(table, "Nationaliteit")
        items: list[dict] = []
        periods = [k for k in odata_titles(table, "Perioden") if c.cbs_period_year(k)]
        for offence in c.CBS_OFFENCES:  # CBS refuses a query that returns 10 000 records or more: one request per offence and year
            for period in periods:
                items += odata_items(f"{c.CBS_ODATA}/{table}/UntypedDataSet?$filter=TypeMisdrijf eq '{offence}' and Perioden eq '{period}'&$format=json".replace(" ", "%20"))
        parsed, unmapped = c.cbs_suspect_rows(items, titles)
        if unmapped:
            log.info("CBS nationalities without a country: %s", unmapped)
        land("nl_suspects", "CBS StatLine 85656NED : registrations de suspects selon l'infraction, le sexe, la nationalité et la résidence aux Pays-Bas",
             c.CBS_PAGE.format(table=table), c.CBS_SUSPECT_DIMS, parsed, extra={"unmapped": unmapped})

    run("nl_suspects", netherlands)

    def netherlands_population() -> None:
        pop_titles = odata_titles(c.CBS_POPULATION, "Nationaliteit")
        pop_items = odata_items(f"{c.CBS_ODATA}/{c.CBS_POPULATION}/UntypedDataSet?$filter=startswith(Leeftijd,'10000')&$format=json".replace(" ", "%20"))
        land("nl_population", "CBS StatLine 85510NED : population au 1er janvier selon la nationalité et le sexe",
             c.CBS_PAGE.format(table=c.CBS_POPULATION), c.CBS_POP_DIMS, c.cbs_population_rows(pop_items, pop_titles))

    run("nl_population", netherlands_population)

    # -- Denmark
    def denmark() -> None:
        post = lambda body: request(f"{c.DST_API}/data", headers={"Content-Type": "application/json"}, data=json.dumps(body).encode()).decode("utf-8-sig")  # noqa: E731
        info = as_json(f"{c.DST_API}/tableinfo?id={c.DST_GUILTY}&lang=en&format=JSON")
        origins = next(v for v in info["variables"] if v["id"] == "IELAND")["values"]
        years = [v["id"] for v in next(v for v in info["variables"] if v["id"] == "Tid")["values"]]
        guilty = post({"table": c.DST_GUILTY, "format": "CSV", "lang": "en", "variables": [
            {"code": "KOEN", "values": ["M", "K"]}, {"code": "ALDER", "values": ["TOT", "15-29", "30-49", "50-79"]},
            {"code": "IELAND", "values": ["*"]}, {"code": "Tid", "values": years}]})
        parsed, unmapped = c.dst_guilty_rows(guilty)
        land("dk_guilty", "Danmarks Statistik STRAFNA3 : personnes reconnues coupables selon le sexe, l'âge et le pays d'origine",
             c.DST_PAGE.format(table=c.DST_GUILTY), c.DST_DIMS, parsed, extra={"unmapped": unmapped})

    run("dk_guilty", denmark)

    def denmark_population() -> None:
        post = lambda body: request(f"{c.DST_API}/data", headers={"Content-Type": "application/json"}, data=json.dumps(body).encode()).decode("utf-8-sig")  # noqa: E731
        info = as_json(f"{c.DST_API}/tableinfo?id={c.DST_GUILTY}&lang=en&format=JSON")
        origins = next(v for v in info["variables"] if v["id"] == "IELAND")["values"]
        years = [v["id"] for v in next(v for v in info["variables"] if v["id"] == "Tid")["values"]]
        pop_info = as_json(f"{c.DST_API}/tableinfo?id={c.DST_POP}&lang=en&format=JSON")
        pop_origins = {v["id"] for v in next(v for v in pop_info["variables"] if v["id"] == "IELAND")["values"]}
        wanted = [o["id"] for o in origins if o["id"] in pop_origins]
        pop = ""
        for i in range(0, len(years), 4):  # a few years at a time: the API refuses very large extracts
            part = post({"table": c.DST_POP, "format": "CSV", "lang": "en", "variables": [
                {"code": "ALDER", "values": [str(a) for a in range(15, 80)]}, {"code": "KØN", "values": ["M", "K"]},
                {"code": "IELAND", "values": wanted}, {"code": "Tid", "values": years[i:i + 4]}]})
            pop += part if not pop else part.split("\n", 1)[1]
        land("dk_population", "Danmarks Statistik FOLK2 : population au 1er janvier, 15-79 ans, selon le sexe, l'âge et le pays d'origine",
             c.DST_PAGE.format(table=c.DST_POP), c.DST_DIMS, c.dst_population_rows(pop))

    run("dk_population", denmark_population)

    # -- Italy
    def italy() -> None:
        parsed: list[dict] = []
        for flow in (c.ISTAT_ALLEGED, c.ISTAT_ALLEGED.replace("AUTVITTPS_7", "AUTVITTPS_5")):
            key = "A.IT.OFFEND" + "." * 5
            parsed += c.istat_alleged_rows(text(f"{c.ISTAT_BASE}/data/IT1,{flow},1.0/{key}?startPeriod=2010", headers={"Accept": c.ISTAT_ACCEPT}))
        land("it_alleged", "ISTAT : auteurs présumés d'infractions dénoncées par les forces de l'ordre à l'autorité judiciaire, selon l'infraction, le sexe, l'âge et la citoyenneté",
             c.ISTAT_ALLEGED_PAGE, c.ISTAT_DIMS, parsed)

    run("it_alleged", italy)

    # -- Spain
    def spain() -> None:
        convicted = as_json(f"{c.INE_TEMPUS}/DATOS_TABLA/{c.INE_CONVICTED}?nult=40")
        land("es_convicted", "INE, Estadística de condenados adultos (25645) : personnes condamnées selon le sexe, l'âge et la nationalité",
             c.INE_PAGE.format(table=c.INE_CONVICTED), c.INE_CONVICTED_DIMS, c.ine_convicted_rows(convicted))
        violence = (c.ine_violence_rows(as_json(f"{c.INE_TEMPUS}/DATOS_TABLA/{c.INE_VGD_DOMESTIC}?nult=40"), "domestic")
                    + c.ine_violence_rows(as_json(f"{c.INE_TEMPUS}/DATOS_TABLA/{c.INE_VGD_GENDER}?nult=40"), "gender"))
        land("es_violence", "INE, Estadística de violencia doméstica et de violencia de genre (28402, 28305) : condamnés selon le lieu de naissance",
             c.INE_PAGE.format(table=c.INE_VGD_DOMESTIC), c.INE_VIOLENCE_DIMS, violence)

    run("es_convicted", spain)

    # -- France
    def france() -> None:
        resource = c.ssmsi_mec_resource(as_json(c.SSMSI_MEC_DATASET))
        land("fr_mec", "SSMSI (data.gouv.fr) : mis en cause pour des infractions élucidées selon le sexe, l'âge et la nationalité française ou étrangère, 2016-2025",
             c.SSMSI_MEC_PAGE, c.SSMSI_DIMS, c.ssmsi_mec_rows(request(resource["url"])), updated=resource.get("last_modified"), extra={"file": resource["url"]})

    run("fr_mec", france)

    # -- Eurostat: the denominators
    def population() -> None:
        for name, url in c.eurostat_pop_urls().items():
            payload = json.loads(request(url))
            if "id" not in payload:
                raise RuntimeError(f"Eurostat did not return a dataset: {str(payload)[:200]}")
            what = "migr_pop3ctb : population au 1er janvier selon le pays de naissance" if name == "birth" else "migr_pop1ctz : population au 1er janvier selon la citoyenneté, l'âge et le sexe"
            land(f"pop_{name}", f"Eurostat, {what}", c.EUROSTAT_VIEW.replace("migr_pop1ctz", "migr_pop3ctb") if name == "birth" else c.EUROSTAT_VIEW,
                 c.EUROSTAT_POP_DIMS, c.eurostat_pop_rows(payload), updated=payload.get("updated"))

    run("pop", population)

    # -- France, the SSMSI's annual bilan and its Info rapide on domestic violence: manual inputs in the lake, the bilan PDF by a plain request once a year
    from homelab_datalake.assets.ssmsi_bilan import collect_ssmsi_bilan

    more_rows, more_blocks, more_failed = collect_ssmsi_bilan(fetched_at, log, only=only)
    rows += more_rows
    blocks.update(more_blocks)
    failed += more_failed
    return rows, blocks, failed
