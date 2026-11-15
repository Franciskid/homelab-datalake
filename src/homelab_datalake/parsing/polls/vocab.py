# Vendored from the politico repository (scripts/pollslib/vocab.py, ee9ed1d). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Common vocabulary for the categories of a poll's cross-tabulations.

Every institute words its breakdown categories its own way ("18-24 ans", "18 à 24 ans", "Moins de 25 ans"; "Ouvriers",
"Ouvrier"; "Région parisienne", "Ile-de-France"). `categorize(section, label)` maps a printed label to
(dimension, category id, parent id) of this vocabulary. The institute's label is always kept next to it in the
data, and a label the vocabulary does not know comes back as dimension "autre" so a reviewer sees it.

Dimensions: sexe, age, csp, statut, agglomeration, region, diplome, revenu, religion, proximite (party
closeness), vote_2022_t1, vote_2022_t2, vote_leg_2024, vote_eu_2024, autre.
"""
import re
import unicodedata

DIMENSIONS = {
    "sexe": "Sexe", "age": "Âge", "csp": "Catégorie socioprofessionnelle", "statut": "Statut d'activité",
    "agglomeration": "Catégorie d'agglomération", "region": "Région", "diplome": "Diplôme", "revenu": "Revenu du foyer",
    "religion": "Religion", "proximite": "Proximité partisane", "vote_2022_t1": "Vote présidentielle 2022 (1er tour)",
    "vote_2022_t2": "Vote présidentielle 2022 (2nd tour)", "vote_leg_2024": "Vote législatives 2024 (1er tour)",
    "vote_eu_2024": "Vote européennes 2024", "autre": "Autre",
}


def norm(s: str) -> str:
    s = s.replace("&nbsp;", " ").replace("’", "'").lower().replace("œ", "oe")
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9+<>'%()/ -]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# (dimension, id, parent, [regex on the normalised label]). First match wins; order matters inside a dimension.
_C = [
    ("age", "50-59", None, [r"^50 ?(?:-|a) ?59( ans)?$"]),
    ("age", "60-69", None, [r"^60 ?(?:-|a) ?69( ans)?$"]),
    ("age", "70+", None, [r"^70 ans et (?:plus|\+)$", r"^70 (?:ans )?(?:et )?plus$"]),
    ("age", "65-74", None, [r"^65 ?(?:-|a) ?74( ans)?$"]),
    ("age", "75+", None, [r"^75 ans et (?:plus|\+)$", r"^75 (?:ans )?(?:et )?plus$"]),
    ("csp", "agriculteurs-independants", "actifs", [r"^agriculteurs? et prof", r"^agriculteurs?"]),
    ("csp", "salaries", "actifs", [r"^salaries?$"]),
    ("csp", "salaries-prive", "salaries", [r"^(?:\.\.\.|…)? ?(?:du )?secteur prive$", r"^salaries? du secteur prive"]),
    ("csp", "salaries-public", "salaries", [r"^(?:\.\.\.|…)? ?(?:du )?secteur public$", r"^salaries? du secteur public"]),
    ("statut", "retraites-csp-plus", "retraites", [r"^retraites csp ?\+$"]),
    ("statut", "retraites-csp-moins", "retraites", [r"^retraites csp ?-$"]),
    ("statut", "retraites", "inactifs", [r"^sous-total retraites$"]),
    ("statut", "demandeurs-emploi", "actifs", [r"^demandeurs? d'emploi$", r"^chomeurs?$"]),
    ("statut", "etudiants", "inactifs", [r"^eleves, etudiants$"]),
    ("agglomeration", "rurale", None, [r"^communes? rurales?$"]),
    ("agglomeration", "petite", None, [r"^petites? villes?$"]),
    ("agglomeration", "moyenne", None, [r"^villes? moyennes?$"]),
    ("agglomeration", "grande", None, [r"^metropoles?$"]),
    ("agglomeration", "paris", "grande", [r"^agglomeration parisienne$"]),
    ("region", "auvergne-rhone-alpes", "province", [r"^auvergne-rhone-alpes$"]),
    ("region", "bourgogne-franche-comte", "province", [r"^bourgogne-franche-comte$"]),
    ("region", "bretagne", "province", [r"^bretagne$"]),
    ("region", "centre-val-de-loire", "province", [r"^centre-val de loire$"]),
    ("region", "grand-est", "province", [r"^grand est$"]),
    ("region", "hauts-de-france", "province", [r"^hauts-de-france$"]),
    ("region", "normandie", "province", [r"^normandie$"]),
    ("region", "nouvelle-aquitaine", "province", [r"^nouvelle-aquitaine$"]),
    ("region", "occitanie", "province", [r"^occitanie$"]),
    ("region", "pays-de-la-loire", "province", [r"^pays de la loire$"]),
    ("region", "paca", "province", [r"^provence-alpes-cote d'azur$", r"^paca$"]),
    ("proximite", "gauche", None, [r"^st gauche$"]),
    ("proximite", "centre", None, [r"^st centre$"]),
    ("proximite", "droite", None, [r"^st droite$"]),
    ("proximite", "pcf", "gauche", [r"^le parti communiste"]),
    ("proximite", "ps", "gauche", [r"^le parti socialiste"]),
    ("proximite", "modem", "centre", [r"^le modem$"]),
    ("proximite", "rn", "rn-souverainistes", [r"^le rassemblement national$"]),
    ("proximite", "majorite-presidentielle", "centre", [r"^majorite presidentielle$"]),
    ("sexe", "homme", None, [r"^hommes?$", r"^masculin$"]),
    ("sexe", "femme", None, [r"^femmes?$", r"^feminin$"]),
    ("age", "18-24", None, [r"^18 ?(?:-|a|à) ?24( ans)?$", r"^moins de 25 ans$", r"^18-24"]),
    ("age", "25-34", None, [r"^25 ?(?:-|a) ?34( ans)?$"]),
    ("age", "35-49", None, [r"^35 ?(?:-|a) ?49( ans)?$"]),
    ("age", "50-64", None, [r"^50 ?(?:-|a) ?64( ans)?$"]),
    ("age", "65+", None, [r"^65 (?:ans )?(?:et )?plus( ans)?$", r"^65 ?\+", r"^65 ans et plus$", r"^65 et plus$"]),
    ("age", "18-34", None, [r"^18 ?(?:-|a) ?34( ans)?$", r"^moins de 35 ans$"]),
    ("age", "35-64", None, [r"^35 ?(?:-|a) ?64( ans)?$"]),
    ("age", "50+", None, [r"^50 ans et plus$", r"^50 (?:ans )?(?:et )?plus$"]),
    ("statut", "actifs", None, [r"^actifs?$", r"^total actifs?$"]),
    ("statut", "inactifs", None, [r"^inactifs?$", r"^total inactifs?$"]),
    ("statut", "retraites", "inactifs", [r"^retraites?$"]),
    ("statut", "autres-inactifs", "inactifs", [r"^autres? inactifs?$"]),
    ("statut", "etudiants", "inactifs", [r"^etudiants?$"]),
    ("csp", "independants", "actifs", [r"^independants?$", r"^artisans?,? commercants?"]),
    ("csp", "csp-plus", "actifs", [r"^csp ?\+$", r"^csp plus$"]),
    ("csp", "cadres", "csp-plus", [r"^cadres?$", r"^cadres? et professions? intellectuelles?", r"^cadres? sup"]),
    ("csp", "prof-interm", "csp-plus", [r"^prof(?:\.|essions?)? interm"]),
    ("csp", "csp-moins", "actifs", [r"^csp ?-$", r"^csp moins$"]),
    ("csp", "employes", "csp-moins", [r"^employes?$"]),
    ("csp", "ouvriers", "csp-moins", [r"^ouvriers?$"]),
    ("agglomeration", "rurale", None, [r"^communes? rurales?$", r"^rural$"]),
    ("agglomeration", "petite", None, [r"^petites? agglos?", r"^(?:2|3|5)\d? ?000 a (?:19|20)\d? ?999"]),
    ("agglomeration", "moyenne", None, [r"^moyennes? agglos?"]),
    ("agglomeration", "grande", None, [r"^grandes? agglos?$", r"^grandes? agglomerations?$"]),
    ("agglomeration", "grande-province", "grande", [r"^grandes? agglos? de province$"]),
    ("agglomeration", "paris", "grande", [r"^agglo(?:meration)? parisienne$", r"^agglomeration de paris$"]),
    ("region", "ile-de-france", None, [r"^region parisienne$", r"^ile-de-france$", r"^ile de france$"]),
    ("region", "province", None, [r"^province$"]),
    ("region", "nord-est", "province", [r"^nord-est$", r"^nord est$"]),
    ("region", "nord-ouest", "province", [r"^nord-ouest$", r"^nord ouest$"]),
    ("region", "sud-est", "province", [r"^sud-est$", r"^sud est$"]),
    ("region", "sud-ouest", "province", [r"^sud-ouest$", r"^sud ouest$"]),
    ("region", "bassin-parisien", "province", [r"^bassin parisien"]),
    ("region", "centre-est", "province", [r"^centre-est$", r"^centre est$"]),
    ("region", "ouest", "province", [r"^ouest$"]),
    ("region", "nord", "province", [r"^nord$"]),
    ("region", "est", "province", [r"^est$"]),
    ("region", "sud", "province", [r"^sud$"]),
    ("diplome", "sans-diplome", None, [r"^sans diplome", r"^aucun diplome", r"^inferieur au bac"]),
    ("diplome", "bac", None, [r"^bac(?:calaureat)?$", r"^niveau bac"]),
    ("diplome", "superieur", None, [r"^bac ?\+", r"^diplome du superieur", r"^superieur$", r"^etudes superieures"]),
    ("religion", "catholique", None, [r"^catholiques?"]),
    ("religion", "musulman", None, [r"^musulmans?"]),
    ("religion", "sans-religion", None, [r"^sans religion", r"^aucune religion"]),
    ("proximite", "gauche", None, [r"^total gauche$", r"^gauche$"]),
    ("proximite", "lfi", "gauche", [r"^la france insoumise", r"^lfi$"]),
    ("proximite", "ps", "gauche", [r"^parti socialiste", r"^ps$"]),
    ("proximite", "ecologistes", "gauche", [r"^les ecologistes", r"^eelv", r"^europe ecologie"]),
    ("proximite", "pcf", "gauche", [r"^parti communiste", r"^pcf$"]),
    ("proximite", "centre", None, [r"^total renaissance", r"^total centre", r"^centre$"]),
    ("proximite", "renaissance", "centre", [r"^renaissance$", r"^ensemble$"]),
    ("proximite", "modem", "centre", [r"^modem$", r"^mouvement democrate"]),
    ("proximite", "horizons", "centre", [r"^horizons$"]),
    ("proximite", "droite", None, [r"^total droite$", r"^droite$"]),
    ("proximite", "lr", "droite", [r"^les republicains", r"^lr$"]),
    ("proximite", "rn-souverainistes", None, [r"^total rn", r"^rn et allies$"]),
    ("proximite", "rn", "rn-souverainistes", [r"^rassemblement national \(rn\)$", r"^rassemblement national$", r"^rn$"]),
    ("proximite", "reconquete", "rn-souverainistes", [r"^reconquete"]),
    ("proximite", "sans-preference", None, [r"^sans preference", r"^aucun parti", r"^ni gauche ni droite"]),
    ("vote_2022_t1", "melenchon", None, [r"^jean-luc melenchon$", r"^melenchon$"]),
    ("vote_2022_t1", "jadot", None, [r"^yannick jadot$", r"^jadot$"]),
    ("vote_2022_t1", "macron", None, [r"^emmanuel macron$", r"^macron$"]),
    ("vote_2022_t1", "pecresse", None, [r"^valerie pecresse$", r"^pecresse$"]),
    ("vote_2022_t1", "le-pen", None, [r"^marine le pen$", r"^le pen$"]),
    ("vote_2022_t1", "zemmour", None, [r"^eric zemmour$", r"^zemmour$"]),
    ("vote_2022_t1", "abstention", None, [r"^abstention"]),
    ("vote_2022_t2", "macron", None, [r"^emmanuel macron$", r"^macron$"]),
    ("vote_2022_t2", "le-pen", None, [r"^marine le pen$", r"^le pen$"]),
    ("vote_2022_t2", "abstention", None, [r"^abstention", r"^blanc", r"^nul"]),
    ("vote_leg_2024", "nfp", None, [r"^nfp", r"^nouveau front populaire"]),
    ("vote_leg_2024", "ensemble", None, [r"^ensemble ?\(", r"^ensemble$", r"^renaissance"]),
    ("vote_leg_2024", "lr-dvd", None, [r"^lr,? ?dvd", r"^les republicains"]),
    ("vote_leg_2024", "rn", None, [r"^rassemblement national et ses allies", r"^rn et allies", r"^rassemblement national"]),
    ("vote_leg_2024", "abstention", None, [r"^abstention"]),
]

# Section titles (normalised, prefix) -> dimension. A row's section decides which vote-year vocabulary applies.
SECTIONS = [
    (r"^sexe", "sexe"), (r"^age", "age"), (r"^categorie socio|^csp", "csp"), (r"^statut|^situation", "statut"),
    (r"^categorie d'agglomeration|^agglomeration|^taille d'agglo", "agglomeration"), (r"^region|^zone", "region"),
    (r"^diplome|^niveau d'etude", "diplome"), (r"^revenu|^tranche de revenu", "revenu"), (r"^religion", "religion"),
    (r"^preference partisane|^proximite|^sympathie|^proche d|^par proximite", "proximite"),
    (r"^pcs|^activite|^par activite|^profession", "csp"), (r"^habitat|^par habitat", "agglomeration"),
    (r"^vote presidentielle 2022 ?\(1er tour\)|^vote .*2022.*1er tour|^presidentielle 2022 \(1er|^vote a la presidentielle|^election presidentielle 2022", "vote_2022_t1"),
    (r"^vote presidentielle 2022 ?\(2nd tour\)|^vote .*2022.*(?:2nd|2eme|second) tour", "vote_2022_t2"),
    (r"^vote legislatives 2024|^legislatives 2024", "vote_leg_2024"), (r"^vote europeennes 2024|^europeennes 2024", "vote_eu_2024"),
]
_SECTIONS = [(re.compile(p), d) for p, d in SECTIONS]
_INDEX = [(d, i, par, [re.compile(p) for p in pats]) for d, i, par, pats in _C]


def dimension_of(section: str | None) -> str | None:
    n = norm(section or "")
    for rx, dim in _SECTIONS:
        if rx.search(n):
            return dim
    return None


def categorize(section: str | None, label: str) -> dict:
    """Map a printed row label to the vocabulary. {dimension, category, parent, known}."""
    dim = dimension_of(section)
    n = norm(label)
    # The statut/csp mix of Elabe's CSP section: actifs, retraités and inactifs belong to "statut".
    order = [t for t in _INDEX if dim is None or t[0] == dim or (dim == "csp" and t[0] == "statut")]
    for d, i, par, rxs in order:
        if any(rx.search(n) for rx in rxs):
            return {"dimension": d, "category": i, "parent": par, "known": True}
    for d, i, par, rxs in _INDEX:   # a label under the wrong printed section (Odoxa prints age rows under "SEXE")
        if d in ("sexe", "age", "csp", "statut", "agglomeration", "region") and any(rx.search(n) for rx in rxs):
            return {"dimension": d, "category": i, "parent": par, "known": True}
    if dim and dim.startswith("vote_"):   # any answer of a vote-recall question is a party or a candidate
        return {"dimension": dim, "category": re.sub(r"[^a-z0-9]+", "-", n).strip("-")[:60], "parent": None, "known": True}
    slug = re.sub(r"[^a-z0-9]+", "-", n).strip("-")[:60] or "inconnu"
    return {"dimension": dim or "autre", "category": slug, "parent": None, "known": False}


# Display labels for age categories. Some reports print the label in an order the text layer scrambles
# ("65 plus ans"); the page shows the canonical wording and keeps the institute's text in label_original.
AGE_DISPLAY = {"18-24": "18-24 ans", "25-34": "25-34 ans", "35-49": "35-49 ans", "50-64": "50-64 ans",
               "65+": "65 ans et plus", "70+": "70 ans et plus", "75+": "75 ans et plus"}


def display_label(dimension: str, category: str, label: str) -> str:
    return AGE_DISPLAY.get(category, label) if dimension == "age" else label
