"""Values read by hand-run extraction (pdftotext -layout) from the annual performance projects (PAP) annexed to the
finance bills, table "Présentation des crédits par catégorie et contributions employeurs" of each programme's
"Justification au premier euro". One tuple per programme and budget year:
(edition, mission, programme, year, cotisations et contributions sociales (€), contributions d'équilibre au CAS Pensions (€),
 of which civils incl. ATI (€), of which militaires (€), page of the PDF, url).

LFI 2025 and PLF 2026 come from the PLF 2026 annexes (PAP2026), LFI 2026 and PLF 2027 from the PLF 2027 annexes (PAP2027).
Missions not listed (Agriculture, Travail, Outre-mer, Anciens combattants...) were not extracted; together they are about
2 % of the State's social contributions. Access date of every file: 2026-11-15.
"""

PAP_CAS_PENSIONS = (
    ('PAP2026', 'Action extérieure de l’État', '105', '2025', 276273104, 192853767, 162182301, 30671466, 42, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087939/file/x.pdf'),
    ('PAP2026', 'Action extérieure de l’État', '105', '2026', 291054689, 204376039, 171675873, 32700166, 42, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087939/file/x.pdf'),
    ('PAP2026', 'Administration générale et territoriale de l’État', '354', '2025', 772603357, 574937796, 572697663, 2240133, 46, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087942/file/x.pdf'),
    ('PAP2026', 'Administration générale et territoriale de l’État', '354', '2026', 809536863, 601400621, 599113367, 2287254, 46, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087942/file/x.pdf'),
    ('PAP2026', 'Administration générale et territoriale de l’État', '232', '2025', 1475130, 420000, 420000, None, 81, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087942/file/x.pdf'),
    ('PAP2026', 'Administration générale et territoriale de l’État', '232', '2026', 2076117, 552600, 552600, None, 81, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087942/file/x.pdf'),
    ('PAP2026', 'Administration générale et territoriale de l’État', '216', '2025', 349282585, 262143627, 209921915, 9023786, 107, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087942/file/x.pdf'),
    ('PAP2026', 'Administration générale et territoriale de l’État', '216', '2026', 359510242, 267878756, 214583240, 8426059, 107, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087942/file/x.pdf'),
    ('PAP2026', 'Culture', '224', '2025', 276537096, 194137092, 194137092, None, 264, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087963/file/x.pdf'),
    ('PAP2026', 'Culture', '224', '2026', 287350130, 205104353, 205104353, None, 264, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087963/file/x.pdf'),
    ('PAP2026', 'Écologie, développement et mobilité durables', '217', '2025', 1186518759, 945961711, 743758172, 25753857, 402, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087969/file/x.pdf'),
    ('PAP2026', 'Écologie, développement et mobilité durables', '217', '2026', 1184097103, 939643990, 734685499, 25907113, 402, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087969/file/x.pdf'),
    ('PAP2026', 'Économie', '134', '2025', 141859945, 98274690, 98274690, None, 49, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087972/file/x.pdf'),
    ('PAP2026', 'Économie', '134', '2026', 155236241, 112069873, 112069873, None, 49, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087972/file/x.pdf'),
    ('PAP2026', 'Économie', '220', '2025', 146900982, 110816017, 110816017, None, 106, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087972/file/x.pdf'),
    ('PAP2026', 'Économie', '220', '2026', 152733877, 116277665, 116277665, None, 106, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087972/file/x.pdf'),
    ('PAP2026', 'Économie', '305', '2025', 35253123, 21935143, 21635896, 299247, 134, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087972/file/x.pdf'),
    ('PAP2026', 'Économie', '305', '2026', 35095761, 21147140, 20427992, 719148, 134, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087972/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '140', '2025', 12147831421, 9849231566, 9849231566, None, 49, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '140', '2026', 12532384396, 10211500624, 10211500624, None, 49, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '141', '2025', 16403643339, 12929252989, 12929252989, None, 102, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '141', '2026', 16880920288, 13336950442, 13336950442, None, 102, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '230', '2025', 1725438981, 670712911, 670712911, None, 160, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '230', '2026', 1786607042, 683496007, 683496007, None, 160, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '139', '2025', 2166166151, 51640192, 51640192, None, 212, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '139', '2026', 2116788682, 72599556, 72599556, None, 212, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '214', '2025', 777427618, 574218637, 574218637, None, 267, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '214', '2026', 809555567, 603523287, 603523287, None, 267, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '143', '2025', 415391612, 249415353, 249415353, None, 335, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Enseignement scolaire', '143', '2026', 410369299, 251401047, 251401047, None, 335, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087978/file/x.pdf'),
    ('PAP2026', 'Gestion des finances publiques', '156', '2025', 2708609666, 2155058393, 2152751607, 491978, 49, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087984/file/x.pdf'),
    ('PAP2026', 'Gestion des finances publiques', '156', '2026', 2781313114, 2231387618, 2228731314, 750000, 49, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087984/file/x.pdf'),
    ('PAP2026', 'Gestion des finances publiques', '218', '2025', 198680666, 144116392, 100775799, 1132823, 110, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087984/file/x.pdf'),
    ('PAP2026', 'Gestion des finances publiques', '218', '2026', 204718405, 148516819, 105157799, 1179734, 110, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087984/file/x.pdf'),
    ('PAP2026', 'Gestion des finances publiques', '302', '2025', 515490949, 423435821, 421232106, 694369, 155, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087984/file/x.pdf'),
    ('PAP2026', 'Gestion des finances publiques', '302', '2026', 534278141, 440636600, 438451266, 630678, 155, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1087984/file/x.pdf'),
    ('PAP2026', 'Justice', '166', '2025', 1164219302, 835465215, 834754048, 711167, 55, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '166', '2026', 1229439460, 930417617, 929681157, 736460, 55, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '107', '2025', 1381471469, 1145534751, 1145231940, 302811, 102, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '107', '2026', 1488763541, 1245549910, 1245256066, 293844, 102, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '182', '2025', 259243022, 189506535, 189506535, None, 154, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '182', '2026', 270891012, 199456980, 199456980, None, 154, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '310', '2025', 79849426, 46066651, 44270060, 1796591, 216, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '310', '2026', 85254842, 49343530, 48813604, 529926, 216, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '335', '2025', 880282, 700000, 700000, None, 277, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Justice', '335', '2026', 946627, 751194, 751194, None, 277, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088005/file/x.pdf'),
    ('PAP2026', 'Sécurités', '176', '2025', 4730039004, 3856216540, 3848429158, 7787382, 54, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088035/file/x.pdf'),
    ('PAP2026', 'Sécurités', '176', '2026', 5037415456, 4137738433, 4129530171, 8208262, 54, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088035/file/x.pdf'),
    ('PAP2026', 'Sécurités', '152', '2025', 4458127615, 3999423319, 94876546, 3904546773, 118, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088035/file/x.pdf'),
    ('PAP2026', 'Sécurités', '152', '2026', 4511689392, 4041155439, 100311406, 3940844033, 118, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088035/file/x.pdf'),
    ('PAP2026', 'Sécurités', '161', '2025', 95569650, 72369657, 15317161, 57052496, 177, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088035/file/x.pdf'),
    ('PAP2026', 'Sécurités', '161', '2026', 97960546, 74900540, 15582047, 59318493, 177, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1088035/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '140', '2026', 12532384396, 10211500624, 10211500624, None, 51, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '140', '2027', 12911983978, 10360718366, 10360718366, None, 51, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '141', '2026', 16880920288, 13336950442, 13336950442, None, 101, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '141', '2027', 17174329097, 13346586339, 13346586339, None, 101, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '230', '2026', 1786607042, 683496007, 683496007, None, 137, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '230', '2027', 1884600811, 699328067, 699328067, None, 137, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '139', '2026', 2116788682, 72599556, 72599556, None, 190, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '139', '2027', 2181226367, 52448340, 52448340, None, 190, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '214', '2026', 809555567, 603523287, 603523287, None, 243, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '214', '2027', 842753499, 614813653, 614813653, None, 243, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '143', '2026', 410369299, 251401047, 251401047, None, 312, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Enseignement scolaire', '143', '2027', 425841471, 253858739, 253858739, None, 312, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128234/file/x.pdf'),
    ('PAP2027', 'Sécurités', '176', '2026', 5037415456, 4137738433, 4129683650, 8054783, 58, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128275/file/x.pdf'),
    ('PAP2027', 'Sécurités', '176', '2027', 5127842818, 4221451326, 4213483894, 7967432, 58, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128275/file/x.pdf'),
    ('PAP2027', 'Sécurités', '152', '2026', 4510308574, 4041155439, 100312223, 3940843216, 122, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128275/file/x.pdf'),
    ('PAP2027', 'Sécurités', '152', '2027', 4637329514, 4138158921, 98625225, 4039533696, 122, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128275/file/x.pdf'),
    ('PAP2027', 'Sécurités', '161', '2026', 97960546, 74900540, 17063331, 57837209, 182, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128275/file/x.pdf'),
    ('PAP2027', 'Sécurités', '161', '2027', 113258716, 79830048, 15469571, 64360477, 182, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128275/file/x.pdf'),
    ('PAP2027', 'Justice', '166', '2026', 1229439460, 930417617, 930417617, None, 59, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '166', '2027', 1262606809, 947136082, 947136082, None, 59, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '107', '2026', 1488763541, 1245549910, 1245256066, 293844, 109, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '107', '2027', 1514646152, 1263919046, 1263689761, 229285, 109, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '182', '2026', 270891012, 199456980, 199456980, 52325, 159, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '182', '2027', 277555693, 203139534, 203087209, None, 159, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '310', '2026', 85254842, 49343530, 48813604, 529926, 223, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '310', '2027', 92216649, 52733115, 52438139, 294976, 223, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '335', '2026', 946627, 751194, 751194, None, 290, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Justice', '335', '2027', 985647, 741073, 741073, None, 290, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128243/file/x.pdf'),
    ('PAP2027', 'Défense', '212', '2026', 11345804471, 9576217747, 852178932, 7301404086, 242, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128221/file/x.pdf'),
    ('PAP2027', 'Défense', '212', '2027', 11524724802, 9744219699, 921708516, 7431055197, 242, 'https://www.assemblee-nationale.fr/dyn/contenu/visualisation/1128221/file/x.pdf'),
)

# Employers' pension contributions of the CNRACL (civil servants of local authorities and hospitals) and of the
# Ircantec (public contract staff), in million euros: Les comptes de la Sécurité sociale (Commission des comptes de la
# Sécurité sociale), table 2 of the old-age fiche of each scheme ("Cotisations patronales"). 2024 is the report's forecast.
# (scheme, year, M€, report, page, url)
CCSS_COTISATIONS_PATRONALES = (
    ("CNRACL", "2021", 16219, "CCSS mai 2023 (résultats 2022)", 179, "https://www.vie-publique.fr/files/rapport/pdf/299163.pdf"),
    ("CNRACL", "2022", 16851, "CCSS octobre 2024 (résultats 2023)", 188, "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf"),
    ("CNRACL", "2023", 17484, "CCSS octobre 2024 (résultats 2023)", 188, "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf"),
    ("CNRACL", "2024", 18401, "CCSS octobre 2024 (prévision)", 188, "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf"),
    ("IRCANTEC", "2022", 2642, "CCSS octobre 2024 (résultats 2023)", 252, "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf"),
    ("IRCANTEC", "2023", 2785, "CCSS octobre 2024 (résultats 2023)", 252, "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf"),
    ("IRCANTEC", "2024", 2937, "CCSS octobre 2024 (prévision)", 252, "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf"),
)

# CNRACL employer contribution rate (% of the pensionable pay), from the IGAS/IGF report on the CNRACL's finances (2024):
# 27.4 % in 2012, 30.65 % in 2023, +1 point on 1 January 2024. Years in between are not in the report.
# (year, rate, page, url)
CNRACL_TAUX_EMPLOYEUR = (
    ("2012", 27.40, "IGAS/IGF, Situation financière de la CNRACL (2024)", "https://www.vie-publique.fr/files/rapport/pdf/295580.pdf"),
    ("2023", 30.65, "IGAS/IGF, Situation financière de la CNRACL (2024)", "https://www.vie-publique.fr/files/rapport/pdf/295580.pdf"),
    ("2024", 31.65, "CCSS octobre 2024, fiche 4.11", "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf"),
)

# Contributors and beneficiaries (old-age) of two schemes, annual average, CCSS octobre 2024, table 1 of the scheme's fiche.
# (scheme, kind, year, number, page)
CCSS_EFFECTIFS = (
    ("FPE", "cotisants", "2022", 2033691, 186), ("FPE", "cotisants", "2023", 2028245, 186), ("FPE", "cotisants", "2024", 2021679, 186),
    ("FPE", "beneficiaires", "2022", 2244346, 186), ("FPE", "beneficiaires", "2023", 2260477, 186), ("FPE", "beneficiaires", "2024", 2269437, 186),
    ("CNRACL", "cotisants", "2022", 2140201, 188), ("CNRACL", "cotisants", "2023", 2144492, 188), ("CNRACL", "cotisants", "2024", 2145992, 188),
    ("CNRACL", "beneficiaires", "2022", 1276473, 188), ("CNRACL", "beneficiaires", "2023", 1323278, 188), ("CNRACL", "beneficiaires", "2024", 1358974, 188),
)
