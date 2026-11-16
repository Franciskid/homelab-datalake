#!/usr/bin/env bash
# Puts the SSMSI files the owner downloaded into the lake as manual inputs (sha256 checked, provenance recorded).
#
#   on the Mac:     put-ssmsi-inputs.sh ~/dev/politico-sources/ssmsi <command prefix>
#   on fraserv:     scp the folder, then (the container cannot see the host's files):
#                   docker cp ssmsi datalake-daemon:/tmp/ssmsi
#                   bash scripts/put-ssmsi-inputs.sh /tmp/ssmsi docker exec -e DAGSTER_HOME=/dagster_home datalake-daemon python -m homelab_datalake.manual_inputs
#   (run it inside the container instead if bash is there: docker exec datalake-daemon bash /app/scripts/put-ssmsi-inputs.sh /tmp/ssmsi python -m homelab_datalake.manual_inputs)
#
# The zip of the bilan's data tables is renewed by hand each July, the Info rapide on domestic violence each November.
set -euo pipefail
S="$1"; shift
PUT=("$@" put --source ssmsi)
"${PUT[@]}" --role bilan_data_zip --edition 2026 --file "$S/SSMSI-bilan-2025-donnees.zip" --sha256 22d8e61688a4790de8b81b95b0dde730b79f0eff4c818af8e4df5155c57b8528 --url "https://statistiques.interieur.gouv.fr/ssmsi/files/ssmsi/2026-07/Ins%C3%A9curit%C3%A9%20et%20d%C3%A9linquance%20donn%C3%A9es%20-%20%C3%A9dition%20juillet%202026.zip" --how "manual: downloaded in a browser (the host answers 403 to scripts)" --obtained-on 2026-11-16 --note "Renew by hand each July."
"${PUT[@]}" --role bilan_pdf --edition 2026 --file "$S/SSMSI-bilan-2025-edition-2026.pdf" --sha256 5395fb4adb40173331b1b35b3f5e10e93f6677755a4aa60dd9631b7262cf7eeb --url "https://statistiques.interieur.gouv.fr/ssmsi/files/ssmsi/2026-07/Ins%C3%A9curit%C3%A9%20et%20d%C3%A9linquance%20en%202025%20-%20Bilan%20statistique%20-%20%C3%A9dition%202026%20-%20SSMSI.pdf" --how "fetched: plain request, official files host" --obtained-on 2026-11-16
"${PUT[@]}" --role ir_domestic_pdf --edition 2024 --file "$S/IR56-violences-conjugales-2024.pdf" --sha256 0d5caeecdd7dcb190473b354ba818b77cfaacc71b133a3ab3f13cee60e807419 --url "https://mobile.interieur.gouv.fr/content/download/138737/1095839/file/IR%2056%20-%20Violences%20conjugales%20-%20V2-3.pdf" --how "manual: copy of the PDF on the Paris bar's site (avocatparis.org); the official address answers 503 to scripts" --obtained-on 2026-11-16 --note "Interstats Info rapide n°56, Oct 2025"
"${PUT[@]}" --role ir_domestic_pdf --edition 2025 --file "$S/IR69-violences-conjugales-2025.pdf" --sha256 a3c637f6bbaeb0ae28abb8fd08a5d717fcdfaf03e4624126e092137e6c2e7d5f --url "https://statistiques.interieur.gouv.fr/ssmsi/files/ssmsi/2026-11/IR69%20Violences%20conjugales.pdf" --how "manual: plain request on the official files host, address found in a browser" --obtained-on 2026-11-16 --note "Interstats Info rapide n°69, 5 Nov 2026"
"${PUT[@]}" --role ir_domestic_xlsx --edition 2025 --file "$S/IR69-donnees-violences-conjugales-2025.xlsx" --sha256 f72d8765110d764f5a4f6b7dfcd38c4a5a06f3aaba0c7444ffc5c80a26831049 --url "https://statistiques.interieur.gouv.fr/ssmsi/files/ssmsi/2026-11/IR%2069%20Donn%C3%A9es%20violences%20conjugales%20en%202025_0.xlsx" --how "manual: plain request on the official files host, address found in a browser" --obtained-on 2026-11-16 --note "Data tables of Info rapide n°69"
