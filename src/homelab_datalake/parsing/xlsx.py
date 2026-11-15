"""Minimal xlsx reader on the standard library: openpyxl refuses some of the COR's workbooks (an invalid defined name).

read(bytes) returns {sheet title: [(row number, [cell, ...]), ...]}; numbers are floats, text is str, empty cells None.
"""
import re
import io
import zipfile
import xml.etree.ElementTree as ET

NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main', 'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'}


def col_index(ref):
    letters = re.match(r'[A-Z]+', ref).group()
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n - 1


def read(data: bytes):
    z = zipfile.ZipFile(io.BytesIO(data))
    shared = []
    if 'xl/sharedStrings.xml' in z.namelist():
        root = ET.fromstring(z.read('xl/sharedStrings.xml'))
        for si in root.findall('m:si', NS):
            shared.append(''.join(t.text or '' for t in si.iter('{%s}t' % NS['m'])))
    wb = ET.fromstring(z.read('xl/workbook.xml'))
    rels = ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
    target = {r.get('Id'): r.get('Target') for r in rels}
    out = {}
    for s in wb.find('m:sheets', NS):
        rid = s.get('{%s}id' % NS['r'])
        t = target[rid]
        t = t.lstrip('/')
        t = t if t.startswith('xl/') else 'xl/' + t
        root = ET.fromstring(z.read(t))
        rows = []
        for row in root.iter('{%s}row' % NS['m']):
            cells = {}
            for c in row.findall('m:c', NS):
                v = c.find('m:v', NS)
                if v is None:
                    isel = c.find('m:is', NS)
                    val = ''.join(x.text or '' for x in isel.iter('{%s}t' % NS['m'])) if isel is not None else None
                else:
                    ty = c.get('t')
                    val = shared[int(v.text)] if ty == 's' else (v.text if ty in ('str', 'e', 'b') else float(v.text))
                cells[col_index(c.get('r'))] = val
            if cells:
                r = [None] * (max(cells) + 1)
                for k, x in cells.items():
                    r[k] = x
                rows.append((int(row.get('r')), r))
        out[s.get('name')] = rows
    return out
