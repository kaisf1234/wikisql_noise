import csv
import json

import re


def camel_case_split(str):
    words = [[str[0]]]

    for c in str[1:]:
        if words[-1][-1].islower() and c.isupper():
            words.append(list(c))
        else:
            words[-1].append(c)

    return [''.join(word) for word in words]

def do_title():
    num_rows = 10000000
    f = open("./title.basics.tsv")
    reader = csv.reader(f, delimiter="\t")
    rows = []
    for i, row in enumerate(reader):
        if i == 0:
            header = row
        elif i <= num_rows:
            rows.append(row)
    header = header[:3] + header[5:]
    header = [" ".join(camel_case_split(x)) for x in header]
    types = ["text"] * 2 + ["real"] * 3 + ["text"]
    rows = [x[:3] + x[5:] for x in rows]
    print(len(rows))
    f.close()
    return header, rows, types, "tile_basic", "1-999999-69"


header, rows, types, page_title, tid = do_title()
with open(page_title + "_parsed.json", "w") as g:
    val = {"header": header, "rows": rows, "types": types, "page_title": page_title, "id": tid, "section_title": "",
           "caption": "", "name": "table_" + (tid.replace("-", "_"))}
    json.dump(val, g)

"""
10K
Took :  0.37319397926330566
1325.412352

1M
Took :  1.6772091388702393
4076.863488

10M
Took :  105.92677998542786
18243.751936
"""