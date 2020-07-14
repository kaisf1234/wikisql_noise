import copy
import json

with open("dev.jsonl") as f,  open("simplified_proc.tx") as g:
    lines = []
    for line, simplified in zip(f, g):
        line = json.loads(line)
        lines.append([line, simplified])
print("ok")

new_data = []
missed_conds = 0
full_skip = 0
for orig, simplified in lines:
    conds = orig["sql"]["conds"]
    new_conds = []
    for cond in conds:
        value = cond[2]
        if str(value).lower() in simplified.lower():
            new_conds.append(cond)
    if len(new_conds) > 0 and simplified != orig["question"]:
        orig_clone = copy.copy(orig)
        orig_clone["question"] = simplified.strip()
        orig_clone["sql"]["conds"] = new_conds

        if len(new_conds) < len(conds):
            missed_conds += 1
        new_data.append(orig_clone)
    else:
        full_skip += 1
print(missed_conds, full_skip)
f = open("simplified_dev.jsonl", "w")
for data in new_data:
    f.write(json.dumps(data) +"\n")
f.close()