import json

f = open("dev_noised_min.jsonl")

for line in f:
    line = json.loads(line)
    print(line["question"])