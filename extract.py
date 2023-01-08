
with open("../datasets/data.json", encoding="utf-8") as file:
    for i in range(0, 5):
        line = file.readline()
        open(f"sample-{i}.json", "w+", encoding="utf-8").write(line)