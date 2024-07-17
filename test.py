ret = []
with open("run.sh", 'r', encoding='utf-8') as f:
    lines = f.readlines()

    for line in lines:
        if "echo" in line:
            line = line.split("\"")
            line = "".join(line)
            line = line.split("echo ")
            line = "".join(line)
        ret.append(line)

with open("output.sh", 'a+', encoding='utf-8') as f:
    for line in ret:
        f.write(line)
