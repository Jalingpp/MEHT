# ret = []
# with open("run.sh", 'r', encoding='utf-8') as f:
#     lines = f.readlines()
#
#     for line in lines:
#         if "echo" in line:
#             line = line.split("\"")
#             line = "".join(line)
#             line = line.split("echo ")
#             line = "".join(line)
#         ret.append(line)
#
# with open("output.sh", 'a+', encoding='utf-8') as f:
#     for line in ret:
#         f.write(line)
a = "3078343833353563653662613337376430363333356265393439396665616637333539343834383462632f37313933"
print(len(a), a[51])