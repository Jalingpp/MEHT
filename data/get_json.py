import json
import os

write_path_prefix = "output/"
input_path = "input/"

def convert_to_json(file_path):
    with open(file_path, 'r', encoding="utf-8") as f:
        lines = f.readlines()
    actual_start_checkpoint = "[\"TokenID"
    start_checkpoint = "\"Attributes\""
    end_checkpoint = "}]"
    dict_ = {}
    cur_tokenid = ""
    cur_dict = {}
    start_flag = False
    for line in lines:
        if not start_flag:
            if actual_start_checkpoint in line:
                cur_tokenid = line.strip().split("\": ")[-1][:-1]
            elif start_checkpoint in line:
                start_flag = True
        else:
            if end_checkpoint in line:
                start_flag = False
                dict_[cur_tokenid] = cur_dict
                cur_dict = {}
            else:
                line = line.strip().split("\": \"")
                key_ = line[0][1:]
                if line[1][-1] == ",":
                    cur_dict[key_] = line[1][:-2]
                else:
                    cur_dict[key_] = line[1][:-1]
    ret = json.dumps(dict_)
    address = file_path.split("/")[-1].split(".txt")[0]
    if not os.path.exists(write_path_prefix):
        os.mkdir(write_path_prefix)
    with open(write_path_prefix + address + ".json", 'w', encoding="utf-8") as f:
        f.writelines(ret) 

if __name__ == "__main__":
    files = os.listdir(input_path)
    for file in files:
        convert_to_json(input_path + file)
        
