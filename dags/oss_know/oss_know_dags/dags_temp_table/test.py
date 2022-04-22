# import time
#
# REPO_NUM = 10
#
# with open('../../../../data_schema/variables/projectlist0', 'r', encoding='utf8') as reader:
#     lines = reader.readlines()
# print(type(lines))
# count = 0
# write_lines = []
#
# for line in lines:
#     count = count + 1
#     if count < REPO_NUM:
#         print(line)
#         write_lines.append(line)
#     else:
#         count = 0
#         write_lines.append(line)
#         with open("../../../../data_schema/variables/projectlist1", "w") as writer:
#             print("=========================")
#             writer.writelines(write_lines)
#             time.sleep(0.3)
#         write_lines = []
#
# with open("../../../../data_schema/variables/projectlist1", "w") as writer:
#     print("=========================")
#     writer.writelines(write_lines)

import json
with open("../../../../data_schema/variables/big_variable.json","r",encoding="utf8") as variable_reader:
    variables = json.load(variable_reader)
    variable={}
    for key in variables:
        if key.startswith('need_init_'):
            variable[key] = variables[key]
print(variable)
with open('../../../../data_schema/variables/big_variable0.json', 'w', encoding='utf8') as variable_writer:
    json.dump(variable, variable_writer, indent=4)
