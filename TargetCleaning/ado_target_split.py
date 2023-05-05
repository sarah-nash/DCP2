import pandas as pd
import ast

'''
Code for splitting "targetings" column of fb_monolith.csv
Reads in "targetings" col, splits information into separate for target category. 
Data is somewhat readable as is; couple with ado_target_mutate.py to enhance readability. 
'''

# Read in monolith data.
df = pd.read_csv(
    "Data/fb_monolith.csv")
# print(df.head())
# sample_json = df.loc[1].at["targetings"]


# Strategy: convert everything to a list of dicts, then convert to a data frame.
def get_target_dicts():
    count = 0  # count for indexing
    targets_list = []  # write to list before converting to df; bad form to construct df row by row
    for i in df["targetings"]:  # row by row not ideal fix later?
        if i != "{}":
            s = ast.literal_eval(i)
            for key in s.keys():
                # Single out interests; set structure was throwing errors
                if key == "INTERESTS":
                    interests = [ints["Interests"] for ints in s[key]]
                    s[key] = interests
                if len(s[key]) > 1:
                    if type(s[key][0]) is set:
                        s[key] = [set.union(*s[key])]
            targets_list.append(s)
        else:
            targets_list.append("")
        count += 1
    return targets_list


my_df = pd.json_normalize(get_target_dicts())  # just the targets
best = pd.concat([df, my_df], axis=1)  # targets merged with original dataframe

# save dataframes to a new output
my_df.to_csv("Data/Targets/test.csv", index=True)
best.to_csv("Data/Targets/merged.csv", index=False)
