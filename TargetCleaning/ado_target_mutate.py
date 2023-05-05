import pandas as pd

'''
Code for cleaning up the monolith data. 
Each of the target categories should be in its own column. 
Code reads in massive merged dataset (use ado_target_split to obtain), rewrites columns into a usable format, drops extraneous columns, writes to csv. 
'''
# Read in dataset
df = pd.read_csv("Data/Targets/merged.csv", index_col=0)
# df1 = pd.read_csv("fb_monolith.csv")

# Write columns to readable ones
df = df.assign(
    target_age=lambda dataframe: dataframe['AGE_GENDER'].map(lambda x: eval(x)[0]["Age"] if not pd.isna(x) else None),
    target_gender=lambda dataframe: dataframe['AGE_GENDER'].map(lambda x: eval(x)[0]["Gender"] if not pd.isna(x) else None),
    target_cae_page=lambda dataframe: dataframe['CUSTOM_AUDIENCES_ENGAGEMENT_PAGE'].map(lambda x: [y for y in eval(x)[0] if y != "CAEngagementPage" ] if not pd.isna(x) else None),
    target_location=lambda dataframe: dataframe['LOCATION'].map(lambda x: [y for y in eval(x)[0] if y != "Location" ] if not pd.isna(x) else None),
    target_ca_datafile=lambda dataframe: dataframe['CUSTOM_AUDIENCES_DATAFILE'].map(lambda x: [y for y in eval(x)[0] if y != "CustomAudiencesDatafile" ] if not pd.isna(x) else None),
    target_locale=lambda dataframe: dataframe['LOCALE'].map(lambda x: eval(x)[0]['Locale'] if not pd.isna(x) else None),
    target_ca_website=lambda dataframe: dataframe['CUSTOM_AUDIENCES_WEBSITE'].map(lambda x: eval(x)[0]['CAWebsite'] if not pd.isna(x) else None),
    target_ca_lookalike=lambda dataframe: dataframe['CUSTOM_AUDIENCES_LOOKALIKE'].map(lambda x: [y for y in eval(x)[0] if y != "CALookalike"] if not pd.isna(x) else None),
    target_ca_video=lambda dataframe: dataframe['CUSTOM_AUDIENCES_ENGAGEMENT_VIDEO'].map(lambda x: eval(x)[0]['CAEngagementVideo'] if not pd.isna(x) else None),
    target_ed_status=lambda dataframe: dataframe['ED_STATUS'].map(lambda x: eval(x)[0]['EdStatus'] if not pd.isna(x) else None),
    target_friend_of_connection=lambda dataframe: dataframe['FRIENDS_OF_CONNECTION'].map(lambda x: eval(x)[0]['FriendOfConnection']['PAGE'] if not pd.isna(x) else None)
    )

# DROP COLUMNS WITH NO EFFECT
df = df.drop(df.loc[:, "targetings":"COLLABORATIVE_AD"].columns, axis=1)

# Write to csv.
df.to_csv("Data/Targets/fb_monolith_with_targets.csv")

# # Testing Code
# for i in df["FRIENDS_OF_CONNECTION"]:
#     # print(i)
#     if not pd.isna(i):
#         lst = eval(i)[0]
#         print(lst)
#         # lst.remove("CustomAudiencesDatafile")
#         # print(lst)
#         print("===")
#         print(eval(i)[0]["FriendOfConnection"])
#     else:
#         print("elephant")
# l = df.assign(
#     target_bct=lambda dataframe: dataframe['BCT'].map(lambda x: eval(x)[0]["BCT"] if not pd.isna(x) else None)
# )