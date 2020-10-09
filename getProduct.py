from functools import reduce
import pandas as pd

df_a= pd.read_csv("E:\pycharm\convert_xlsx_df\ExportDF\TeD_Products.csv")
df_b= pd.read_csv("E:\pycharm\convert_xlsx_df\ExportDF\TnS_Products.csv")
# df_c= pd.read_csv("E:\pycharm\convert_xlsx_df\ExportDF\CCC_Products.csv")


def getDF(filePath):
    dict = {}

    setupDict = 1

    # filePath = self.root + 'source/exportedDF/' + filePath
    # filePath = "E:\\pycharm\\practice\\source\\exportedDF\\RatesFile.csv"
    print("Source filepath is " + filePath)
    try:
        with open(filePath, 'r') as f:
            for l in f:
                if setupDict == 1:
                    for key in l.split("|"):
                        dict.update({key.strip()[1: -1]: []})
                    setupDict = 0
                else:
                    i = 0

                    keys = list(dict.keys())

                    for val in l.split("|"):
                        if val in ['None', 'nan']:
                            dict[keys[i]].append('')

                        else:
                            try:
                                dict[keys[i]].append(val.strip()[1:-1])
                            except:
                                dict[keys[i]].append('')
                        i += 1

    except:
        # return []
        return pd.DataFrame({})

    df = pd.DataFrame(dict)

    return df

df1 = getDF(df_a)
df2 = getDF(df_b)
df1.to_csv("E:\pycharm\convert_xlsx_df\ExportDF\\aa123.csv")
# df3 = getDF(df_b)

merged_df = pd.concat([df1, df2])
file_dfs = [df1, df2]


df_cols = ['Product ID (Mandatory)', 'productCategory (Mandatory)', 'CUA Effective From (Mandatory) Date/Time',
                             'Effective From', 'Effective To', 'Product Name (Mandatory)', 'Description', 'Apply Here URL (Mandatory)',
                             'isTailored (Mandatory)', 'Overview URL', 'Terms URL', 'Eligibility URL', 'Fees And PricingURL',
                             'Bundle URL'
                             ]

df_joinerColumn = 'productCategory (Mandatory)'

# df_final = reduce(lambda left,right: pd.merge(left,right,on='Product ID'), file_dfs )

merged_df.to_csv("E:\pycharm\convert_xlsx_df\ExportDF\\getProduct_final.csv")