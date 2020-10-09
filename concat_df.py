import pandas as pd

df_a= ("E:\pycharm\convert_xlsx_df\ExportDF\TeD_Products.csv")
df_b= ("E:\pycharm\convert_xlsx_df\ExportDF\TnS_Products.csv")
df_c= ("E:\pycharm\convert_xlsx_df\ExportDF\CCC_Products.csv")


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
df2= getDF(df_b)
df3 = getDF(df_c)

merged_df = pd.concat([df1, df2, df3])

merged_df.to_csv("E:\pycharm\convert_xlsx_df\ExportDF\\final_souce.csv")