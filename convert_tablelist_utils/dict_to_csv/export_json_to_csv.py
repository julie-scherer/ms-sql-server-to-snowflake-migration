import pandas as pd

# ** BATCH 1 **
# from utils.Staging_CM_utils import fullTableList as utils_cm
# from utils.Staging_LD_utils import fullTableList as utils_ld
# utils = [utils_cm, utils_ld]
# util_names = ['utils_cm', 'utils_ld']

# ** BATCH 2 **
from utils.Staging_CREO_utils import fullTableList as utils_creo
utils = [utils_creo]
util_names = ['utils_creo']

# ** BATCH 3 **
# from utils.Staging_CM_utils import tableList as utils_creo_dq
# from utils.Staging_CM_utils import tableList as utils_cm_dq
# from utils.Staging_LD_utils import tableList as utils_ld_dq


def utils_to_csv(util_data, table_name):
    df = pd.DataFrame.from_dict(
        util_data, 
        orient='index'
    ).reset_index(level=0).rename(columns={'index':'table_name'})
    filename = f'csvs/{table_name}.csv'
    df.to_csv(
        filename,
        na_rep='NULL',
        sep="|",
        header=True,
        index=False
    )
    print(f"[SUCCESS] - Dictionary saved to {filename}")


for idx, util_data in enumerate(utils):
    filename = str(util_names[idx]).upper()
    utils_to_csv(util_data, filename)


# utils = [utils_cm, utils_cm_dq, utils_creo, utils_creo_dq, utils_ld, utils_ld_dq]
# util_names = ['utils_cm', 'utils_cm_dq', 'utils_creo', 'utils_creo_dq', 'utils_ld', 'utils_ld_dq']

# python3 export_json_to_csv.py
