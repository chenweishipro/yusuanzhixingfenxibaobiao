# %%
import pandas as pd
import os
from sqlalchemy import * 
from urllib.parse import quote

# %%
def create_otm_con(database = "ssc_1001_db"):
    # 假设你的原始密码包含特殊字符
    password = "1q2w3e!@#"

    # 对密码进行URL编码
    encoded_password = quote(password)

    # 构建DATABASE_URL
    username = "saiyi"
    hostname = "rm-uf6um0z4uf7deto67.mysql.rds.aliyuncs.com"
    port = "3306"
    db_type = "mysql+pymysql"

    DATABASE_URL = f"{db_type}://{username}:{encoded_password}@{hostname}:{port}/{database}"

    return create_engine(DATABASE_URL)

# %%
con = create_otm_con(database = 'ssc_mdm_1001_db')

# %%
EBU = """
 SELECT * FROM mdm_organization where PARENT_ORGANIZATION_ID = '6177' union all
select * from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6177') union all

select * from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6177')) union all

select * from mdm_organization where PARENT_ORGANIZATION_ID  IN (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6177')))
"""

# %%
df_EBU = pd.read_sql(EBU, con)

# %%
df_EBU

# %%
df_EBU["BU"] = "EBU"

# %%
df_EBU.shape

# %%
GBU = """
 SELECT * FROM mdm_organization where PARENT_ORGANIZATION_ID = '6172' union all
select * from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6172') union all

select * from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6172')) union all

select * from mdm_organization where PARENT_ORGANIZATION_ID  IN (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6172'))) 
"""

# %%
df_GBU = pd.read_sql(GBU, con)

# %%
df_GBU["BU"] = "GBU"

# %%
df_GBU.shape

# %%
TBU = """
 SELECT * FROM mdm_organization where PARENT_ORGANIZATION_ID = '6182' union all
select * from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6182') union all

select * from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6182')) union all

select * from mdm_organization where PARENT_ORGANIZATION_ID  IN (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6182'))) 
"""

# %%
df_TBU = pd.read_sql(TBU, con)

# %%
df_TBU["BU"] = "TBU"

# %%
df_TBU.shape

# %%
NBU = """
 SELECT * FROM mdm_organization where PARENT_ORGANIZATION_ID = '6366' union all
select * from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6366') union all

select * from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6366')) union all

select * from mdm_organization where PARENT_ORGANIZATION_ID  IN (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6366')))
"""

# %%
df_NBU = pd.read_sql(NBU, con)

# %%
df_NBU['BU'] = 'NBU'

# %%
df_NBU.shape

# %%
WBU = """
 SELECT * FROM mdm_organization where PARENT_ORGANIZATION_ID = '6212' union all
select * from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6212') union all

select * from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6212')) union all

select * from mdm_organization where PARENT_ORGANIZATION_ID  IN (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID in (select BUSINESS_ID from mdm_organization where PARENT_ORGANIZATION_ID  in (SELECT BUSINESS_ID FROM mdm_organization where PARENT_ORGANIZATION_ID = '6212'))) 
"""

# %%
df_WBU = pd.read_sql(WBU, con)

# %%
df_WBU['BU'] = 'WBU'

# %%
df_WBU.shape

# %%
df = pd.concat([df_EBU, df_GBU, df_NBU, df_TBU, df_WBU])

# %%
def create_otm_con():
    # 假设你的原始密码包含特殊字符
    password = "3s123456"

    # 对密码进行URL编码
    encoded_password = quote(password)

    # 构建DATABASE_URL
    username = "root"
    database = "otm"
    hostname = "10.10.2.51"
    port = "3306"
    db_type = "mysql+pymysql"

    DATABASE_URL = f"{db_type}://{username}:{encoded_password}@{hostname}:{port}/{database}"

    return create_engine(DATABASE_URL)

# %%
df_result = df[['ORGANIZATION_CODE', 'BU']].copy()
df_result = pd.concat([df_result, pd.DataFrame({'ORGANIZATION_CODE':['6172','6520', '6307', '10035', '10009', '9136', '10128', '10123'], 'BU':['GBU','全国市场准入部', '全国关键客户部', '中央医学事务部', '中央医学事务部', '全国市场准入部', '中央医学事务部', '中央医学事务部']})])

# %%
df_result.drop_duplicates(inplace=True)

# %%
df_result

# %%
df_result.to_sql("BU成本中心code映射表", con = create_otm_con(),chunksize=20000,index=False, if_exists="replace")

# %%



