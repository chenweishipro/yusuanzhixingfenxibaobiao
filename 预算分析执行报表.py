# %%
from urllib.parse import quote
from sqlalchemy import create_engine
import pandas as pd
import re
from datetime import datetime


# %%
def create_otm_con():
    # 假设你的原始密码包含特殊字符
    password = "1q2w3e!@#"

    # 对密码进行URL编码
    encoded_password = quote(password)

    # 构建DATABASE_URL
    username = "saiyi"
    database = "ssc_1001_db"
    hostname = "rm-uf6um0z4uf7deto67.mysql.rds.aliyuncs.com"
    port = "3306"
    db_type = "mysql+pymysql"

    DATABASE_URL = f"{db_type}://{username}:{encoded_password}@{hostname}:{port}/{database}"

    return create_engine(DATABASE_URL)


# %%
con = create_otm_con()

summary_sql = """
select temp.* from (
        SELECT
            bm.BUSINESS_ID as budgetMaintainBusinessId,
            bm.DISPLAY_CODE as displayCode,
            bm.PERIOD_ID as periodId,
            bm.MAINTAIN_NAME as maintainName,
            bm.SUBJECT_ID as subjectId,
            bm.ITEM_ID as itemId,
            bm.SEGMENT as segment,
            bm.COST_CENTER as costCenter,
            bm.PROJECT as project,
            bm.SERVICE_LINE as serviceLine,
            bm.BUDGET_AMOUNT as budgetAmount,
            bm.BUDGET_REMAINING_SUM as budgetRemainingSum,
            bm.TOLERANCE as tolerance,
            bm.CONTROL_STRATEGY as controlStrategy,
            bm.BUDGET_CARRY_OVER as budgetCarryOver,
            bm.CARRY_OVER_PERIOD as carryOverPeriod,
            bm.RELATED_YEAR_BUDGET as relatedYearBudget,
            bm.ACTIVE_DATE as activeDate,
            bm.INACTIVE_DATE as inactiveDate,
            bm.DIMENSION1 as dimension1,
            bm.DIMENSION2 as dimension2,
            bs.SUBJECT_NAME as subjectNameLocal,
            bp.PERIOD_NAME as periodNameLocal,
            bm.BUDGET_DEDUCTION_SUM AS budgetDeductionSum,
            bm.SEGMENT_CATEGORY as segmentCategory,
            bm.BUDGET_FREEZE_SUM AS budgetFreezeSum,
            bm.CREATE_DATE

        FROM
            budget_maintain bm
        LEFT JOIN budget_subject bs ON bm.SUBJECT_ID = bs.BUSINESS_ID
        LEFT JOIN budget_period bp ON bm.PERIOD_ID = bp.BUSINESS_ID

        ) temp
        order by temp.CREATE_DATE desc
"""

# %%
df_summary = pd.read_sql(summary_sql, con)

# %%
df_summary

# %%


# %%
print(
    '############################################开始读取数据！##########################################################')

# %%
detail_sql = """
SELECT
            bm.BUSINESS_ID as budgetMaintainBusinessId,
            bm.DISPLAY_CODE as displayCode,
            bm.PERIOD_ID as periodId,
            bm.MAINTAIN_NAME as maintainName,
            bm.SUBJECT_ID as subjectId,
            bm.ITEM_ID as itemId,
            bm.SEGMENT as segment,
            bm.COST_CENTER as costCenter,
            bm.PROJECT as project,
            bm.SERVICE_LINE as serviceLine,
            bm.BUDGET_AMOUNT as budgetAmount,
            bm.BUDGET_REMAINING_SUM as budgetRemainingSum,
            bm.TOLERANCE as tolerance,
            bm.CONTROL_STRATEGY as controlStrategy,
            bm.BUDGET_CARRY_OVER as budgetCarryOver,
            bm.CARRY_OVER_PERIOD as carryOverPeriod,
            bm.RELATED_YEAR_BUDGET as relatedYearBudget,
            bm.ACTIVE_DATE as activeDate,
            bm.INACTIVE_DATE as inactiveDate,
            bm.DIMENSION1 as dimension1,
            bm.DIMENSION2 as dimension2,
            bs.SUBJECT_NAME as subjectNameLocal,
            bp.PERIOD_NAME as periodNameLocal,
            bm.BUDGET_DEDUCTION_SUM AS budgetDeductionSum,
            bm.SEGMENT_CATEGORY as segmentCategory,
            bm.BUDGET_FREEZE_SUM AS budgetFreezeSum,
            bm.CREATE_DATE

                ,bad.PLATFORM as platform
                ,bad.BUDGET_TYPE as budgetType
                ,bad.DISPLAY_CODE as documentNum
                ,bad.DOCUMENT_TYPE_CODE as documentTypeCode
                ,bad.DOCUMENT_TYPE_NAME as documentTypeName
                ,bad.APPLICANT as applicant
                ,bad.CURRENCY as currency
                ,bad.EXCHANGE_RATE as exchangeRate
                ,bad.ORIGINAL_CURRENCY_AMOUNT as originalCurrencyAmount
                ,bad.BUDGET_AMOUNT as budgetAmountDetail
                ,bad.DESCRIPTION as description
                ,bad.CREATE_DATE as operateDate
                ,case when bad.DOCUMENT_TYPE_NAME = '税务报账单' then str.application_date
                    when bad.DOCUMENT_TYPE_NAME = '薪酬计提单' then gsc.application_date
                    when bad.DOCUMENT_TYPE_NAME = '对公报销申请' then spe.application_date
                    when bad.DOCUMENT_TYPE_NAME = '会议费用报销单' then smce.application_date
                    when bad.DOCUMENT_TYPE_NAME = '营销费用报销单' then smr.application_date
                    when bad.DOCUMENT_TYPE_NAME = '员工费用报销单' then ter.application_date
                    when bad.DOCUMENT_TYPE_NAME = '员工借款申请' then loan.application_date
                else null
                end as applicationDate
                ,case when bad.DOCUMENT_TYPE_NAME = '税务报账单' then str.total_date
                    when bad.DOCUMENT_TYPE_NAME = '薪酬计提单' then gsc.total_date
                    when bad.DOCUMENT_TYPE_NAME = '对公报销申请' then spe.total_date
                    when bad.DOCUMENT_TYPE_NAME = '会议费用报销单' then smce.total_date
                    when bad.DOCUMENT_TYPE_NAME = '营销费用报销单' then smr.total_date
                    when bad.DOCUMENT_TYPE_NAME = '员工费用报销单' then ter.total_date
                    when bad.DOCUMENT_TYPE_NAME = '员工借款申请' then loan.total_date
                else null
                end as glDate
                ,case when bad.DOCUMENT_TYPE_NAME = '税务报账单' then str.department_id
                    when bad.DOCUMENT_TYPE_NAME = '薪酬计提单' then gsc.department_id
                    when bad.DOCUMENT_TYPE_NAME = '对公报销申请' then spe.department_id
                    when bad.DOCUMENT_TYPE_NAME = '会议费用报销单' then smce.department_id
                    when bad.DOCUMENT_TYPE_NAME = '营销费用报销单' then smr.department_id
                    when bad.DOCUMENT_TYPE_NAME = '员工费用报销单' then ter.department_id
                    when bad.DOCUMENT_TYPE_NAME = '员工借款申请' then loan.department_id
                else null
                end as APPLICATION_DEPT
                ,smce.CONFERENCE_NAME
                ,case when bad.DOCUMENT_TYPE_NAME = '税务报账单' then str.STATUS
                when bad.DOCUMENT_TYPE_NAME = '薪酬计提单' then gsc.STATUS
                when bad.DOCUMENT_TYPE_NAME = '对公报销申请' then spe.STATUS
                when bad.DOCUMENT_TYPE_NAME = '会议费用报销单' then smce.STATUS
                when bad.DOCUMENT_TYPE_NAME = '营销费用报销单' then smr.STATUS
                when bad.DOCUMENT_TYPE_NAME = '员工费用报销单' then ter.STATUS
                when bad.DOCUMENT_TYPE_NAME = '员工借款申请' then loan.STATUS
                else null
                end as STATUS
                ,case when bad.DOCUMENT_TYPE_NAME = '税务报账单' then str.BUSINESS_ID
                when bad.DOCUMENT_TYPE_NAME = '薪酬计提单' then gsc.BUSINESS_ID
                when bad.DOCUMENT_TYPE_NAME = '对公报销申请' then spe.BUSINESS_ID
                when bad.DOCUMENT_TYPE_NAME = '会议费用报销单' then smce.BUSINESS_ID
                when bad.DOCUMENT_TYPE_NAME = '营销费用报销单' then smr.BUSINESS_ID
                when bad.DOCUMENT_TYPE_NAME = '员工费用报销单' then ter.BUSINESS_ID
                when bad.DOCUMENT_TYPE_NAME = '员工借款申请' then loan.BUSINESS_ID
                else null
                end as docId
                ,case when bad.DOCUMENT_TYPE_NAME = '税务报账单' then ''
                when bad.DOCUMENT_TYPE_NAME = '薪酬计提单' then ''
                when bad.DOCUMENT_TYPE_NAME = '对公报销申请' then sped.remark
                when bad.DOCUMENT_TYPE_NAME = '会议费用报销单' then smcec.remark
                when bad.DOCUMENT_TYPE_NAME = '营销费用报销单' then smrc.remark
                when bad.DOCUMENT_TYPE_NAME = '员工费用报销单' then tref.remark
                when bad.DOCUMENT_TYPE_NAME = '员工借款申请' then ''
                else null
                end as remark

        FROM
            budget_maintain bm
        LEFT JOIN budget_subject bs ON bm.SUBJECT_ID = bs.BUSINESS_ID
        LEFT JOIN budget_period bp ON bm.PERIOD_ID = bp.BUSINESS_ID

            LEFT JOIN budget_amount_detail bad ON bm.BUSINESS_ID = bad.BUDGET_MAINTAIN_ID
            left join ssc_tax_reporting str on bad.DISPLAY_CODE = str.document_number
            left join gl_salary_calculation gsc on bad.DISPLAY_CODE = gsc.document_number
            left join ssc_public_expense spe on bad.DISPLAY_CODE = spe.document_number
            left join ssc_meeting_costing_expense smce on bad.DISPLAY_CODE = smce.document_number
            left join ssc_marketing_reimbursement smr on bad.DISPLAY_CODE = smr.document_number
            left join te_emp_reimbursement ter on bad.DISPLAY_CODE = ter.document_number
            left join te_loan_information loan on bad.DISPLAY_CODE = loan.document_number
            left join te_emp_reimbursement_fees tref on bad.DOCUMENT_LINE_NUM = tref.budget_line_id
            left join ssc_public_expense_detail sped on bad.DOCUMENT_LINE_NUM = sped.budget_line_id
            left join ssc_meeting_costing_expense_cost smcec on bad.DOCUMENT_LINE_NUM = smcec.budget_line_id
            left join ssc_marketing_reimbursement_cost smrc on bad.DOCUMENT_LINE_NUM = smrc.budget_line_id

"""

# %%

print(
    f'{datetime.now()}############################################开始读取数据源！##########################################################')
# %%
df_detail = pd.read_sql(detail_sql, con)

# %%

print(
    f'{datetime.now()}############################################读取数据源成功！##########################################################')
df_detail.shape

# %%
df_detail


# %%


# %%


# %%
def create_otm_con():
    # 假设你的原始密码包含特殊字符
    password = "1q2w3e!@#"

    # 对密码进行URL编码
    encoded_password = quote(password)

    # 构建DATABASE_URL
    username = "saiyi"
    database = "ssc_mdm_1001_db"
    hostname = "rm-uf6um0z4uf7deto67.mysql.rds.aliyuncs.com"
    port = "3306"
    db_type = "mysql+pymysql"

    DATABASE_URL = f"{db_type}://{username}:{encoded_password}@{hostname}:{port}/{database}"

    return create_engine(DATABASE_URL)


# %%
con2 = create_otm_con()
print('############################################开始计算！##########################################################')
# %%
# 成本中心名称
zdbc_sql = """
select VALUE_SET_CODE,VALUE_SET_NAME from mdm_section_value v
left join mdm_section_value_set s on s.SECTION_ID = v.BUSINESS_ID
where SECTION_CODE = 'BUDGET_COST_CENTER'
"""
df_other_fields = pd.read_sql(zdbc_sql, con2)
df_other_fields.set_index('VALUE_SET_CODE', inplace=True)
df_other_fields.columns = ['成本中心名称']
df_detail = pd.merge(df_detail, df_other_fields, left_on='costCenter', right_on='VALUE_SET_CODE', how='left')

# %%
# 预算科目名称
zdbc_sql1 = """
select VALUE_SET_CODE,VALUE_SET_NAME from mdm_section_value v
left join mdm_section_value_set s on s.SECTION_ID = v.BUSINESS_ID
where SECTION_CODE = 'SSC_BUDGET_ACCOUNT'
"""
df_other_fields1 = pd.read_sql(zdbc_sql1, con2)
df_other_fields1.set_index('VALUE_SET_CODE', inplace=True)
df_other_fields1.columns = ['预算科目名称']
df_detail = pd.merge(df_detail, df_other_fields1, left_on='segment', right_on='VALUE_SET_CODE', how='left')

# %%
# 匹配项目阶段名称
zdbc_sql2 = """
select VALUE_SET_CODE,VALUE_SET_NAME from mdm_section_value v
left join mdm_section_value_set s on s.SECTION_ID = v.BUSINESS_ID
where SECTION_CODE = 'SSC_PROJECT_PHASE'
"""
df_other_fields2 = pd.read_sql(zdbc_sql2, con2)
df_other_fields2.set_index('VALUE_SET_CODE', inplace=True)
df_other_fields2.columns = ['项目阶段名称']
df_detail = pd.merge(df_detail, df_other_fields2, left_on='dimension1', right_on='VALUE_SET_CODE', how='left')

# %%
# 匹配产品名称
zdbc_sql3 = """
select VALUE_SET_CODE,VALUE_SET_NAME from mdm_section_value v
left join mdm_section_value_set s on s.SECTION_ID = v.BUSINESS_ID
where SECTION_CODE = 'SSC_PRODUCT'
"""
df_other_fields2 = pd.read_sql(zdbc_sql3, con2)
df_other_fields2.set_index('VALUE_SET_CODE', inplace=True)
df_other_fields2.columns = ['产品名称']
df_detail = pd.merge(df_detail, df_other_fields2, left_on='dimension2', right_on='VALUE_SET_CODE', how='left')

# %%
# 科目类别名称
zdbc_sql4 = """
select VALUE_SET_CODE,VALUE_SET_NAME from mdm_section_value v
left join mdm_section_value_set s on s.SECTION_ID = v.BUSINESS_ID
where SECTION_CODE = 'SSC_ACCOUNT_CATEGORY'
"""
df_other_fields2 = pd.read_sql(zdbc_sql4, con2)
df_other_fields2.set_index('VALUE_SET_CODE', inplace=True)
df_other_fields2.columns = ['科目类别名称']
df_detail = pd.merge(df_detail, df_other_fields2, left_on='segmentCategory', right_on='VALUE_SET_CODE', how='left')

# %%
df_summary

# %%
df_detail

# %%
df_other_fields

# %%
df_detail['STATUS'].unique()

# %%
df_detail.loc[df_detail['STATUS'] == '处理中']

# %%


# %%


# %%


# %%
df_detail

# %%
sql_text_project_name = """
SELECT
            BUSINESS_ID AS businessId
            ,ORGANIZATION_NAME AS organizationName
            ,PROJECT_ID AS projectId
            ,PROJECT_NAME AS projectName
            ,START_TIME AS startTime
            ,END_TIME AS endTime
            ,DELETE_FLAG AS deleteFlag
            ,PROPERTY AS property
            ,ERP_FLAG AS erpFlag
        FROM te_project_management
        WHERE 1 = 1 AND delete_flag = 0	
        ORDER BY modified_date DESC
"""

# %%
df_project_name = pd.read_sql(sql_text_project_name, con)

# %%


# %%
df_project_name

# %%
df_project_name = df_project_name[['projectId', 'projectName']].copy()

# %%
df_project_name.drop_duplicates(subset=['projectId'], keep='last', inplace=True)

# %%
df_project_name.set_index('projectId', inplace=True)

# %%
df_project_name.columns = ['项目名称']

# %%
df_detail = pd.merge(df_detail, df_project_name, left_on='project', right_on='projectId', how='left')

# %%
# 员工信息
sql_text = """
  select * from mdm_employee
"""
df_emp = pd.read_sql(sql_text, con)
df_emp = df_emp[['AD_ACCOUNT', 'EMPLOYEE_NUMBER', 'REAL_NAME']].copy()
df_detail = pd.merge(df_detail, df_emp, left_on=['applicant'], right_on=['AD_ACCOUNT'], how='left')

# %%
# 部门信息
sql_text = "select * from mdm_organization"
df_department = pd.read_sql(sql_text, con2)
df_department = df_department[['BUSINESS_ID', 'ORGANIZATION_NAME']].copy()
df_detail = pd.merge(df_detail, df_department, left_on='APPLICATION_DEPT', right_on='BUSINESS_ID', how='left')

# %%
# 业务类型
df_detail

# %%
# 业务小类


# %%
tmp = df_detail[['docId', 'segment']].drop_duplicates()

# %%
s1 = list(df_detail['docId'].unique())
s1 = [_ for _ in s1 if pd.notna(_)]

# %%
s2 = list(df_detail['segment'].unique())
s2 = ['"' + _ + '"' for _ in s1 if pd.notna(_)]

# %%
sql = f"""
select
        t1.business_id docId,
        t2.invoice_subcategory_code categoryCode,
        t2.segment_value_id segment,
        t1.document_number docNum
        from te_emp_reimbursement t1 inner join
        te_emp_reimbursement_fees t2 on t1.business_id = t2.parent_id

        union all
        select
        t1.business_id docId,
        t2.invoice_subcategory_code categoryCode,
        t2.segment_value_id segment,
        t1.document_number docNum
        from ssc_meeting_costing_expense t1 inner join
        ssc_meeting_costing_expense_cost t2 on t1.business_id = t2.expense_id

        union all
        select
        t1.business_id docId,
        t2.invoice_subcategory_code categoryCode,
        t2.segment_value_id segment,
        t1.document_number docNum
        from ssc_public_expense t1 inner join
        ssc_public_expense_detail t2 on t1.business_id = t2.expense_id

        union all
        select
        t1.business_id docId,
        t2.invoice_subcategory_code categoryCode,
        t2.segment_value_id segment,
        t1.document_number docNum
        from ssc_marketing_reimbursement t1 inner join
        ssc_marketing_reimbursement_cost t2 on t1.business_id = t2.expense_id

"""

# %%
df_bs_small_kind = pd.read_sql(sql, con)

# %%


# %%
t3 = pd.read_sql("select CATEGORY_LINE_CODE, CATEGORY_LINE_NAME from mdm_business_category_line", con2)

# %%


# %%
df_bs_small_kind = pd.merge(df_bs_small_kind, t3, left_on='categoryCode', right_on='CATEGORY_LINE_CODE', how='left')

# %%


# %%
df_bs_small_kind.head()

# %%
df_bs_small_kind.columns

# %%
df_bs_small_kind = df_bs_small_kind[['docId', 'segment', 'CATEGORY_LINE_NAME']].copy()

# %%
df_bs_small_kind.head()

# %%
df_bs_small_kind = df_bs_small_kind.groupby(['docId', 'segment']).apply(
    lambda x: set(x['CATEGORY_LINE_NAME'])).to_frame()

# %%
df_bs_small_kind.columns = ['CATEGORY_LINE_NAME']

# %%
df_bs_small_kind.columns = ['业务小类名称']

# %%
df_bs_small_kind

# %%
list(df_bs_small_kind.index.names)

# %%
df_detail.shape

# %%
df_detail = pd.merge(df_detail, df_bs_small_kind, on=list(df_bs_small_kind.index.names), how='left')

# %%
df_detail.shape
print(datetime.now())
print('############################################完成计算！##########################################################')


# %%
def create_otm_con():
    # 假设你的原始密码包含特殊字符
    password = "jyk+WGv5ah"

    # 对密码进行URL编码
    encoded_password = quote(password)

    # 构建DATABASE_URL
    username = "root"
    database = "otm"
    hostname = "10.10.2.191"
    port = "3306"
    db_type = "mysql+pymysql"

    DATABASE_URL = f"{db_type}://{username}:{encoded_password}@{hostname}:{port}/{database}"

    return create_engine(DATABASE_URL)


# %%
def set_to_str(x):
    try:
        return ",".join(list(x))
    except:
        return str(x)


# %%
df_detail['业务小类名称'] = df_detail['业务小类名称'].map(set_to_str)

# %%
a = """CREATE_DATE 创建日期,
activeDate 预算生效日期,
applicationDate 单据发起日期,
glDate  总账日期,
inactiveDate 预算失效日期,
operateDate 预算执行时间,
APPLICATION_DEPT 单据发起部门,
CONFERENCE_NAME 会议名称,
STATUS 单据状态,
 applicant 发起人,
budgetCarryOver 预算结转,
budgetMaintainBusinessId 预算id,
budgetType 预算操作类型,
carryOverPeriod   结转期间,
controlStrategy 控制策略,
costCenter 成本中心,
currency 币种,
description 摘要,
dimension1 项目阶段,
dimension2 产品,
displayCode 预算编号,
docId 单据id,
documentNum 单据编号,
documentTypeCode 单据类型编号,
documentTypeName 单据类型名称,
itemId  预算表项目ID,
maintainName 预算名称,
periodId  预算期间ID,
periodNameLocal 预算期间,
platform 来源,
project  项目,
relatedYearBudget 关联年度预算ID,
remark 备注,
segment  预算科目,
segmentCategory 科目类别,
serviceLine  业务线,
subjectId 预算主体ID,
subjectNameLocal 预算主体名称,
budgetAmount 预算金额,
budgetAmountDetail 本币金额,
budgetDeductionSum  预算扣减金额,
budgetFreezeSum 预算冻结金额,
budgetRemainingSum 预算余额,
exchangeRate 汇率,
originalCurrencyAmount 原币金额,
tolerance  允差"""

# %%
names = {_.split()[0]: _.split()[-1] for _ in a.split(',\n')}

# %%
df_detail.rename(columns=names, inplace=True)


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


conx = create_otm_con()


def my_clean(x):
    x = str(x)
    try:
        return ";".join(re.findall(r'"zh-CN":"([^"]+)"', x))
    except:
        return str(x)


df_detail['业务小类名称'] = df_detail['业务小类名称'].map(my_clean)
df_detail.rename(columns={"ORGANIZATION_NAME": "部门名称"}, inplace=True)
df_detail['部门名称'] = df_detail['部门名称'].map(my_clean)
df_detail['预算主体名称'] = df_detail['预算主体名称'].map(my_clean)

dic = {'AD_ACCOUNT': 'ad账号', 'EMPLOYEE_NUMBER': '员工号', 'REAL_NAME': '名称', 'BUSINESS_ID': '预算维护表ID',
       'ORGANIZATION_NAME': '部门名称'}
df_detail.rename(columns=dic, inplace=True)
df_detail['预算操作类型'] = df_detail['预算操作类型'].map({"FREEZE": "冻结",
                                                           "RELEASE": "释放",
                                                           "DEDUCTION": "扣减",
                                                           "ADJUST": "调整",
                                                           "ADD": "新增"
                                                           })

# %%
print(df_detail.shape)

df_cost = pd.read_sql("SELECT * FROM `BU成本中心code映射表` ", con=conx)
df_cost.set_index('ORGANIZATION_CODE', inplace=True)
df_detail = pd.merge(df_detail, df_cost, left_on=['成本中心'], right_on=['ORGANIZATION_CODE'], how='left')
df_detail['成本中心'] = df_detail['成本中心名称'].astype(str) + '-' + df_detail['成本中心'].astype(str)
df_detail['预算科目'] = df_detail['预算科目名称'].astype(str) + '-' + df_detail['预算科目'].astype(str)
df_detail['申请人'] = df_detail['名称'].fillna("").astype(str) + '-' + df_detail['员工号'].fillna("").astype(str)
df_detail['申请人部门'] = df_detail['部门名称']
print(f'{datetime.now()}计算完成....,开始写入数据库')


def write_data_to_clickhouse(df):
    import clickhouse_connect, time
    client = clickhouse_connect.get_client(host='10.10.2.51', port=8123, username='root', password='3s123456')
    try:
        client.command("TRUNCATE TABLE `预算执行报表`")
    except Exception:
        print(".................删除表数据失败了！")
    t1 = datetime.now()
    client.insert_df('预算执行报表', df)
    t2 = datetime.now()
    print(f"写入时间为：{t2 - t1}")


try:
    write_data_to_clickhouse(df_detail)
except Exception:
    print("⚠️导出数据到ClickHouse失败了！")
    df_detail.to_sql("预算执行报表", con=conx, chunksize=10000, index=False, if_exists="replace")
print(f'{datetime.now()}写入完成！')



