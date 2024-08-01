# reads in daily DH files (M:\Data\aws-sync\FTPToLocal\deephaven) and imports to database table (report.dh_pipeline_risk)

# import python utils
import pandas as pd
import datetime
import math

print("deephaven data - start")
start=datetime.datetime.now()

# import raw data from encompass, mtrade, fwd tracker -----------------------------------------------------------------------------------------------

# read in files - fwd_active, mtrade_pipeline, encompass_extract

dir = "M:\\Data\\aws-sync\\FTPToLocal\\deephaven\\"

dt = datetime.date.today()
# dt = datetime.date(2024,7,29)

dt_format = str(dt.strftime('%Y%m%d'))

delimiter = ","

mt_pipeline = pd.read_csv(dir + "mTrade Pipeline_" + dt_format + ".csv", sep=delimiter).rename(columns=lambda x: ' '.join(x.split()).strip())
enc_extract = pd.read_csv(dir + "Encompass Extract_" + dt_format + ".csv", sep=delimiter).rename(columns=lambda x: ' '.join(x.split()).strip())
fwd_active = pd.read_csv(dir + "Fwd Tracker_" + dt_format + ".csv", sep=delimiter).rename(columns=lambda x: ' '.join(x.split()).strip())

# clean, map, and standardize data tapes ------------------------------------------------------------------------------------------------------------

# set the mappings for cleaning data

date_replace = {"-":None,"UAC":None,"NaN":None}

prop_type_map = {
    'Single Family':'SNGLFAM',
    'PUD':'PUD',
    'Condominium':'CONDO',
    '2 to 4 Unit':'MULTIFAM',
    'Co-operative':'COOP',
    '0':'NA',
    '10254':'NA',
    '1007':'NA',
    'Detached':'SNGLFAM',
    'Attached':'SNGLFAM',
    'HighRiseCondominium':'CONDO',
    'DetachedCondo':'CONDO',
    ' ':'UNK',
    '':'UNK',
    '1 - Detached':'SNGLFAM',
    '2 - Detached':'MULTIFAM',
    '1 - Condominium':'CONDO',
    '1 - PUD':'PUD',
    '3 - Detached':'MULTIFAM',
    '4 - Detached':'MULTIFAM',
    '6 - Detached':'MULTIFAM',
    '5 - Detached':'MULTIFAM',
    '2 - Attached':'MULTIFAM',
    '1 - Attached':'SNGLFAM',
    '1 - HighRiseCondominium':'CONDO',
    '4 - Attached':'MULTIFAM',
    '3 - Attached':'MULTIFAM',
    '7 - Detached':'MULTIFAM',
    '1 - DetachedCondo':'CONDO',
    '4 - PUD':'PUD',
    '6 - Attached':'MULTIFAM',
    '  - Detached':'SNGLFAM',
    '2 - PUD':'PUD',
    '  - Condominium':'CONDO',
    '3 - Condominium':'CONDO',
    '8 - Detached':'MULTIFAM',
    '9 - Detached':'MULTIFAM',
    '  - DetachedCondo':'CONDO',
    '2 -  ':'MULTIFAM',
    '2 - ':'MULTIFAM',
    '3 - DetachedCondo':'MULTIFAM',
    '   - Detached':'SNGLFAM',
    '   - Condominium':'CONDO'}

prepay_term_map = {
    'No Penalty': 0,
    '36 Months': 36,
    '60 Months': 60,
    '24 Months': 24,
    '12 Months': 12,
    '0': 0,
    '48 Months': 48,
    '1': 12,
    '3': 36,
    '5 Years': 60,
    '3 Years': 36,
    '2 Years': 24,
    '1 Year': 12,
    '4 Years': 48,
    0:0
}

bwr_citizenship_map = {
    'U.S. Citizen': 'USCITIZEN',
    'Foreign National': 'FOREIGNNATIONAL',
    'ITIN': 'ITIN',
    'Permanent Resident-Alien': 'PERMRESIDENT',
    'Non-Permanent Resident-Alien': 'NONPERMRESIDENT',
    'Permanent Resident Alien': 'PERMRESIDENT',
    'Non-Permanent Resident Alien': 'NONPERMRESIDENT',
    '0': 'UNK',
    0: 'UNK',
    'USCitizen': 'USCITIZEN',
    'PermanentResidentAlien': 'PERMRESIDENT',
    'NonPermanentResidentAlien': 'NONPERMRESIDENT'
}

purpose_map = {
    'Purchase':'PURCHASE',
    'Cash-Out/Other':'CASHOUT',
    'No Cash Out':'RATETERM',
    'Cash-Out/Home Improvement':'CASHOUT',
    'Cash-Out/Debt Consolidation':'CASHOUT',
    'Limited Cash-Out':'CASHOUT',
    'Cash Out':'CASHOUT',
    'No Cash-Out Rate/Term':'RATETERM',
    'No Cash-Out/Other':'RATETERM',
    'Limited Cash Out':'CASHOUT',
    'Cash-Out Refinance':'CASHOUT',
    'NoCash-Out Refinance':'RATETERM',
    'Other':'UNK'}

occupancy_map = {
    'Primary Residence':'OWNER',
    'Investment Property':'INVESTMT',
    'Secondary Residence':'2NDHOME',
    '0':'UNK',
    'NonOwnerOccupied':'INVESTMT',
    'OwnerOccupied':'OWNER'}

doc_type_map = {
    '2 Year PL Only':'2YP&L',
    '1 Year PL Only':'1YP&L',
    '12 Mo Bank Statement':'BANKSTMT-12M',
    'DSCR':'DSCR',
    '12 Mo Full Documentation':'FULLDOC-12M',
    '24 Mo Full Documentation':'FULLDOC',
    'Asset Utilization':'ASSETDEPLETION',
    '24 Mo Bank Statement':'BANKSTMT-24M',
    '24 Mo 1099':'2Y1099',
    '12 Mo 1099':'1Y1099',
    'Full Documentation':'FULLDOC',
    'No Doc':'UNK',
    'Full Doc':'FULLDOC',
    'Business Bank Statement: 12 Mos':'BANKSTMT-12M',
    'Full Doc 24 Months':'FULLDOC',
    'Personal Bank Statement: 12 Mos':'BANKSTMT-12M',
    'Business Bank Statement: 24 Mos':'BANKSTMT-24M',
    'P&L 1 Year':'1YP&L',
    '1099 1 Year':'1Y1099',
    'Asset Depletion':'ASSETDEPLETION',
    '1099 2 Year':'2Y1099',
    'P&amp;L 1 Year':'1YP&L',
    'Personal Bank Statement: 24 Mos':'BANKSTMT-24M',
    'Asset Depletion Option 2':'ASSETDEPLETION',
    'DSCR >= 1.00':'DSCR',
    'Asset Related':'ASSETDEPLETION',
    '':'UNK'}

amort_type_map = {
    'Fixed Rate':'FIXED',
    'ARM':'ARM',
    '0':'FIXED',
    'Fixed':'FIXED',
    'AdjustableRate':'ARM'
}

mt_clean = pd.DataFrame(
    data = {
        'dh_loan_id': mt_pipeline["Buyer Loan Number"].map(lambda x: str(math.trunc(x))).to_list(),
        'channel': mt_pipeline["Trade Type"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'seller_originator': mt_pipeline["Seller Name"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'loan_amount': mt_pipeline["Loan Amount"].to_list(),
        'int_rate': mt_pipeline["Rate"].to_list(),
        'ltv': mt_pipeline["LTV"].to_list(),
        'cltv': mt_pipeline["Combined LTV (CLTV)"].replace(r'^\s*$', 0, regex=True).map(lambda x: float(x)*100).to_list(),
        'fico': mt_pipeline["FICO"].replace(r'^\s*$', 0, regex=True).fillna(0).to_list(),
        'purpose': mt_pipeline["Refinance Purpose"].replace(r'^\s*$', 'Purchase', regex=True).map(lambda x:' '.join(x.split()).strip()).map(purpose_map).to_list(),
        'occupancy': mt_pipeline["Occupancy"].map(lambda x:' '.join(x.split()).strip()).map(occupancy_map).to_list(),
        'doc_type': mt_pipeline["Loan Documentation"].map(lambda x:' '.join(x.split()).strip()).map(doc_type_map).to_list(),
        'io_flag': mt_pipeline['Interest Only (Y/N)'].map(lambda x: x.strip()).map({"Yes":"Y","No":"N"}).to_list(),
        'prepay_term': mt_pipeline["Prepayment Period (# of Months)"].map(lambda x:' '.join(x.split()).strip()).map(prepay_term_map).fillna(0).to_list(),
        'loan_term': mt_pipeline["Loan Term Months"].fillna(0).to_list(),
        'amort_type': mt_pipeline["Amortization Type"].map(lambda x:' '.join(x.split()).strip()).map(amort_type_map).to_list(),
        'dscr': mt_pipeline["DSCR"].replace(r'^\s*$', 0, regex=True).fillna(0).to_list(),
        'dti': mt_pipeline["Total Obligations/Income (DTI Ratio - Back)"].fillna(0).replace(r'^\s*$', 0, regex=True).map(lambda x: float(x)*100).to_list(),
        'prop_type': mt_pipeline["Property Type"].map(lambda x:' '.join(x.split()).strip()).map(prop_type_map).to_list(),
        'num_units': mt_pipeline["Number of Units"].map(lambda x: str(x).strip('\"')).to_list(),
        'appr_val': mt_pipeline["Appraised Value"].fillna(0).to_list(),
        'state': mt_pipeline["State"].to_list(),
        'bwr_citizenship': mt_pipeline["Borrower Citizenship"].map(lambda x:' '.join(x.split()).strip()).map(bwr_citizenship_map).to_list(),
        'lock_date': pd.to_datetime(mt_pipeline["Lock Date"].map(lambda x: x[0:10]), format="%Y-%m-%d", errors='coerce'),
        'lock_px':mt_pipeline["Net Price"].to_list(),
        'lock_status':mt_pipeline["Lock Status"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'source': 'mtr',
        'clean_room_status':mt_pipeline["Clean Room Workflow Status"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'purchase_date':mt_pipeline["Purchase Date"].to_list(),
        'upload_date':pd.to_datetime(mt_pipeline["Upload Date"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
    }
).set_index("dh_loan_id")

# for mtrade manual locks/bulk trades - use upload date as lock date
for ix, row in mt_clean.iterrows():
    if(pd.isnull(mt_clean.loc[ix]['lock_date']) and pd.notnull(mt_clean.loc[ix]['upload_date'])):
        mt_clean.at[ix, 'lock_date'] = mt_clean.loc[ix]['upload_date']

mt_clean['lock_date'] = pd.DatetimeIndex(mt_clean['lock_date']).normalize()
mt_clean = mt_clean[mt_clean.clean_room_status.notnull()]

enc_clean = pd.DataFrame(
    data = {
        'dh_loan_id': enc_extract["Loan ID"].map(lambda x: str(math.trunc(x))).to_list(),
        'channel': enc_extract["Loan Folder Name"].map(lambda x: str(x).split()[0]).map(lambda x:' '.join(x.split()).strip()).to_list(),
        'seller_originator': enc_extract["Originator Name"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'loan_amount': enc_extract["Loan Amount"].to_list(),
        'int_rate': enc_extract["Note Rate"].to_list(),
        'ltv': enc_extract["LTV"].to_list(),
        'cltv': enc_extract["CLTV"].to_list(),
        'fico': enc_extract["FICO"].to_list(),
        'purpose': enc_extract["Loan Purpose"].map(lambda x:' '.join(x.split()).strip()).map(purpose_map).to_list(),
        'occupancy': enc_extract["Occupancy Type"].map(lambda x:' '.join(x.split()).strip()).map(occupancy_map).to_list(),
        'doc_type': enc_extract["Doc Type"].map(lambda x:' '.join(x.split()).strip()).map(doc_type_map).to_list(),
        'io_flag': enc_extract['I/O Flag'].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'prepay_term': enc_extract["Penalty Term"].map(lambda x:' '.join(x.split()).strip()).replace(r'^\s*$', 0, regex=True).fillna(0).map(prepay_term_map).to_list(),
        'loan_term': enc_extract["Loan Term"].fillna(0).to_list(),
        'amort_type': enc_extract["Amortization Type"].map(lambda x:' '.join(x.split()).strip()).fillna(0).map(amort_type_map).to_list(),
        'dscr': enc_extract["DSCR Ratio"].fillna(0).to_list(),
        'dti': enc_extract["Original DTI"].fillna(0).to_list(),
        'prop_type': enc_extract["Subject Property Type"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'num_units': enc_extract["# of Units"].map(lambda x: str(x).strip('\"')).to_list(),
        'appr_val': 0,
        'state': enc_extract["State"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'bwr_citizenship': enc_extract["Borr Citizenship"].map(lambda x:' '.join(x.split()).strip()).replace(r'^\s*$', 0, regex=True).fillna(0).map(bwr_citizenship_map).to_list(),
        'lock_date': pd.to_datetime(enc_extract["Lock Date"].map(lambda x: x[0:10]), format="%Y-%m-%d %H:%M:%S", errors='coerce'),
        'lock_px': enc_extract["Lock Px"].to_list(),
        'lock_status':enc_extract["Current Milestone"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'loan_folder_name':enc_extract["Loan Folder Name"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'source': 'enc'
    }
).set_index("dh_loan_id")

for ix, row in enc_clean.iterrows():
    cat = (str(row['num_units']).split('.')[0] + " - " + row['prop_type'])
    enc_clean.at[ix, 'prop_type'] = prop_type_map[cat]

fwd_clean = pd.DataFrame(
    data = {
        'dh_loan_id': fwd_active['Deephaven ID'].map(lambda x: str(math.trunc(x))).to_list(),
        'fwd_name': fwd_active["Forward Name"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'investor': fwd_active['Investor'].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'dh_deal': fwd_active['Deal'].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'dh_funded_dt': pd.to_datetime(pd.Series(fwd_active['DH Funded Date']).replace(date_replace), errors='coerce').to_list(),
        'fallout_dt': pd.to_datetime(pd.Series(fwd_active['Fallout Date']).replace(date_replace), errors='coerce').to_list(),
        'dh_lock_dt': pd.to_datetime(pd.Series(fwd_active['DHM Lock Date']).replace(date_replace), errors='coerce').to_list(),
        'investor_shown_dt': pd.to_datetime(pd.Series(fwd_active['Date Presented to Investor']).replace(date_replace), errors='coerce').to_list(),
        'investor_lock_dt': pd.to_datetime(pd.Series(fwd_active['Investor Lock Date']).replace(date_replace), errors='coerce').to_list(),
        'investor_funded_dt': pd.to_datetime(pd.Series(fwd_active['Investor Funded Date']).replace(date_replace), errors='coerce').to_list(),
        'dh_px': fwd_active['DHM Px'].to_list(),
        'investor_px': fwd_active['Investor Px'].to_list(),
        'target_px': fwd_active['Target Px'].to_list(),
        'dh_program': fwd_active["Program"].map(lambda x:' '.join(x.split()).strip()).to_list(),
        'dh_product_flag': fwd_active["DHM Product Flag"].map(lambda x:' '.join(x.split()).strip()).to_list()
    }
).set_index("dh_loan_id")

# combine files with fwd tracker and export to csv --------------------------------------------------------------------------------------------------

# create combined output tape and final cleaning

df1 = pd.concat([mt_clean, enc_clean])
df2 = df1.join(fwd_clean, how="left").replace("-",None)

df2.insert(0, 'data_dt', dt) 
df2.reset_index(inplace=True)

# clean up stale records
df2 = df2[df2.lock_date.notnull()]
df2 = df2[df2.lock_status != "Funded"]
df2 = df2[df2.lock_status != "Withdrawn"]
df2 = df2[df2.lock_status != "Investor Canceled"] 

# cleanup:
# mtrade:
#   set funded date to purchase date if present
#   else set fallout date to upload date
# encompass:
#   if in adverse folder set fallout date to yday
for ix, row in df2.iterrows():
    df_data_source = df2.at[ix,'source']
    if (df_data_source == "mtr"):
        mtr_upload_dt = df2.at[ix,'upload_date']
        if (df2.at[ix,'clean_room_status'] == "Loan Rejected"):
            df2.at[ix, 'dh_lock_dt'] = mtr_upload_dt
            df2.at[ix, 'fallout_dt'] = mtr_upload_dt
        elif ((df2.at[ix,'clean_room_status'] == "Loan Purchased") or (pd.isnull(df2.at[ix,'dh_funded_dt']) and pd.isnull(df2.at[ix,'investor_funded_dt']) and pd.notnull(df2.at[ix,'purchase_date']))):
            df2.at[ix, 'dh_lock_dt'] = mtr_upload_dt
            df2.at[ix, 'dh_funded_dt'] = df2.at[ix,'purchase_date'] if pd.notnull(df2.at[ix,'purchase_date']) else mtr_upload_dt
            df2.at[ix, 'investor_funded_dt'] = df2.at[ix,'dh_funded_dt']
            df2.at[ix, 'dh_px'] = df2.at[ix,'lock_px']
            df2.at[ix, 'investor_px'] = df2.at[ix,'lock_px']
            df2.at[ix, 'target_px'] = df2.at[ix,'lock_px']
        elif pd.isnull(df2.at[ix,'dh_lock_dt']):
            df2.at[ix, 'dh_lock_dt'] = mtr_upload_dt
            df2.at[ix, 'fallout_dt'] = mtr_upload_dt
    elif (df_data_source == "enc"):
        if (df2.at[ix, 'loan_folder_name'][-7:] == "Adverse" and df2.at[ix, 'fallout_dt'] is not None):
            df2.at[ix, 'fallout_dt'] = (dt - datetime.timedelta(days=1))

# drop the extra columns
df2 = df2.drop(columns=['clean_room_status', 'upload_date', 'purchase_date', 'loan_folder_name']).copy()

# save output to csv
df2.to_csv("M:\\Analysts\\Rushil\\DH Risk Project\\daily_output\\" + dt_format + "_out_combined.csv",index=False)

# load to database ----------------------------------------------------------------------------------------------------------------------------------

# imports for db
import mysql.connector

# helper fn to clean up data before loading
def map_dtypes(x):
    if (type(x) is pd._libs.tslibs.nattype.NaTType or str(x) == "nan" or str(x) == "None"):
        return "null"
    elif isinstance(x, datetime.date):
        return x.strftime('%Y%m%d')
    elif isinstance(x, str):
        return "'" + x.replace("'","").replace('"', '') + "'"
    else:
        return str(x)


# load to report.dh_pipeline_risk_stg and call proc to update report.dh_pipeline_risk

# load to database and update table

pw = open("C:\\Users\\rshah\\.addin\\.addin.txt", "r").read()

db = mysql.connector.connect(
  host="db.pmc.app.pretium.com",
  port=3306,
  database = 'report',
  user="rshah",
  password=pw
)

cursor = db.cursor()

cursor.execute("delete from report.dh_pipeline_risk_stg")
db.commit()

cols = ",".join(df2.columns.tolist())

q1 = "insert into report.dh_pipeline_risk_stg ("
q2 = ") values "

top = len(df2)
chk = round(top/20,0)
vals=""

for i in range(top):
    if(i%chk == 0): print('\r', 'loading data - ' + str(round(i/top*100,0))+"%", end='')
    val = ",".join(df2.iloc[i].map(lambda x: map_dtypes(x)).to_list())
    val = val.replace("'nan'","null")
    vals += "("+val+"),"
    if (i%80 == 0 or i == top-1): 
      cursor.execute(q1+cols+q2+vals[:-1])
      db.commit()
      vals=""
      if(i == top-1): print('\r', "loading data - 100.0%", end='')

# run daily load proc
cursor = db.cursor()
cursor.execute("call report.dh_daily_risk_update")
db.commit()

cursor.close()
db.close()

# check runtime
print()
print("deephaven data - complete ("+str(datetime.datetime.now() - start)+")")