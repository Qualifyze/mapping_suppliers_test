import pandas as pd

qcr_2024 = "inputs/qcr_2024.csv"
qcr_2025 = "inputs/qcr_2025.csv"
audits = "inputs/audits.csv"

output_file = "outputs/latest_qcr.csv"


# load the data

df_2024 = pd.read_csv(qcr_2024)
df_2024 = df_2024[['Final QCR', 'auditID', 'qualityType']]
# rename auditID to id_audit
df_2024.rename(columns={'auditID': 'id_audit'}, inplace=True)

df_2025 = pd.read_csv(qcr_2025)
df_2025 = df_2025[['Final QCR', 'auditID', 'qualityType']]
# rename auditID to id_audit
df_2025.rename(columns={'auditID': 'id_audit'}, inplace=True)

# combine the data 
all = pd.concat([df_2024, df_2025], axis=0)
# Filter the data
all = all[all['qualityType'] == 'GMP_API']
all = all[all['Final QCR'] != 'Missing/wrong Data']


df_audits = pd.read_csv(audits)
df_audits['audit_date_cmp'] = pd.to_datetime(df_audits['audit_date'])

# inner join the data df_audits with all
merged = pd.merge(df_audits, all, on='id_audit', how='inner')

merged_sorted = merged.sort_values(by=['ceapp_id_supplier_site', 'audit_date_cmp'], ascending=[True, False])

latest_qcr_per_site = merged_sorted.groupby('ceapp_id_supplier_site').first().reset_index()

final = latest_qcr_per_site[['ceapp_id_supplier_site', 'id_audit', 'audit_date', 'Final QCR']]
final = final.rename(columns={'Final QCR': 'qcr'})
final = final.rename(columns={'audit_date': 'qcr_date'})

# save the data
final.to_csv(output_file, index=False)
