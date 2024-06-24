import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import IsolationForest
import joblib

# Load the data
data = pd.read_csv('data.csv', low_memory=False)

# Inspect the columns
print(data.columns)

# Define features
numerical_features = [
    'employee_count', 'fortune_rank', 'forbes_rank', 'number_of_branches', 'uu_employee_total',
    'atc_capdb_score', 'b1ddi_capdb_score', 'ddi_capdb_score', 'b1td_capdb_total', 'b1ddi_capdb_total',
    'sales_volume_usd', 'total_cdat_certifications', 'total_cdca_certifications', 'total_ddip_certifications',
    'dnsfsp_certifications', 'psbo_ddiq_certifications', 'ps_botdq_certifications', 'pssdm_sdm_certifications',
    'total_sasi_certifications', 'total_tasi_certifications'
]

categorical_features = [
    'status_cd', 'distributor_flg', 'reseller_flg', 'bill_state', 'owner_geo_cd', 'owner_region_name',
    'account_geo_cd', 'account_region_name', 'territory_geo', 'territory_region', 'bill_country', 'industry_name',
    'reseller_level', 'account_type', 'customer_class', 'source_active_flg', 'sp_flag', 'partner_maintenance_eligible',
    'new_logo_flag', 'uber_ultimate_industry', 'data_steward_validation', 'account_segment', 'ownership',
    'shipping_country', 'eligible_for_perpetual', 'sales_focus', 'rc_type', 'billing_country', 'is_end_user',
    'partner_classification', 'partner_track', 'bloxone_sales_motions_pricing_models', 'focus_partner_multi',
    'partner_level', 'preferred_partner', 'focus_partner', 'dq_exempt', 'renewal_tier', 'renewal_authorized_direct',
    'archived_account', 'account_source'
]

date_features = [
    'cust_since_dt', 'customer_until_date', 'subs_end_date', 'last_modified_date', 'new_logo_since_date',
    'rc_until_date', 'oss_last_validated_date'
]

# Convert numerical features to numeric types and handle NaN values
data[numerical_features] = data[numerical_features].apply(pd.to_numeric, errors='coerce')
data[numerical_features] = data[numerical_features].fillna(0)  # or another suitable value

# Convert date features to numerical values (e.g., days since a reference date) and handle NaN values
for feature in date_features:
    data[feature] = (pd.to_datetime('now') - pd.to_datetime(data[feature], errors='coerce')).dt.days
data[date_features] = data[date_features].fillna(0)  # or another suitable value

# One-hot encode categorical features
data = pd.get_dummies(data, columns=categorical_features)

# Ensure all columns are numeric
all_features = numerical_features + date_features + list(data.columns.difference(numerical_features + date_features))

# Selecting relevant features
features = data[all_features]

# Drop any remaining rows with NaN values if any
features = features.dropna()

# Ensure all data is numeric and no NaN values
features = features.astype(float)

# Verify the shape of the DataFrame before fitting the model
print(features.shape)

# Train the Isolation Forest model
model = IsolationForest(contamination=0.01)
model.fit(features)

# Save the model
joblib.dump(model, 'isolation_forest_model.pkl')
