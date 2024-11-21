from modelling.utils import *
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV
from io import StringIO

# Database connection details
db_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'D2st3n1t34n21rth$',
    'host': 'localhost',
    'port': '5432'  # Default port for PostgreSQL
}

# SQL query to fetch data from different tables
query1 = """
SELECT 
    -- Residential consumption selection
    rc.date::DATE,
    rc.postalcode, 
    rc.contracts::FLOAT, 
    rc.consumption::FLOAT,
    -- Calculate calendar features
    EXTRACT(YEAR FROM rc.date) AS year,
    EXTRACT(MONTH FROM rc.date) AS month,
    EXTRACT(DAY FROM rc.date) AS day,
    EXTRACT(DOW FROM rc.date) AS dayofweek,  -- 0=Sunday, 6=Saturday
    EXTRACT(DOY FROM rc.date) AS dayofyear,
    EXTRACT(WEEK FROM rc.date) AS weekofyear,
    -- Weather data
    el.airtemperature::FLOAT, 
    el.hdd::FLOAT, 
    el.cdd::FLOAT, 
    el.relativehumidity::FLOAT,
    el.windspeed::FLOAT, 
    el.winddirection::FLOAT, 
    el.ghi::FLOAT, 
    el.dni::FLOAT, 
    el.sunelevation::FLOAT,
    -- Cadaster data
    cd.totalbuiltareaaboveground::FLOAT, 
    cd.totalbuiltarea::FLOAT,
    cd.averageyearconstruction::FLOAT, 
    cd.dwellings::FLOAT, 
    cd.buildingsshare::FLOAT,
    -- Socioeconomical data
    se.percentagepopulationover65::FLOAT,
    se.percentagepopulationunder18::FLOAT,
    se.percentagesinglepersonhouseholds::FLOAT,
    se.population::FLOAT,
    se.incomesperhousehold::FLOAT,
    se.netincomesperhousehold::FLOAT,
    se.incomesperunitofconsumption::FLOAT,
    se.grossincomesperperson::FLOAT,
    se.netincomesperperson::FLOAT,
    se.incomessourceisotherbenefits::FLOAT,
    se.incomessourceisotherincomes::FLOAT,
    se.incomessourceispension::FLOAT,
    se.incomessourceisunemploymentbenefit::FLOAT,
    se.incomessourceissalary::FLOAT,
    se.giniindex::FLOAT,
    se.incomesratioq80byq20::FLOAT,
    se.averagepopulationage::FLOAT,
    se.peopleperhousehold::FLOAT
FROM residential_consumption_aggregated rc
LEFT JOIN era5land_aggregated el
ON rc.date = el.date AND rc.postalcode = el.postalcode
LEFT JOIN cadaster cd
ON cd.sector = '1_residential' AND cd.conditionofconstruction = 'functional' AND cd.postal_code = rc.postalcode
LEFT JOIN socioeconomic_filled se
ON se.year = EXTRACT(YEAR FROM rc.date) AND se.postalcode = rc.postalcode
ORDER BY rc.postalcode,rc.date;
"""

# Fetch data
df = fetch_data_from_db(
    query1, db_config)

# Filter the DataFrame to exclude rows where any column is null
df = df.filter(
    ~pl.any_horizontal(
        pl.col("*").is_null()
    )
)
df = df.sort("date")
# df = df.with_columns(
#     (df["consumption"]/df["contracts"]).alias("consumption"))

# Split data into features (X) and target (y)
X = df.drop(["consumption", "date", "postalcode"])  # replace 'target_column' with your actual target variable
y = df["consumption"]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Convert Polars DataFrame to XGBoost DMatrix
X_train_pd = X_train.to_pandas()
X_test_pd = X_test.to_pandas()
y_train_pd = y_train.to_pandas()
y_test_pd = y_test.to_pandas()
dtrain = xgb.DMatrix(X_train_pd, label=y_train_pd)
dtest = xgb.DMatrix(X_test_pd, label=y_test_pd)

# XGBoost parameters
params = {
    'objective': 'reg:squarederror',  # Use 'binary:logistic' for classification
    'max_depth': 12,
    'min_child_weight': 1,
    'subsample': 0.9,
    'colsample_bytree': 0.9,
    'eta': 0.1,
    'seed': 42
}

# Train the model
model = xgb.train(params, dtrain, num_boost_round=200, evals=[(dtest, 'eval')])

# Predict and evaluate
predictions = model.predict(dtest)# * X_test.to_pandas()["contracts"]
rmse = mean_squared_error(y_test_pd,#y_test.to_pandas() * X_test.to_pandas()["contracts"]
    predictions, squared=False)
mean_y_test = y_test_pd.mean()#(y_test.to_pandas() * X_test.to_pandas()["contracts"]).mean()
print(f"First attempt CVRMSE: {round(rmse/mean_y_test*100, 2)}%")

# Define XGBOOST hyperparameter grid
param_grid = {
    'max_depth': [9, 12, 15],
    'min_child_weight': [3, 5],
    'subsample': [0.8, 0.9, 1],
    'colsample_bytree': [0.8, 0.9, 1],
    'eta': [0.12, 0.075]
}

# Initialize XGBoost regressor
xgb_reg = xgb.XGBRegressor(objective='reg:squarederror', seed=42)

# Set up GridSearchCV
grid_search = GridSearchCV(estimator=xgb_reg, param_grid=param_grid, cv=5,
                           scoring='neg_mean_squared_error', verbose=1)

# Fit the model
grid_search.fit(X_train_pd, y_train_pd)

# Best parameters
print("Best parameters:", grid_search.best_params_)

# Update the hyperparams
params.update(grid_search.best_params_)
# Train the model with early stopping
model = xgb.train(
    params,
    dtrain,
    num_boost_round=200,
    evals=[(dtest, 'eval')],
    early_stopping_rounds=10
)

predictions = model.predict(dtest)# * X_test.to_pandas()["contracts"]
rmse = mean_squared_error(y_test_pd,
                          predictions, squared=False)# * X_test.to_pandas()["contracts"], predictions, squared=False)
mean_y_test = y_test_pd.mean()#(y_test.to_pandas() * X_test.to_pandas()["contracts"]).mean()
print(f"Hyperparams optimised CVRMSE: {round(rmse/mean_y_test*100, 2)}%")

# Get feature importance directly from the Booster object
importance = model.get_score(importance_type='gain')

# Convert to a DataFrame for better visualization
importance_df = pd.DataFrame({
    'Feature': importance.keys(),
    'Importance': importance.values()
})

# Replace NaN and zero values with a small positive value (e.g., 1e-5) to avoid log issues
importance_df['Importance'] = importance_df['Importance'].replace(0, 1e-5)  # Replace zeros
importance_df['Importance'] = importance_df['Importance'].fillna(1e-5)       # Replace NaNs

# Sort the features by importance
importance_df = importance_df.sort_values(by='Importance', ascending=False)

###
# PREDICT THE DT SCENARIO AND UPLOAD RESULTS
###

# SQL query for Climate Adaptation DT among others
query2 = """
SELECT 
    -- Residential consumption selection
    el.date::DATE,
    el.postalcode,
    -- Calculate calendar features
    EXTRACT(YEAR FROM el.date) AS year,
    EXTRACT(MONTH FROM el.date) AS month,
    EXTRACT(DAY FROM el.date) AS day,
    EXTRACT(DOW FROM el.date) AS dayofweek,  -- 0=Sunday, 6=Saturday
    EXTRACT(DOY FROM el.date) AS dayofyear,
    EXTRACT(WEEK FROM el.date) AS weekofyear,
    -- Weather data
    el.airtemperature::FLOAT, 
    el.hdd::FLOAT, 
    el.cdd::FLOAT, 
    el.relativehumidity::FLOAT,
    el.windspeed::FLOAT, 
    el.winddirection::FLOAT, 
    el.ghi::FLOAT, 
    el.dni::FLOAT, 
    el.sunelevation::FLOAT,
    -- Cadaster data
    cd.totalbuiltareaaboveground::FLOAT, 
    cd.totalbuiltarea::FLOAT,
    cd.averageyearconstruction::FLOAT, 
    cd.dwellings::FLOAT, 
    cd.buildingsshare::FLOAT,
    -- Socioeconomic data
    se.percentagepopulationover65::FLOAT,
    se.percentagepopulationunder18::FLOAT,
    se.percentagesinglepersonhouseholds::FLOAT,
    se.population::FLOAT,
    se.incomesperhousehold::FLOAT,
    se.netincomesperhousehold::FLOAT,
    se.incomesperunitofconsumption::FLOAT,
    se.grossincomesperperson::FLOAT,
    se.netincomesperperson::FLOAT,
    se.incomessourceisotherbenefits::FLOAT,
    se.incomessourceisotherincomes::FLOAT,
    se.incomessourceispension::FLOAT,
    se.incomessourceisunemploymentbenefit::FLOAT,
    se.incomessourceissalary::FLOAT,
    se.giniindex::FLOAT,
    se.incomesratioq80byq20::FLOAT,
    se.averagepopulationage::FLOAT,
    se.peopleperhousehold::FLOAT
FROM climatedt_aggregated el
LEFT JOIN cadaster cd
ON cd.sector = '1_residential' AND cd.conditionofconstruction = 'functional' AND cd.postal_code = el.postalcode
LEFT JOIN socioeconomic_filled se
ON se.year = 2024 AND se.postalcode = el.postalcode
WHERE EXTRACT(YEAR FROM el.date) > 2024
ORDER BY el.postalcode, el.date;
"""

# Fetch data
df_climatedt = fetch_data_from_db(
    query2, db_config)

df_climatedt = df_climatedt.join(
    df.group_by("postalcode").agg(
        pl.col("contracts").max().alias("contracts")
    ), on="postalcode", how="left")
X_climatedt = df_climatedt.drop(["date","postalcode"])
X_climatedt = X_climatedt[X.columns]
X_climatedt = xgb.DMatrix(X_climatedt.to_pandas())

# NumPy array
predictions = model.predict(X_climatedt)

# Convert the predictions to a Polars Series
predictions_series = pl.Series("consumption", predictions)

# Add the predictions as a new column to df_climatedt
df_climatedt = df_climatedt.with_columns(predictions_series)
df_climatedt_pandas = df_climatedt.select(["date", "postalcode", "consumption", "contracts"]).to_pandas()

# Step 3: Establish connection to the PostgreSQL database
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# Step 4: Create table (if it doesn't exist) to store data
create_table_query = """
DROP TABLE IF EXISTS residential_consumption_predicted_climatedt;
CREATE TABLE IF NOT EXISTS residential_consumption_predicted_climatedt (
    date DATE,
    postalcode VARCHAR(10),
    consumption FLOAT,
    contracts FLOAT
);
"""
cur.execute(create_table_query)
conn.commit()

# Step 5: Use StringIO to copy the DataFrame into PostgreSQL
output = StringIO()
df_climatedt_pandas.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)

# Step 6: Copy data from the DataFrame to the PostgreSQL table
cur.copy_from(output, 'residential_consumption_predicted_climatedt', null='', sep='\t', columns=("date", "postalcode", "consumption", "contracts"))
conn.commit()

# Step 7: Close the cursor and connection
cur.close()
conn.close()

###
# PREDICT THE DT SCENARIO AND UPLOAD RESULTS
###

# SQL query for Extreme Weather DT among others
query3 = """
SELECT 
    -- Residential consumption selection
    el.date::DATE,
    el.postalcode,
    -- Calculate calendar features
    EXTRACT(YEAR FROM el.date) AS year,
    EXTRACT(MONTH FROM el.date) AS month,
    EXTRACT(DAY FROM el.date) AS day,
    EXTRACT(DOW FROM el.date) AS dayofweek,  -- 0=Sunday, 6=Saturday
    EXTRACT(DOY FROM el.date) AS dayofyear,
    EXTRACT(WEEK FROM el.date) AS weekofyear,
    -- Weather data
    el.airtemperature::FLOAT, 
    el.hdd::FLOAT, 
    el.cdd::FLOAT, 
    el.relativehumidity::FLOAT,
    el.windspeed::FLOAT, 
    el.winddirection::FLOAT, 
    el.ghi::FLOAT, 
    el.dni::FLOAT, 
    el.sunelevation::FLOAT,
    -- Cadaster data
    cd.totalbuiltareaaboveground::FLOAT, 
    cd.totalbuiltarea::FLOAT,
    cd.averageyearconstruction::FLOAT, 
    cd.dwellings::FLOAT, 
    cd.buildingsshare::FLOAT,
    -- Socioeconomic data
    se.percentagepopulationover65::FLOAT,
    se.percentagepopulationunder18::FLOAT,
    se.percentagesinglepersonhouseholds::FLOAT,
    se.population::FLOAT,
    se.incomesperhousehold::FLOAT,
    se.netincomesperhousehold::FLOAT,
    se.incomesperunitofconsumption::FLOAT,
    se.grossincomesperperson::FLOAT,
    se.netincomesperperson::FLOAT,
    se.incomessourceisotherbenefits::FLOAT,
    se.incomessourceisotherincomes::FLOAT,
    se.incomessourceispension::FLOAT,
    se.incomessourceisunemploymentbenefit::FLOAT,
    se.incomessourceissalary::FLOAT,
    se.giniindex::FLOAT,
    se.incomesratioq80byq20::FLOAT,
    se.averagepopulationage::FLOAT,
    se.peopleperhousehold::FLOAT
FROM extremesdt_aggregated el
LEFT JOIN cadaster cd
ON cd.sector = '1_residential' AND cd.conditionofconstruction = 'functional' AND cd.postal_code = el.postalcode
LEFT JOIN socioeconomic_filled se
ON se.year = 2024 AND se.postalcode = el.postalcode
ORDER BY el.postalcode, el.date;
"""

# Fetch data
df_extremedt = fetch_data_from_db(
    query3, db_config)

df_extremedt = df_extremedt.join(
    df.group_by("postalcode").agg(
        pl.col("contracts").max().alias("contracts")
    ), on="postalcode", how="left")
X_extremedt = df_extremedt.drop(["date","postalcode"])
X_extremedt = X_extremedt[X.columns]
X_extremedt = xgb.DMatrix(X_extremedt.to_pandas())

# NumPy array
predictions = model.predict(X_extremedt)

# Convert the predictions to a Polars Series
predictions_series = pl.Series("consumption", predictions)

# Add the predictions as a new column to df_extremedt
df_extremedt = df_extremedt.with_columns(predictions_series)
df_extremedt_pandas = df_extremedt.select(["date", "postalcode", "consumption", "contracts"]).to_pandas()

# Step 3: Establish connection to the PostgreSQL database
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# Step 4: Create table (if it doesn't exist) to store data
create_table_query = """
DROP TABLE IF EXISTS residential_consumption_predicted_extremedt;
CREATE TABLE IF NOT EXISTS residential_consumption_predicted_extremedt (
    date DATE,
    postalcode VARCHAR(10),
    consumption FLOAT,
    contracts FLOAT
);
"""
cur.execute(create_table_query)
conn.commit()

# Step 5: Use StringIO to copy the DataFrame into PostgreSQL
output = StringIO()
df_extremedt_pandas.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)

# Step 6: Copy data from the DataFrame to the PostgreSQL table
cur.copy_from(output, 'residential_consumption_predicted_extremedt', null='', sep='\t', columns=("date", "postalcode", "consumption", "contracts"))
conn.commit()

# Step 7: Close the cursor and connection
cur.close()
conn.close()

