# ETL Script: Summary Table Data Cleaning and Reload to Oracle

This script:
- Connects to an Oracle database
- Extracts data from a summary table
- Sends an email alert if DB connection fails
- Cleans the data (missing, duplicate, format issues)
- Sends another email if location is missing
- Loads the cleaned data back into Oracle

---

## Requirements

```bash
pip install pandas sqlalchemy cx_Oracle
💻 Python ETL Script
python
# --- Import necessary libraries ---
import pandas as pd
import sqlalchemy
import cx_Oracle
from sqlalchemy import create_engine
import smtplib
from email.message import EmailMessage
import sys

# --- Function to send mail if DB connection fails ---
def send_connection_failure_email(error_details):
    msg = EmailMessage()
    msg['Subject'] = 'ETL Job Failed: DB Connection Error'
    msg['From'] = 'sender_email@example.com'
    msg['To'] = 'your_email@example.com, source_team@example.com'

    msg.set_content(f'''
Hello Team,

The ETL job failed to connect to the Oracle database.

Error Details:
{error_details}

Please check the database credentials or connection parameters and take corrective action.

Regards,  
BI Team
''')

    with smtplib.SMTP('smtp.yourmailserver.com', 587) as server:
        server.starttls()
        server.login('sender_email@example.com', 'your_password')  # Use env vars in real use
        server.send_message(msg)

# --- Define Oracle DB credentials ---
user_name = 'your_username'
password = 'your_password'
host = 'your_host'
port = '1560'
service_name = 'your_service_name'

# --- Try connecting and reading from DB ---
try:
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    engine = create_engine(f'oracle+cx_oracle://{user_name}:{password}@{dsn}')
    
    query = 'SELECT * FROM summary_table'
    df = pd.read_sql(query, con=engine)
    print("Successfully connected to the database and extracted data.")

except Exception as e:
    print("Failed to connect to the Oracle database or extract data.")
    print(f"Error details: {e}")
    send_connection_failure_email(str(e))  # trigger email alert
    sys.exit(1)
Data Cleaning Steps
python
# 1. Drop rows with missing values in fan_no and soid
df = df.dropna(subset=['fan_no', 'soid'])

# 2. Remove duplicate company names
df = df.drop_duplicates(subset=['company_name'])

# 3. Convert created and closed dates to datetime
df['created_dt'] = pd.to_datetime(df['created_dt'], errors='coerce')
df['closed_date'] = pd.to_datetime(df['closed_date'], errors='coerce')

# 4. Standardize activity values
valid_activity = ['upgrade', 'downgrade', 'shifting', 'normal']
df['activity'] = df['activity'].str.lower().str.strip()
df = df[df['activity'].isin(valid_activity)]
df['activity'] = df['activity'].str.capitalize()

# 5. Clean sales_person_id to be lowercase alphanumeric
df['sales_person_id'] = df['sales_person_id'].astype(str).str.lower()
df = df[df['sales_person_id'].str.match(r'^[a-z0-9]+$', na=False)]

# 6. Fill missing charges with 0
df['charges'] = pd.to_numeric(df['charges'], errors='coerce').fillna(0)
Email Trigger for Missing location Values
python

missing_location_df = df[df['location'].isna() | (df['location'].str.strip() == '')]

def send_missing_location_email(df_with_issues):
    msg = EmailMessage()
    msg['Subject'] = 'Missing Location Detected in Summary Table'
    msg['From'] = 'sender_email@example.com'
    msg['To'] = 'source_team@example.com, your_email@example.com'

    msg.set_content(f'''
Hello Team,

The ETL job detected {len(df_with_issues)} record(s) with missing or blank 'location' values
in the summary table. Please find the attached file for details.

Regards,  
BI Team
''')

    filename = 'missing_location_records.csv'
    df_with_issues.to_csv(filename, index=False)

    with open(filename, 'rb') as file:
        msg.add_attachment(file.read(), maintype='application', subtype='octet-stream', filename=filename)

    with smtplib.SMTP('smtp.yourmailserver.com', 587) as server:
        server.starttls()
        server.login('sender_email@example.com', 'your_password')  # Use env vars in real use
        server.send_message(msg)

if not missing_location_df.empty:
    send_missing_location_email(missing_location_df)
Reload Cleaned Data to Oracle
python

df.to_sql(
    name='cleaned_summary_table',
    con=engine,
    if_exists='replace',
    index=False,
    method='multi',
    chunksize=1000
)
print("✅ Cleaned data successfully reloaded into the database.")
