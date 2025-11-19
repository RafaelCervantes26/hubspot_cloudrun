# hubspot_cloudrun

Cloud-based script that uploads new records and keeps both companies and their associated contacts updated in real time.

# What does this script do? 🧠

This project synchronizes data from BigQuery to HubSpot.
For each record obtained from BigQuery, the script:

Creates or updates a company in HubSpot.

Creates or updates an associated contact.

Creates the association between the company and the contact.

Everything runs automatically based on the data extracted from BigQuery.

# 1. Initial Setup ⚙️

The script imports the necessary libraries:

json, time, os

requests to call the HubSpot API

bigquery to connect to Google BigQuery

Then it reads two important values:

HUBSPOT_API_KEY: the HubSpot API Key stored as an environment variable.

BIGQUERY_PROJECT: the BigQuery project name.

# 2. Mappings between BigQuery and HubSpot 🔁 

Since BigQuery and HubSpot use different column names, two dictionaries are created:

COMPANY_MAPPING: mapping of company properties.

CONTACT_MAPPING: mapping of contact properties.

Example:
profile_id (BigQuery) → ruc (HubSpot)

This ensures that each column maps to the correct property.

# 3. SQL Query 🧮

The QUERY_STRING must contain the SQL query that retrieves the records from BigQuery.
Each row of the result will be synchronized with HubSpot.

# 4. Date Conversion 🧱

The function to_timestamp(date_str) converts dates in the format DD-MM-YYYY into a timestamp in milliseconds, which is the format required by HubSpot for date fields.

If the date does not have the correct format, it returns None to avoid errors.

# 5. Fetching Data from BigQuery 📥

The function fetch_data_from_bigquery():

Connects to BigQuery.

Executes the SQL query.

Converts each row into a dictionary.

Returns a list of records ready to be synchronized.

If an error occurs, it shows a message and allows simulated data to be used during development.

# 6. upsert_company(): Create or Update Companies 🏢

For each record:

Retrieves the profile_id (RUC). If it doesn't exist, the record is skipped.

Searches in HubSpot for a company with that same RUC.

If it exists, obtains its company_id.

Builds the properties to send based on COMPANY_MAPPING.

Converts dates to timestamps.

Normalizes booleans (true/false → True/False).

Adjusts special values such as patron_operacional.

If the company exists, it performs a PATCH (update).

If it doesn't exist, it performs a POST (creation).

Returns the company_id.

# 7. upsert_contact(): Create or Update Contacts 👤

The logic is similar to the company process, but using DNI and/or email as unique fields.

Searches for the contact by DNI or email.

Builds the properties using CONTACT_MAPPING.

Converts dates to timestamps.

Converts certain values to integers.

If the contact exists: PATCH.

If it doesn't exist: POST.

Returns the contact_id.

# 8. associate_company_contact() 🔗

If both IDs exist:

Creates the association in HubSpot with type contact_to_company.

This links the contact to the newly created/updated company.

# 9. Main Flow (main) 🚀

The main() function coordinates everything:

Starts the job.

Fetches the records from BigQuery.

Sets up the headers with the API key.

For each record:

Creates/updates the company

Creates/updates the contact

Associates them

Pauses half a second between requests to avoid API overload.

Displays a final summary.
