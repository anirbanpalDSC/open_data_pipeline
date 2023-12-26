# open_data_pipeline
Sample data pipeline to capture data, metadata and methodologies for further research, from openly available trustworthy data sources like World Bank, IMF, etc. 

Sample open license data sources captured as example:

### World Bank 
(12/25/2023)

The openData class has the following functions to capture data from World Bank API as well as scrape the metadata section of the databases to save the methodologies and metadata for user's reference. All you need to do is to identify the indicator codes from World Bank Data Bank and define the topic and subtopic (that the indicator belongs to) according to your need. Save those info in the index (multiple indicators) and you can access the data and methodology.

`prep_worldbank_data`

Retrieve and process World Bank data from the specified URL.

`prep_worldbank_data`

Format World Bank data for a given set of indicators.

`save_worldbank_meta`

Fetches metadata from the World Bank website for each key in the provided DataFrame, processes the data, and saves it as a CSV file in the output folder.

### International Monetory Fund (IMF)

WIP