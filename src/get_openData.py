# Import modules
import pandas as pd
import requests
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup

# Class for functions
class openData():
    def __init__(self):
        self.today = datetime.now().strftime('%Y-%m-%d')

    def get_worldbank_data(self, url):
        """Retrieve and process World Bank data from the specified URL.

            Args:
                url (str): The URL to retrieve World Bank data.

            Returns:
                list: A list containing World Bank data, last update information, and data source.
                    The list has the following format: [all_data, last_update, source]

            Raises:
                requests.exceptions.RequestException: If the HTTP request to the specified URL fails,
                                                    an exception is raised, and a message is printed.
                json.JSONDecodeError: If there is an error decoding the JSON response from the URL,
                                    an exception is raised, and a message is printed.
            """
        page = 1
        all_data = []
        request_delay = 1
        total_page = 1
        source = 'World Bank'
        last_update = None
        
        try:
            while page <= total_page:
                response = requests.get(f"{url}&page={page}")
                
                # Check if the request was successful (status code 200)
                response.raise_for_status()

                data = json.loads(response.text)

                if total_page == 1:
                    total_page = data[0]['pages']
                    if page==1:
                        last_update = data[0]['lastupdated']

                all_data.extend(data[1])
                page += 1
                time.sleep(request_delay)

        except requests.exceptions.RequestException as e:
            print(f"Request failed with exception: {str(e)}")
            return [None, None, None] 
        
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {str(e)}")
            return [None, None, None] 

        return [all_data, last_update, source]


    def prep_worldbank_data(self, df_source):

        """Format World Bank data for a given set of indicators.

        Args:
            data (type): The input data for formatting.

        Returns:
            pd.DataFrame: A DataFrame containing the formatted World Bank data.

        Raises:
            Exception: If an error occurs during data processing for a specific indicator,
                    an exception is raised, and an error message is printed.
        """
        df_combo = pd.DataFrame()

        for index, row in df_source.iterrows():
            key = row.key
            print(f"Getting data for {key}.")

            url = f"https://api.worldbank.org/v2/country/all/indicator/{key}?format=json{row.extra}&per_page=1000"

            try:

                result = self.get_worldbank_data(url)
                data = result[0]               
                if len(data) > 0:
                    df = pd.DataFrame(data)
                    df['indicator_name'] = df['indicator'].apply(lambda x: x.get('value',''))
                    df['iso2_code'] = df['country'].apply(lambda x: x.get('value',''))
                    df['country'] = df['country'].apply(lambda x: x.get('value',''))
                    df['source_code'] = key
                    df['topic'] = row.topic
                    df['subtopic'] = row.subtopic
                    df['temporality'] = row.temporality
                    df['unit'] = row.unit
                    df['variable_id'] = row.variable_id
                    df['acquired_dt'] = self.today
                    df['recency_dt'] = result[1]
                    df['source'] = result[2]

                    # Columns to potentially drop
                    columns_to_drop = ['indicator', 'obs_status', 'decimal']
                    if (columns_exist := [col for col in columns_to_drop if col in df.columns]):
                        df.drop(columns_exist, axis=1, inplace=True)

                    df = df.rename(columns={'countryiso3code':'iso3_code'})
                    df_combo = pd.concat([df_combo, df], axis=0)

            except Exception as e:
                print(f"Error processing data for {key}: {str(e)}")

        return df_combo


    def save_worldbank_meta(self, df_source):
        """
            Fetches metadata from the World Bank website for each key in the provided DataFrame,
            processes the data, and saves it as a CSV file in the output folder.

            Parameters:
            - df_source (pd.DataFrame): The DataFrame containing a column named 'key', which
            represents the keys for fetching metadata from the World Bank website.

            Raises:
            - ValueError: If df_source is not a valid DataFrame or is empty.

            Notes:
            - The function uses the 'key' column from df_source to construct URLs for fetching
            metadata from the World Bank website.
            - Metadata is scraped from the website, processed, and saved as a CSV file in the
            '../output/' folder with the filename '{key}_meta.csv'.
            - Column names in the resulting CSV file are converted to lowercase, and spaces
            are replaced with underscores.

            Example:
            >>> format_worldbank_data(your_dataframe)
            """
        try:
            # Check if df_source is a DataFrame and is not empty
            if not isinstance(df_source, pd.DataFrame) or df_source.empty:
                raise ValueError("Input is not a valid DataFrame or is empty.")

            for index, row in df_source.iterrows():
                key = row.get('key')  # Assuming 'key' is a column in df_source
                if not key:
                    print("Warning: 'key' column is missing in the DataFrame.")
                    continue

                print(f"Getting methodology and metadata for {key}.")

                url = f"https://databank.worldbank.org/metadataglossary/all/series/{key}"

                response = requests.get(url)
                if response.status_code == 200:
                    html = response.text
                    soup = BeautifulSoup(html, 'html.parser')
                    td_elements = soup.find_all('td', class_='glossary-1')
                    td_data = soup.find_all('td', class_='glossary-2')

                    if not td_elements or not td_data:
                        print(f"{key}: Metadata missing or not in expected format.")
                        continue

                    data_dict = {
                        title.text: data.text
                        for title, data in zip(td_elements, td_data)
                    }

                    df = pd.DataFrame.from_dict(data_dict, orient='index', columns=['Data'])
                    df.reset_index(inplace=True)
                    df.rename(columns={'index': 'Title'}, inplace=True)

                    pivot_df = df.pivot_table(index=None, columns='Title', values='Data', aggfunc='first')

                    # Remove the name of the index axis and set it to 0
                    pivot_df.columns.name = None
                    pivot_df = pivot_df.reset_index(drop=True)
                    pivot_df.columns = [col.lower().replace(' ', '_') for col in pivot_df.columns]

                    pivot_df.to_csv(f"../output/{key}_meta.csv", index=False)
                    print(f"{key} methodology and metadata file saved in output folder.")

        except Exception as e:
            print(f"An error occurred: {str(e)}")