"""This script will scrape Wikipedia for ISO 3166-1 and ISO 3166-2 data.
"""

import argparse
import json
import logging

import pandas as pd
import requests


LOGGER = logging.getLogger('iso_3166_data_scrape')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
logging.captureWarnings(True)
LOGGER.setLevel(logging.INFO)


def get_dataframes_from_url(url):
    """Gets the html from the URL and uses Pandas to retrieve the tables from the html as DataFrames

    Args:
        url (str): a url string

    Returns:
        list[pandas.DataFrame]: a list of DataFrames each containing the data from an html table
    """
    response = requests.get(url)
    dataframes = pd.read_html(response.text)
    LOGGER.debug(f"Retrieved {len(dataframes)} DataFrames from {url}")
    return dataframes


def filter_out_dataframes_with_specific_iloc_values(dataframes, iloc_x, iloc_y, value):
    """Filter out any DataFrame containing a certain value at a certain location within the 
    DataFrame and return the rest of the list.

    Args:
        dataframes (list[pandas.DataFrame]): a list of Pandas DataFrames
        iloc_x (int): first coordinate to supply to iloc
        iloc_y (int): second coordinate to supply to iloc
        value (obj): value to check for at the location specified to iloc

    Returns:
        list[pandas.DataFrame]: a list of DataFrames that did not match the criteria speciifed or
            an empty list if all DataFrames filtered out
    """
    included_dataframes = []
    for df in dataframes:
        try:
            if df.iloc[iloc_x, iloc_y] == value:
                LOGGER.debug(f"Filtered out a DataFrame containing '{value}'")
                continue
            else:
                included_dataframes.append(df)
        except IndexError as e:
            LOGGER.debug(f"Error while evaluating DataFrame for exclusion: {e}")
            included_dataframes.append(df)
    return included_dataframes


def get_first_dataframe_with_matching_column_name_from_list(dataframes, column_name):
    """Filter a list of DataFrames down to a single match (or None) by looking comparing column
    names against a given string. Returns the first DataFrame with a column name that exactly 
    matches the given string. If there are no exact matches, it will return the first DataFrame 
    that has a column name that contains the given string. Returns None if no matches found.

    The comparison is case insensitive.

    Args:
        dataframes (list[pandas.DataFrame]): a list of Pandas DataFrames
        column_name (str): a string to use to match against column names within each DataFrame
    
    Returns:
        pandas.DataFrame or None: returns the first matching DataFrame or None if no exact or fuzzy
            matches
    """
    for df in dataframes:
        for column in df.columns.to_list():
            if column_name.lower() == str(column).lower():
                LOGGER.debug(f"DataFrame found with exact match of '{column_name}'")
                return df
    for df in dataframes:
        for column in df.columns.to_list():
            if column_name.lower() in str(column).lower():
                LOGGER.debug(f"DataFrame found with column name containing of '{column_name}'")
                return df
    return None


def string_is_name_of_at_least_one_column(df, string):
    """Performs a case-insensitive check to see if given string is an exact column name of the
    given DataFrame.

    Args:
        df (pandas.DataFrame): a Pandas DataFrame
        string (str): a string that is compared against the column names of the DataFrame

    Returns:
        boolean
    """
    columns = [c.lower() for c in df.columns.to_list()]
    return True if string.lower() in columns else False


def get_first_column_name_matching_string(df, string):
    """Returns the name of the first column that matches the given string in a case-insensitive
    search
    
    Args:
        df (pandas.DataFrame): a Pandas DataFrame
        string (str): a string that is compared against the column names of the DataFrame
    
    Returns:
        str or None: returns the name of the column if one is found otherwise returns None
    """
    for column in df.columns.to_list():
        if string.lower() == str(column).lower():
            return column
    return None


def get_column_names_containing_string(df, string):
    """Returns a list of all column names that contain the given string (case-insensitve)
    
    Args:
        df (pandas.DataFrame): a Pandas DataFrame
        string (str): a string that is compared against the column names of the DataFrame

    Returns:
        list: returns a list with matching column names
    """
    matching_column_names = []
    for column in df.columns.to_list():
        if string.lower() in str(column).lower():
            matching_column_names.append(column)
    return matching_column_names


def get_best_matching_column_name_compared_to_string(df, string):
    """Attempt to find the column name that best matches a given string or return None if no
    column satisfies matching criteria.

    Args:
        df (pandas.DataFrame): a Pandas DataFrame
        string (str): a 

    Returns:
        obj or None: the column name value or None
    """
    # case-insensitive exact match
    if string_is_name_of_at_least_one_column(df, string):
        LOGGER.debug(f"Column identified with exact match of '{string}'")
        return get_first_column_name_matching_string(df, string)
    # just go with first fuzzy match
    column_names_containing_string = get_column_names_containing_string(df, string)
    if column_names_containing_string:
        LOGGER.debug(f"Column identified with fuzzy match of '{string}'. All possible matches: {column_names_containing_string}")
        return column_names_containing_string[0]
    return None


def has_multi_index_columns(df):
    """Check to see if a DataFrame has mult-index column names

    Args:
        df (pandas.DataFrame): a Pandas DataFrame

    Returns:
        boolean
    """
    for column in df.columns.to_list():
        if isinstance(column, tuple):
            return True
    return False


def collapse_multi_index_columns_if_exist(df):
    """If the passed DataFrame has multi-index column names, then collapse them into a single level.
    This will concatenate the two levels together unless the data from each level is identical in 
    which case it is deduplicated. If the DataFrame is not mult-indexed then no operation is 
    performed on it.

    Args:
        df (pandas.DataFrame): a Pandas DataFrame

    Returns:
        pandas.DataFrame: the updated DataFrame or the original DataFrame if not multi-indexed
    """
    if has_multi_index_columns(df):
        df.columns = [f"{i}-{j}" if j != '' and i != j else f"{i}" for i, j in df.columns]
    return df


def string_has_bracketed_text(string):
    """Check if a string has both an open and close bracket. Does not check if brackets exist in 
    pairs or in logical order.

    Args:
        string (str)

    Returns:
        boolean
    """
    return bool('[' in string and ']' in string)


def remove_first_instance_of_bracketed_text_from_string(string):
    """Removes the brackets and all text in between. If brackets are not in logical order this will
    fail.

    Args:
        string (str)

    Returns:
        str: the string with the first instance of bracketed text removed

    Raises:
        IndexError: raises an error if the close bracket occurs before the open bracket
    """
    open_bracket_index = string.index('[')
    close_bracket_index = string.index(']')
    if close_bracket_index < open_bracket_index:
        raise IndexError(f"The next close bracket at index {close_bracket_index} came before the next open bracket at index {open_bracket_index}")
    return f"{string[:open_bracket_index]}{string[close_bracket_index + 1:]}"


def remove_bracketed_text_from_string(string):
    """Loops while string contains brackets and removes first instance of bracketed text one at a 
    time until they are all removed. If brackets are not pai
    """
    more_bracketed_text_to_remove = True
    while more_bracketed_text_to_remove:
        if not string_has_bracketed_text(string):
            more_bracketed_text_to_remove = False
        else:
            try:
                string = remove_first_instance_of_bracketed_text_from_string(string)
            except IndexError as e:
                LOGGER.warning(f"Failed to remove all bracketed text from string '{string}': {e}")
                more_bracketed_text_to_remove = False
    return string


def clean_column_name(value):
    """Convert the value to a str, replace concept separating characters with underscores, remove
    parenthesis, remove bracketed text.

    Args:
        value (obj): the name of the column to be cleaned

    Return
        str: a string that is in snake_case
    """
    return remove_bracketed_text_from_string(
        str(value).lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace(')(', '_').replace('(', '').replace(')', '')
    )


def drop_columns_if_columns_exist(df, columns_to_drop):
    """Attempt to drop columns but gracefully handle errors if column doesn't exist in DataFrame
    
    Args:
        df (pandas.DataFrame): a Pandas DataFrame
        columns_to_drop (list): a list with column names to attempt to drop

    Returns:
        pandas.DataFrame: returns the updated DataFrame
    """
    for column in columns_to_drop:
        try:
            df = df.drop(columns=[column])
            LOGGER.debug(f"Dropped column '{column}'")
        except KeyError as e:
            LOGGER.debug(f"Could not drop column '{column}'")
            continue
    return df


def add_code_column_to_dataframe(df, primary_key_name):
    """Add a column named 'code' and set the data to the primary key column data. If no match can
    be identified then set the data to the data from the first column.

    Args:
        df (pandas.DataFrame): a Pandas DataFrame
        primary_key_name (str): the column name of the DataFrame that helps to identify it as 
            appropriate table from the page

    Returns:
        pandas.DataFrame: returns the updated DataFrame
    """
    if primary_key_name in df.columns.to_list():
        data_column_name = primary_key_name
    elif clean_column_name(primary_key_name) in df.columns.to_list():
        data_column_name = clean_column_name(primary_key_name)
    else:
        data_column_name = df.columns.to_list()[0]
    df['code'] = df[data_column_name]
    return df


def apply_all_updates_to_dataframe_columns(df, handling_map={}):
    """Sort data based on primary key column, drop columns, rename columns, flatten multi-index 
    columns, clean column names, rename columns again, and drop columns again.

    Args:
        df (pandas.DataFrame): a Pandas DataFrame
        handling_map (dict): a dictionary with zero or more of the following keys:
            primary_key_name (str): the column name of the DataFrame that helps to identify it as 
                appropriate table from the page
            rename_columns (dict): the original column name as keys and the new column names as
                values
            drop_columns (list): columns to be dropped
    
    Returns:
        pandas.DataFrame: the updated DataFrame
    """
    LOGGER.debug(f"columns: {df.columns.to_list()}")
    try:
        df = df.sort_values(handling_map.get('primary_key_name'), axis=0)
    except ValueError as e:
        LOGGER.warning(f"Failed to sort table due to ValueError: {e}")
    if handling_map.get('drop_columns'):
        df = drop_columns_if_columns_exist(df, handling_map.get('drop_columns'))
    if handling_map.get('rename_columns'):
        df = df.rename(columns=handling_map.get('rename_columns'))
    df = collapse_multi_index_columns_if_exist(df)
    clean_column_map = {c: clean_column_name(c) for c in df.columns.to_list()}
    df = df.rename(columns=clean_column_map)
    if handling_map.get('rename_columns'):
        df = df.rename(columns=handling_map.get('rename_columns'))
    if 'code' not in df.columns.to_list():
        df = add_code_column_to_dataframe(df, primary_key_name=handling_map.get('primary_key_name'))
    if handling_map.get('drop_columns'):
        df = drop_columns_if_columns_exist(df, handling_map.get('drop_columns'))
    return df


def fix_namibia_country_code(df):
    """When the HTML table for the ISO 3166-1 data is parsed, the country code for Namibia 'NA'
    gets parsed into NaN. This function finds that row and corrects that data point.
    
    Args:
        df (pandas.DataFrame): a Pandas DataFrame containing the ISO 3166-1 with a column named 
            'code' and a column named 'name_en'

    Returns:
        pandas.DataFrame: returns the updated DataFrame
    """
    df['code'] = df.apply(lambda x: 'NA' if x['name_en'] == 'Namibia' else x['code'], axis=1)
    return df


def convert_dataframe_to_dict(df):
    """Transform the DataFrame to a dictionary with keys of the values from the primary key column
    and values of dictionarys of the row data.

    Args:
        df (pandas.DataFrame): a Pandas DataFrame
    
    Returns:
        dict
    """
    data_dict = {i['code']: i for i in df.to_dict(orient='records')}
    return data_dict


URL = 'https://en.wikipedia.org/wiki/ISO_3166-1'
PRIMARY_TABLE_HANDLING_MAP = {
    'primary_key_name': 'Alpha-2 code',
    'rename_columns': {
        'English short name (using title case)': 'name_en',
        'Alpha-2 code': 'code',
        'Link to ISO 3166-2 subdivision codes': 'iso_3166_2_name',
    }
}
DEFAULT_HANDLING_MAP = {
    'primary_key_name': 'Code'
}
COUNTRY_SPECIFIC_HANDLING_MAP = {
    # add country codes as keys and subdivision caption as value
    'CN': {
        'rename_columns': {
            'Subdivision name[note 1](National 1958 = ISO 7098:2015 = UN III/8 1977)': 'subdivision_name'
        }
    }
}
DATA_TO_EXCLUDE_TABLES_USING_ILOC_COMPARISON = [
    [0, 1, 'List of ISO 3166 country codes'], # Table in the footer of most ISO 3166-2 related pages
]


def construct_country_specific_handling_map(country_code):
    handling_map = DEFAULT_HANDLING_MAP.copy()
    country_map = COUNTRY_SPECIFIC_HANDLING_MAP.get(country_code)
    if country_map:
        handling_map.update(country_map)
    return handling_map


def get_dataframe_from_url(url, primary_key_name):
    """Convenience function to process several functions in succession. Gets all tables as Pandas
    DataFrames from the url, filters out DataFrames based on criteria from 
    DATA_TO_EXCLUDE_TABLES_USING_ILOC_COMPARISON, and then selects the first matching DataFrame if
    any match.

    Args:
        url (str): a url string
        primary_key_name (str): the column name of the DataFrame that helps to identify it as 
            appropriate table from the page

    Returns:
        pandas.DataFrame or None: returns the table from the url that best matches given the 
            primary_key_name or returns None if there are no matching tables
    """
    dfs = get_dataframes_from_url(url)
    for filter_criterion in DATA_TO_EXCLUDE_TABLES_USING_ILOC_COMPARISON:
        dfs = filter_out_dataframes_with_specific_iloc_values(
            dfs,
            filter_criterion[0],
            filter_criterion[1],
            filter_criterion[2],
        )
    df = get_first_dataframe_with_matching_column_name_from_list(dfs, primary_key_name)
    return df


def main(args):

    # TODO: finish this to enable the user to set the log level
    if args.debug:
        LOGGER.setLevel(logging.DEBUG)

    # get main ISO 3166 data
    df = get_dataframe_from_url(URL, PRIMARY_TABLE_HANDLING_MAP['primary_key_name'])
    df = apply_all_updates_to_dataframe_columns(df, handling_map=PRIMARY_TABLE_HANDLING_MAP)
    df = fix_namibia_country_code(df)
    data_dict = convert_dataframe_to_dict(df)

    # retrieve subdivision data
    for country_code, country_data in data_dict.items():
        LOGGER.info(f"Retrieving subdivision data for {country_code}")
        handling_map = construct_country_specific_handling_map(country_code)
        url = f"https://en.wikipedia.org/wiki/ISO_3166-2:{country_code}"
        cdf = get_dataframe_from_url(url, handling_map['primary_key_name'])
        if cdf is None:
            error_message = f"Did not find table with column name '{handling_map['primary_key_name']}' at {url}"
            data_dict[country_code]['subdivision_data_retrieval_errors'] = []
            data_dict[country_code]['subdivision_data_retrieval_errors'].append(error_message)
            LOGGER.error(f"Failed: {error_message}\n")
            continue
        cdf = apply_all_updates_to_dataframe_columns(cdf, handling_map=handling_map)
        data_dict[country_code]['subdivision_data'] = convert_dataframe_to_dict(cdf)
        LOGGER.info(f"Successfully downloaded subdivision data for {country_code}\n")

    LOGGER.debug(f"data:\n{data_dict}")
    
    LOGGER.info(f"Done scraping data for all countries\n\n")
    
    LOGGER.info(f"Writing file to {args.output_file_path}")
    with open(args.output_file_path, 'w') as f:
        json.dump(data_dict, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''This script will scrape Wikipedia for ISO 3166 data and save it to a JSON
        file.
        '''
    )
    parser.add_argument(
        '--debug',
        default=False,
        action='store_true',
        help='''Optionally set the logging level to debug'''
    )
    parser.add_argument(
        '--output-file-path',
        default='./data/iso_3166.json',
        type=str,
        help='''Optionally set the file path for the JSON generated by this script. By default this
        will be set to ./data/iso_3166.json.
        '''
    )
    args = parser.parse_args()
    main(args)
