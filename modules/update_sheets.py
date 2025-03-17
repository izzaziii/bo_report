import os.path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
import pandas as pd
from typing import Optional, List
import logging
import numpy as np


# Add this function to convert numpy types to Python native types
def convert_to_serializable(val):
    """Convert numpy data types to JSON serializable Python types."""
    if isinstance(val, (np.integer, np.int64)):
        return int(val)
    elif isinstance(val, (np.floating, np.float64)):
        return float(val)
    elif isinstance(val, np.ndarray):
        return val.tolist()
    elif isinstance(val, np.bool_):
        return bool(val)
    elif pd.isna(val):  # Handle NaN, None, etc.
        return ""
    return val


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_credentials(path_to_credentials: str) -> Optional[Credentials]:
    """Get user credentials for Google Sheets API."""
    creds = None
    logging.info("Getting credentials...")
    if os.path.exists("token.json"):
        logging.info("Reading credentials from token.json")
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        logging.info("Credentials not valid, refreshing...")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                path_to_credentials,
                SCOPES,
            )
            creds = flow.run_local_server(port=0)
        save_credentials(creds)
    logging.info("Credentials obtained successfully")
    return creds


def save_credentials(creds: Credentials) -> None:
    """Save user credentials to token.json file."""
    with open("token.json", "w") as token:
        logging.info("Saving credentials to token.json")
        token.write(creds.to_json())


def generate_body(values: List[List[str]]):
    """Generate dictionary body to push for write_data_to_sheets."""
    logging.info("Generating body...")
    return {"values": values}


def generate_values_from_df(df: pd.DataFrame) -> list:
    """
    Generates list of lists for write_data_to_sheets() function.
    Takes in a dataframe and converts all values to JSON serializable types.
    """
    logging.info("Generating values from dataframe")
    headers = df.columns.tolist()  # get headers of df

    # Convert the DataFrame to a list of lists and ensure all values are serializable
    values = []
    for _, row in df.iterrows():
        # Apply the conversion function to each value in the row
        serializable_row = [convert_to_serializable(val) for val in row]
        values.append(serializable_row)

    # Insert the headers from the df
    values.insert(0, headers)
    logging.info("Values generated successfully")
    return values


def write_data_to_sheets(
    creds: Credentials, spreadsheet_id: str, range_name: str, body: dict
):
    """Write data to Google Sheets using the provided credentials and rows."""
    logging.info("Writing data to Google Sheets...")
    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="USER_ENTERED",
                body=body,
            )
            .execute()
        )
        logging.info("Data written successfully!")
        return result
    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        return error


def update_googlesheets_from_csv(path_to_csv: str):
    creds: Credentials = get_credentials("credentials.json")
    df: pd.DataFrame = pd.read_csv(path_to_csv)
    values = generate_values_from_df(df)
    body = generate_body(values)
    spreadsheet_id = "1HPMS4SFhnGuntA9Pmu4lnBCj8otF3VVwZMIxsJC7tgI"
    range_name = "Sheet1!A1"
    if creds:
        result = write_data_to_sheets(creds, spreadsheet_id, range_name, body)
        print(f"{result.get('updatedCells')} cells updated")
        return result


if __name__ == "__main__":
    # Set up basic logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    update_googlesheets_from_csv(
        r"C:\Users\izzaz\Documents\2 Areas\GitHub\bo_report\data\processed_bo.csv"
    )
