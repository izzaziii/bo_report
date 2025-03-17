import pandas as pd
import logging
import warnings

warnings.filterwarnings("ignore", module="openpyxl")
logging.basicConfig(level=logging.INFO)


def read_bo(
    file_path="Z:\\FUNNEL with PROBABILITY TRACKING_Teefa.xlsx",
) -> pd.DataFrame:
    """
    Imports the BO file from the given path and returns a DataFrame.

    Defaults to "Z:\\FUNNEL with PROBABILITY TRACKING_Teefa.xlsx" if no path is given.
    """
    try:
        logging.info("Reading file...")
        df = pd.read_excel(file_path, engine="openpyxl")
        logging.info("File read successfully")
        return df
    except Exception as e:
        logging.error("Error reading file, maybe check VPN. Error: ", e)


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes BO Report.
    """
    try:
        logging.info("Processing data...")
        processed_df = (
            df.loc[
                (df["Funn Status"] != "Lost")
                & (df[" Channel"].isin(["ONLINE", "INSIDE SALES", "DEALER"]))
            ]
            .astype({"Probability 90% Date": "datetime64[ns]"})
            .assign(Nationality=df["Nationality"].str.title())
            .dropna(subset="Probability 90% Date")
            .set_index("Probability 90% Date")
            .groupby(
                [
                    " Channel",
                    "Blk State",
                    "Funn Monthcontractperiod",
                    "Funnel Bandwidth",
                    "Nationality",
                ]
            )
            .resample("d")["Funnel SO No"]
            .count()
            .reset_index()
            .query("`Funnel SO No` > 0")
            .sort_values("Probability 90% Date", ascending=False)
        )
        logging.info("Data processed successfully")
        return processed_df.sort_values("Probability 90% Date")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return  # Just break when an error occurs


def export_to_csv(
    df: pd.DataFrame,
    file_path: str = r"C:\Users\izzaz\Documents\2 Areas\GitHub\bo_report\data\processed_bo.csv",
) -> None:
    """
    Exports the processed DataFrame to a CSV file.
    """
    try:
        logging.info("Exporting to CSV...")
        df.to_csv(file_path, index=False)
        logging.info("Exported successfully")
    except Exception as e:
        logging.error("Error exporting to CSV: ", e)


if __name__ == "__main__":
    df: pd.DataFrame = read_bo()
    processed_df: pd.DataFrame = process_data(df)
    export_to_csv(processed_df)
