import os
import re
from typing import Optional

from pyspark.sql import (
    SparkSession,
    DataFrame
)

from library.utils import (
    file_exists,
    path_exists
)


def load_csv_to_delta(csv_path: str, delta_table_path: Optional[str] = None) -> None:
    """
    Load a CSV file into a Delta table.

    Parameters:
    -----------
        csv_path (str): The path to the CSV file.
        delta_table_path (str, optional): The path to the Delta table. Defaults to None.

    Returns:
    --------
        None
    """
    # Check if the CSV file exists
    if not file_exists:
        raise FileNotFoundError(f"File not found: {csv_path}")

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("CSV to Delta") \
        .getOrCreate()

    # Load the CSV file into a DataFrame
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load(csv_path)

    # Rename columns with invalid characters
    df = rename_invalid_columns(df)

    # Set the default delta table path if not provided
    if delta_table_path is None:
        delta_table_path = f'./temp/{csv_path.split("/")[-1].split(".")[0]}'

    # Write the DataFrame to a Delta table
    df.write.format("delta") \
        .mode("overwrite") \
        .save(delta_table_path)


def load_jsonl_to_delta(json_path: str, delta_table_path: Optional[str] = None) -> None:
    """
    Load a JSONL file into a Delta table.

    Parameters:
    -----------
        json_path (str): The path to the JSONL file.
        delta_table_path (str, optional): The path to the Delta table. Defaults to None.

    Returns:
    --------
        None
    """
    if not file_exists:
        raise FileNotFoundError(f"File not found: {json_path}")

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("JSONL to Delta") \
        .getOrCreate()

    # Load the JSONL file into a DataFrame
    df = spark.read.format("json") \
        .load(json_path)

    # Rename columns with invalid characters
    df = rename_invalid_columns(df)

    # Set the default delta table path if not provided
    if delta_table_path is None:
        delta_table_path = f'./temp/{json_path.split("/")[-1].split(".")[0]}'

    # Write the DataFrame to a Delta table
    df.write.format("delta") \
        .mode("overwrite") \
        .save(delta_table_path)


def rename_invalid_columns(df: DataFrame) -> DataFrame:
    """
    Rename columns with invalid characters ' ,;{}()\n\t=' in their names.

    Parameters:
    -----------
        df (DataFrame): The DataFrame to rename columns for.

    Returns:
    --------
        DataFrame: The DataFrame with renamed columns.
    """
    for column in df.columns:
        new_column = re.sub(r'[ ,;{}\(\)\n\t=]', '_', column)
        df = df.withColumnRenamed(column, new_column)
    return df


def load_export(source_path: str, target_path: Optional[str] = None) -> None:
    """
    Iterate all files from a given source_path,
    for CSV and Json/Jsonl files load them to delta format,
    if they match the below naming patterns.

    |--------------------------------------|---------------------------|
    |  Possible table names in filename:   |  Target table names:      |
    |--------------------------------------|---------------------------|
    |  profile_cards (jsonl)               |  lever_profile_cards      |
    |  candidates.presence (csv)           |  candidate_presence       |
    |  interviews.interviewEvents (csv)    |  interview_events         |
    |  feedback.interviewCalibration (csv) |  interview_feedback       |
    |  non-consideration.feedback (csv)    |  nogo_feedback            |
    |  jobPostings (csv)                   |  lever_jobs               |
    |--------------------------------------|---------------------------|

    Parameters:
    -----------
        source_path (str): The path to the source directory containing the files to be loaded.
        target_path (str, optional):
            The path to the target directory where the files will be loaded in delta format.
            If not provided, a new directory called temp will be created at the source directory.

    Returns:
    --------
        None
    """
    # Check if the source path exists
    if not path_exists(source_path):
        raise FileNotFoundError(f"Path not found: {source_path}")

    # Set the default target path if not provided
    if target_path is None:
        target_path = f"{source_path}/imported"

    spark = SparkSession.builder \
        .appName("Load Export") \
        .getOrCreate()

    files = os.listdir(source_path)

    for file_name in files:
        extension = file_name.split(".")[-1]

        # Load the file to a Delta table based on the file name
        match extension:
            case "jsonl":
                if "profile_cards" in file_name:
                    load_jsonl_to_delta(f"{source_path}/{file_name}",
                                        f"{target_path}/lever_profile_cards")
            case "csv":
                if "candidates.presence" in file_name:
                    load_csv_to_delta(f"{source_path}/{file_name}",
                                      f"{target_path}/leve_candidate_presence")
                elif "interviews.interviewEvents" in file_name:
                    load_csv_to_delta(f"{source_path}/{file_name}",
                                      f"{target_path}/lever_interview_events")
                elif "feedback.interviewCalibration" in file_name:
                    load_csv_to_delta(f"{source_path}/{file_name}",
                                      f"{target_path}/lever_interview_feedback")
                elif "non-consideration.feedback" in file_name:
                    load_csv_to_delta(f"{source_path}/{file_name}",
                                      f"{target_path}/lever_nogo_feedback")
                elif "jobPostings" in file_name:
                    load_csv_to_delta(f"{source_path}/{file_name}",
                                      f"{target_path}/lever_jobs")
            case _:
                print(f"Unsupported file format: {extension}")
