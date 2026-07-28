from pathlib import Path
from datetime import datetime
import pandas as pd


def load_current_month_data():
    """
    Automatically loads the current month's MTD CSV.

    Example:
    monthly_data/2026/MTD_Jul_26.csv
    """

    today = datetime.now()

    year_folder = str(today.year)

    file_name = today.strftime("MTD_%b_%y.csv")

    csv_path = (
        Path("monthly_data")
        / year_folder
        / file_name
    )

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Monthly file not found:\n{csv_path}"
        )

    print(f"📂 Loading : {csv_path}")

    df = pd.read_csv(
        csv_path,
        low_memory=False
    )

    return df
