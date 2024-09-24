import pandas as pd
import os
import sys
from datetime import datetime, timedelta

INPUT_DIR = 'input'
OUTPUT_DIR = 'output'


def agg_logs(target_date):
    target_date = datetime.strptime(target_date, '%Y-%m-%d')
    start_date = target_date - timedelta(days=7)

    all_data = []
    for i in range(7):
        day = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
        filepath = os.path.join(INPUT_DIR, f"{day}.csv")
        if os.path.exists(filepath):
            daily_data = pd.read_csv(filepath, header=None, names=['email', 'action', 'timestamp'])
            all_data.append(daily_data)

    if not all_data:
        return

    df = pd.concat(all_data, ignore_index=True)
    res = df.groupby(['email', 'action']).size().unstack(fill_value=0).reset_index()
    res.columns = ['email', 'create_count', 'read_count', 'update_count', 'delete_count']

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"{target_date.strftime('%Y-%m-%d')}.csv")
    res.to_csv(output_file, index=False)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)

    date = sys.argv[1]
    agg_logs(date)
