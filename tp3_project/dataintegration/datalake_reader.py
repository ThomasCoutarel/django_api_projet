# dataintegration/datalake_reader.py
import os
import pandas as pd
from datetime import datetime

import pandas as pd
import numpy as np
import os

def read_messages_from_datalake(topic_name, base_path="C:/Users/couta/Python project/data/data_lake", max_files=5):
    topic_path = os.path.join(base_path, topic_name)
    if not os.path.exists(topic_path):
        return []

    messages = []
    # On trie les dossiers par date (du plus récent au plus ancien)
    date_dirs = sorted(
        [d for d in os.listdir(topic_path) if d.startswith("date=")],
        reverse=True
    )

    for date_dir in date_dirs[:max_files]:
        full_dir = os.path.join(topic_path, date_dir)
        if not os.path.isdir(full_dir):
            continue

        for file in os.listdir(full_dir):
            if file.endswith(".csv"):
                file_path = os.path.join(full_dir, file)
                try:
                    df = pd.read_csv(file_path)

                    # Remplacer NaN, inf et -inf par None
                    df.replace([np.nan, np.inf, -np.inf], None, inplace=True)

                    # Et s'assurer qu'on convertit bien vers un format JSON-compatible
                    records = df.to_dict(orient="records")
                    messages.extend(records)

                except Exception as e:
                    print(f"Erreur lors de la lecture de {file_path} : {e}")
                    continue

    return messages
