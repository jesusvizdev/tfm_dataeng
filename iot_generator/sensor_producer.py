import pandas as pd
import numpy as np
import time

SENSOR_RANGES = {
    "sensor_1": (518.67, 518.67),
    "sensor_2": (-3.101768, 3.237859),
    "sensor_3": (-3.503853, 2.777038),
    "sensor_4": (-2.726939, 2.713788),
    "sensor_5": (14.62, 14.62),
    "sensor_6": (-7.057648, 0.141683),
    "sensor_7": (-2.810643, 2.793327),
    "sensor_8": (-2.911155, 2.864571),
    "sensor_9": (-1.843673, 4.065846),
    "sensor_10": (1.3, 1.3),
    "sensor_11": (-2.774953, 2.691300),
    "sensor_12": (-2.756972, 3.181453),
    "sensor_13": (-2.866516, 3.112458),
    "sensor_14": (-1.847969, 4.022048),
    "sensor_15": (-2.915475, 2.646359),
    "sensor_16": (0.03, 0.03),
    "sensor_17": (-2.718733, 2.446631),
    "sensor_18": (2388.0, 2388.0),
    "sensor_19": (100.0, 100.0),
    "sensor_20": (-2.800990, 3.285005),
    "sensor_21": (-3.272978, 3.253595),
}

def generate_synthetic_data(n_samples: int = 1) -> pd.DataFrame:
    synthetic_data = {}
    for sensor, (min_val, max_val) in SENSOR_RANGES.items():
        if min_val == max_val:
            synthetic_data[sensor] = [min_val] * n_samples
        else:
            synthetic_data[sensor] = np.random.uniform(min_val, max_val, n_samples)
    return pd.DataFrame(synthetic_data)

