import numpy as np
import pandas as pd
from iot_generator import sensor_producer as sp

def test_generate_synthetic_data():
    np.random.seed(123)
    df = sp.generate_synthetic_data(n_samples=5)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (5, len(sp.SENSOR_RANGES))
    assert set(df.columns) == set(sp.SENSOR_RANGES.keys())

    for sensor, (mn, mx) in sp.SENSOR_RANGES.items():
        col = df[sensor]
        assert np.issubdtype(col.dtype, np.number)
        if mn == mx:
            assert (col == mn).all()
        else:
            assert (col >= mn).all()
            assert (col <= mx).all()
