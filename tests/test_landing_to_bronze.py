import os
import pytest
from pyspark.sql import SparkSession
from cmapss_pipeline.medallion.landing_to_bronze import BronzeProcessor


@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .appName("bronze-tests")
             .master("local[1]")
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture()
def tmp_paths(tmp_path):
    landing = tmp_path / "landing"
    bronze = tmp_path / "bronze"
    landing.mkdir()
    bronze.mkdir()
    return str(landing), str(bronze)


def _write_sample_train_test(landing_dir: str, name: str):
    row1 = [1, 1] + [0, 0, 0] + [float(i) for i in range(1, 22)]
    row2 = [1, 2] + [0, 0, 0] + [float(i) for i in range(1, 22)]
    def to_line(vals): return " ".join(str(v) for v in vals)

    with open(os.path.join(landing_dir, name), "w", encoding="utf-8") as f:
        f.write(to_line(row1) + "\n")
        f.write(to_line(row2) + "\n")


def _write_sample_rul(landing_dir: str):
    with open(os.path.join(landing_dir, "RUL_FD001.txt"), "w", encoding="utf-8") as f:
        f.write("130\n")
        f.write("129\n")

def test_bronze_run_all_creates_parquets(spark, tmp_paths):
    landing, bronze = tmp_paths

    _write_sample_train_test(landing, "train_FD001.txt")
    _write_sample_train_test(landing, "test_FD001.txt")
    _write_sample_rul(landing)

    bp = BronzeProcessor(spark, landing, bronze)
    bp.run_all()

    assert os.path.isdir(os.path.join(bronze, "train"))
    assert os.path.isdir(os.path.join(bronze, "test"))
    assert os.path.isdir(os.path.join(bronze, "RUL"))

    df_train = spark.read.parquet(os.path.join(bronze, "train"))
    df_test = spark.read.parquet(os.path.join(bronze, "test"))
    df_rul = spark.read.parquet(os.path.join(bronze, "RUL"))

    expected_cols = (
        ["engine_id", "cycle", "op_setting_1", "op_setting_2", "op_setting_3"]
        + [f"sensor_{i}" for i in range(1, 22)]
    )
    assert df_train.columns == expected_cols
    assert df_test.columns == expected_cols
    assert df_train.count() > 0
    assert df_test.count() > 0

    assert df_rul.columns == ["RUL"]
    assert df_rul.count() > 0
