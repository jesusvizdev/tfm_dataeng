import os
import pytest
from pyspark.sql import SparkSession
from cmapss_pipeline.medallion.landing_to_bronze import BronzeProcessor
from cmapss_pipeline.medallion.bronze_to_silver import SilverProcessor
from cmapss_pipeline.medallion.silver_to_gold import GoldProcessor

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .appName("gold-tests")
             .master("local[1]")
             .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture()
def tmp_paths(tmp_path):
    landing = tmp_path / "landing"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    landing.mkdir(); bronze.mkdir(); silver.mkdir(); gold.mkdir()
    return str(landing), str(bronze), str(silver), str(gold)

def _write_train(landing_dir: str):
    r1 = [1, 1] + [0, 0, 0] + [float(i)     for i in range(1, 22)]
    r2 = [1, 2] + [0, 0, 0] + [float(i + 1) for i in range(1, 22)]
    r3 = [1, 3] + [0, 0, 0] + [float(i + 2) for i in range(1, 22)]
    with open(os.path.join(landing_dir, "train_FD001.txt"), "w", encoding="utf-8") as f:
        f.write(" ".join(map(str, r1)) + "\n")
        f.write(" ".join(map(str, r2)) + "\n")
        f.write(" ".join(map(str, r3)) + "\n")

def _write_test(landing_dir: str):
    r1 = [1, 1] + [0, 0, 0] + [float(i)     for i in range(1, 22)]
    r2 = [1, 2] + [0, 0, 0] + [float(i + 1) for i in range(1, 22)]
    r3 = [1, 3] + [0, 0, 0] + [float(i + 2) for i in range(1, 22)]
    with open(os.path.join(landing_dir, "test_FD001.txt"), "w", encoding="utf-8") as f:
        f.write(" ".join(map(str, r1)) + "\n")
        f.write(" ".join(map(str, r2)) + "\n")
        f.write(" ".join(map(str, r3)) + "\n")

def _write_rul(landing_dir: str):
    with open(os.path.join(landing_dir, "RUL_FD001.txt"), "w", encoding="utf-8") as f:
        f.write("99\n")
        f.write("88\n")

def test_gold_run_all_selects_features(spark, tmp_paths):
    landing, bronze, silver, gold = tmp_paths
    _write_train(landing)
    _write_test(landing)
    _write_rul(landing)

    BronzeProcessor(spark, landing, bronze).run_all()
    SilverProcessor(spark, bronze, silver).run_all()
    GoldProcessor(spark, silver, gold).run_all()

    assert os.path.isdir(os.path.join(gold, "train"))
    assert os.path.isdir(os.path.join(gold, "test"))

    df_train = spark.read.parquet(os.path.join(gold, "train"))
    df_test = spark.read.parquet(os.path.join(gold, "test"))

    expected_cols = [f"sensor_{i}" for i in range(1, 22)] + ["RUL"]
    assert df_train.columns == expected_cols
    assert df_test.columns == expected_cols
    assert df_train.count() > 0
    assert df_test.count() > 0
