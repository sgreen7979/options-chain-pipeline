#!/usr/bin/env python3
import datetime as dt
import gzip
import os
import time
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

import pandas as pd
import pyodbc

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col
from pyspark.sql.functions import exp
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_json
from pyspark.sql.functions import least
from pyspark.sql.functions import lit
from pyspark.sql.functions import log1p
from pyspark.sql.functions import pandas_udf  # type: ignore
from pyspark.sql.functions import round
from pyspark.sql.functions import struct
from pyspark.sql.functions import to_json
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.functions import when
from pyspark.sql.types import DecimalType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType

from options_chain_pipeline.lib.market_hours import functions as mh
from options_chain_pipeline.lib import get_expiration_dates
from options_chain_pipeline.lib import get_logger
from options_chain_pipeline.lib import ImpliedVolatilityEngine
from options_chain_pipeline.lib import MarketCalendar
from options_chain_pipeline.lib import MyListener
from options_chain_pipeline.lib import MSSQLClient
from options_chain_pipeline.lib import MSSQLConfig
from options_chain_pipeline.lib import OptionsChainSchema
from options_chain_pipeline.lib import SvenssonYieldTermStructure

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Logging setup
LOG_LEVEL = "DEBUG"
logger = get_logger(
    "options_chain_pipline.processor" if __name__ == "__main__" else __name__,
    level=LOG_LEVEL,
    fh=True,
    fh_level="DEBUG",
    fh_fmt="%(asctime)s %(levelname)s %(name)s %(pathname)s %(lineno)d %(message)s",
    fh_type="RotatingFileHandler",
    fh_type_kwargs={"maxBytes": 1_048_576, "backupCount": 500_000},  # 1MB
)

# Kafka configuration
KAFKA_BROKER = "localhost:9093"
TODAY = dt.date.today()
CURRENT_DATE = TODAY.strftime('%Y%m%d')
KAFKA_TOPIC = f"option_chain_topic_{CURRENT_DATE}"

# Hadoop paths
HADOOP_HOME = os.environ["HADOOP_HOME"]
CHECKPOINT_DIR = f"file:///{HADOOP_HOME}/data/hdfs/checkpoints/{CURRENT_DATE}"
ERROR_PATH = f"file:///{HADOOP_HOME}/data/hdfs/sparkerrors/{CURRENT_DATE}"

# Spark paths
SPARK_HOME = os.environ["SPARK_HOME"]
EVENT_LOG_DIR = HISTORY_LOG_DIR = f"file:///{SPARK_HOME}/spark-events"

# SQL Server configuration
SQL_PORT = "1433"
SQL_DATABASE_NAME = "options"
SQL_SCHEMA_NAME = "chains"
SOCKET_TIMEOUT = 36000
SQL_SERVER_URL = f"jdbc:sqlserver://{MSSQLConfig.PrimarySQLServer}:{MSSQLConfig.SQLServerPort};databaseName={SQL_DATABASE_NAME};SocketTimeout={SOCKET_TIMEOUT}"
SQL_TABLE = "OptionsDataStaging"
SQL_TABLE_NAME = f"{SQL_SCHEMA_NAME}.{SQL_TABLE}"
STORED_PROCEDURE = "ProcessOptionsDataStaging"
STORED_PROCEDURE_NAME = (
    f"[{SQL_DATABASE_NAME}].[{SQL_SCHEMA_NAME}].[{STORED_PROCEDURE}]"
)
SQL_CONNECTION_PROPERTIES = MSSQLConfig.JdbcConnectionProperties
"""
NOTE
Because of the limitations of Bulk Copy API, MONEY, SMALLMONEY, DATE,
DATETIME, DATETIMEOFFSET, SMALLDATETIME, TIME, GEOMETRY, and GEOGRAPHY
data types, are currently not supported for this feature.
"""
SQL_CONNECTION_PROPERTIES["useBulkCopyForBatchInsert"] = "true"
SQL_CONNECTION_PROPERTIES["cacheBulkCopyMetadata"] = "true"


def get_rate_spark(df, t_col):
    """
    Calculate the Svensson interest rate for a given time column using Spark SQL functions.

    :param df: The DataFrame containing the data.
    :param t_col: The column name in the DataFrame that contains the time to maturity.
    :return: A new DataFrame with the calculated interest rate.
    """
    exp_term_tau1 = exp(-col(t_col) / TAU1)
    term2 = BETA1 * ((1 - exp_term_tau1) / (col(t_col) / TAU1))
    term3 = BETA2 * (((1 - exp_term_tau1) / (col(t_col) / TAU1)) - exp_term_tau1)

    exp_term_tau2 = exp(-col(t_col) / TAU2)
    term4 = BETA3 * (((1 - exp_term_tau2) / (col(t_col) / TAU2)) - exp_term_tau2)

    rate = BETA0 + term2 + term3 + term4
    rate = log1p(rate)

    return df.withColumn("rate", rate)


def calculate_implied_volatility(
    mark_series,
    underlyingPrice_series,
    strikePrice_series,
    t_series,
    q_series,
    r_series,
    putCall_series,
    exerciseType_series,
) -> pd.Series:
    iv_series: List[Optional[float]] = []
    for mark, underlyingPrice, strikePrice, t, q, r, putCall, exerciseType in zip(
        mark_series,
        underlyingPrice_series,
        strikePrice_series,
        t_series,
        q_series,
        r_series,
        putCall_series,
        exerciseType_series,
    ):
        iv = IV_ENGINE.value.find(
            float(mark),
            underlyingPrice,
            float(strikePrice),
            t,
            q,
            r,
            putCall,
            exerciseType,
        )
        iv_series.append(iv)
    return pd.Series(iv_series)


# Define the Pandas UDF
@pandas_udf(DoubleType())  # type: ignore
def implied_volatility_udf(
    mark_series: pd.Series,
    underlyingPrice_series: pd.Series,
    strikePrice_series: pd.Series,
    t_series: pd.Series,
    q_series: pd.Series,
    r_series: pd.Series,
    putCall_series: pd.Series,
    exerciseType_series: pd.Series,
) -> pd.Series:
    return calculate_implied_volatility(
        mark_series,
        underlyingPrice_series,
        strikePrice_series,
        t_series,
        q_series,
        r_series,
        putCall_series,
        exerciseType_series,
    )


# Define UDF to decompress gzip data
@udf(returnType=StringType())
def gzip_decompress(data):
    return gzip.decompress(data).decode('utf-8')


def flatten_chains(chains_df: "DataFrame", epoch_id: int, count: Optional[int] = None):

    def explode_options(option_type):
        return (
            chains_df.selectExpr(
                f"explode({option_type}ExpDateMap) as (expiry, strike_dict)",
                "underlyingPrice",
                "fetchTime",
                "dividendAmount",
                "isIndex",
                "time_remaining",
                "q",
            )
            .selectExpr(
                "explode(strike_dict) as (strike, options_list)",
                "underlyingPrice",
                "fetchTime",
                "dividendAmount",
                "isIndex",
                "time_remaining",
                "q",
            )
            .withColumn("option_row", explode(col("options_list")))
            .select(
                "option_row.*",
                "underlyingPrice",
                "fetchTime",
                "dividendAmount",
                "isIndex",
                "time_remaining",
                "q",
            )
        )

    calls_df = explode_options("call")
    puts_df = explode_options("put")
    all_df = calls_df.unionByName(puts_df).drop("optionDeliverablesList")
    return all_df


def call_stored_procedure(epoch_id: int):
    try:
        start = time.perf_counter()
        logger.info(
            f"Executing stored procedure {STORED_PROCEDURE_NAME} (epoch_id={epoch_id})"
        )

        with MSSQLClient(
            MSSQLConfig.ConfiguredConnectionString,
        ) as sql_client:
            sql_client.execute(f"EXEC {STORED_PROCEDURE_NAME}")

        end = time.perf_counter()
        elapsed = end - start
        logger.info(
            f"Successfully executed stored procedure {STORED_PROCEDURE_NAME} "
            f"in {elapsed} seconds (epoch_id={epoch_id})"
        )
    except pyodbc.Error as e:
        logger.error(
            f"Error calling stored procedure {STORED_PROCEDURE_NAME} (epoch_id={epoch_id}): {e}",
            exc_info=True,
        )


def process_batch(df: "DataFrame", epoch_id: int) -> None:

    start_time = time.perf_counter()
    # Deserialize and decompress the Kafka value
    df = df.withColumn("json", gzip_decompress(col("value")))

    chains_df = df.withColumn(
        "data", from_json(col("json"), OptionsChainSchema)
    ).selectExpr("data.*")

    chains_df = chains_df.withColumn(
        "q", log1p(col("dividendAmount") / col("underlyingPrice"))
    )

    chains_df = chains_df.withColumn(
        "time_remaining",
        least(
            when(
                col("isIndex") == True,  # noqa: E712
                (
                    (REGEND_IND - col("fetchTime").cast("long"))
                    / lit(MarketCalendar.OptionMarketHours.IND.TRADING_SECONDS_PER_YEAR)
                ).cast(DoubleType()),
            ).otherwise(
                (
                    (REGEND_EQO - col("fetchTime").cast("long"))
                    / lit(MarketCalendar.OptionMarketHours.EQO.TRADING_SECONDS_PER_YEAR)
                ).cast(DoubleType())
            ),
            lit(0.0),
        ),
    )

    all_df = flatten_chains(chains_df, epoch_id)

    # calculate time to expiration excluding the current trading session
    expiry_df = (
        all_df.groupBy("expirationDate")
        .agg(col("expirationDate").substr(1, 10).alias("iso_date"))
        .join(MARKET_SCHEDULE.value, on="iso_date")  # type: ignore
        .drop("iso_date")
    )
    all_df = all_df.join(broadcast(expiry_df), on="expirationDate", how="left")

    all_df = all_df.withColumn(
        "t",
        when(
            col("isIndex") == True,  # noqa: E712
            col("time_remaining") + col("T_ind"),
        ).otherwise(col("time_remaining") + col("T_eqo")),
    )
    all_df = all_df.drop("time_remaining", "T_ind", "T_eqo")

    all_df = get_rate_spark(
        df=all_df,
        t_col="t",
    )

    all_df = all_df.withColumn(
        "iv_mark",
        implied_volatility_udf(  # type: ignore
            col("mark"),  # type: ignore
            col("underlyingPrice"),  # type: ignore
            col("strikePrice"),  # type: ignore
            col("t"),  # type: ignore
            col("q"),  # type: ignore
            col("r"),  # type: ignore
            col("putCall"),  # type: ignore
            col("exerciseType"),  # type: ignore
        ),
    )

    all_df = all_df.withColumns(
        {
            "extra": to_json(
                struct(
                    col("q"),
                    col("r"),
                    col("t"),
                    col("dividendAmount"),
                    col("isIndex"),
                )
            ),
            "underlyingPrice": round(col("underlyingPrice"), 2).cast(
                DecimalType(10, 2)
            ),
        }
    )

    processed_rows = all_df.drop("q", "r", "t", "dividendAmount", "isIndex")
    num_partitions = spark.sparkContext.defaultParallelism * 2
    processed_rows = processed_rows.repartition(num_partitions, "putCall")

    try:
        logger.info(f"Attempting to write.jdbc (epoch_id={epoch_id})")

        start_time_write_jdbc = time.perf_counter()

        # Write data to SQL Server
        processed_rows.write.jdbc(
            SQL_SERVER_URL,
            SQL_TABLE_NAME,
            "append",
            SQL_CONNECTION_PROPERTIES,
        )

        end_time_write_jdbc = time.perf_counter()
        elapsed_write_jdbc = int((end_time_write_jdbc - start_time_write_jdbc) // 60.0)
        logger.info(
            f"Successfully wrote.jdbc in ~{elapsed_write_jdbc} minutes (epoch_id={epoch_id})"
        )
        call_stored_procedure(epoch_id)

    except Exception as e:
        logger.error(f"Failed to write to SQL (epoch_id={epoch_id}) {e}", exc_info=True)

        path = os.path.join(
            ERROR_PATH,
            str(epoch_id),
        )
        processed_rows.write.parquet(
            path,
            "append",
        )

    end_time = time.perf_counter()
    elapsed = int((end_time - start_time) // 60)
    logger.info(f"Batch processed in ~{elapsed} minutes (epoch_id={epoch_id})")


def main():
    # # fmt: off
    try:
        sdf = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("kafka.fetch.message.max.bytes", "10485880")
            .option("kafka.max.request.size", "10485880")
            .option("subscribe", KAFKA_TOPIC)
            .option(
                "maxOffsetsPerTrigger", 20
            )  # 20 * 15s processingTime = 300 offsets per batch
            .option("startingOffsets", "earliest")
            .option("checkpointLocation", CHECKPOINT_DIR)
            .load()
        )

        sdf.writeStream.outputMode("append").option(
            "checkpointLocation", CHECKPOINT_DIR
        ).foreachBatch(process_batch).queryName(
            f"processAndWriteQuery_{CURRENT_DATE}"
        ).trigger(
            processingTime='15 seconds'
        ).start().awaitTermination()
        # fmt: on
    finally:
        spark.stop()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName(f"OptionsDataProcessor_{CURRENT_DATE}")  # type: ignore
        .config("spark.driver.memory", "32g")
        .config("spark.executor.heartbeatInterval", "36000s")
        .config("spark.eventLog.dir", EVENT_LOG_DIR)
        .config("spark.eventLog.enabled", "true")
        .config("spark.executor.memory", "64g")
        .config("spark.history.fs.logDirectory", HISTORY_LOG_DIR,)
        .config("spark.kafka.maxPartitionFetchBytes", 10_485_760)
        .config("spark.network.timeout", "48000s")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.streaming.kafka.maxRatePerPartition", 40)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    assert isinstance(spark, SparkSession)
    spark.conf.set(
        "spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism
    )
    spark.conf.set("spark.default.parallelism", spark.sparkContext.defaultParallelism)
    spark.streams.addListener(MyListener(logger))
    spark.sparkContext.setLogLevel("INFO")

    def get_session_hours():
        session_hours = mh.fetch_today()
        # session_hours = mh.fetch(date=TODAY, save=False)

        regend_eqo = to_timestamp(
            lit(
                mh.get_hour(
                    session_hours, "$.option.EQO.sessionHours.regularMarket[0].end"
                ).isoformat(" ")
            )
        ).cast("long")
        regend_ind = to_timestamp(
            lit(
                mh.get_hour(
                    session_hours, "$.option.IND.sessionHours.regularMarket[0].end"
                ).isoformat(" ")
            )
        ).cast("long")
        return regend_eqo, regend_ind

    REGEND_EQO, REGEND_IND = get_session_hours()

    def get_svennson_params():
        svennson = SvenssonYieldTermStructure.without_data()
        svennson_params = svennson.get_params()
        beta0 = lit(svennson_params["beta0"])
        beta1 = lit(svennson_params["beta1"])
        beta2 = lit(svennson_params["beta2"])
        beta3 = lit(svennson_params["beta3"])
        tau1 = lit(svennson_params["tau1"])
        tau2 = lit(svennson_params["tau2"])
        return beta0, beta1, beta2, beta3, tau1, tau2

    BETA0, BETA1, BETA2, BETA3, TAU1, TAU2 = get_svennson_params()

    def get_market_schedule():
        market_schedule_pdf = MarketCalendar(start=TODAY).schedule
        expiration_dates = get_expiration_dates()
        market_schedule_pdf = market_schedule_pdf[
            market_schedule_pdf.index.isin(expiration_dates)
        ]
        market_schedule_pdf = market_schedule_pdf.reset_index()
        market_sched_sdf = spark.createDataFrame(market_schedule_pdf)
        market_sched_sdf = market_sched_sdf.select(
            "iso_date", "T_eqo", "T_ind"
        ).collect()
        return spark.sparkContext.broadcast(market_sched_sdf)

    MARKET_SCHEDULE = get_market_schedule()
    IV_ENGINE = spark.sparkContext.broadcast(ImpliedVolatilityEngine())

    main()
