#!/usr/bin/env python3
from csv import DictWriter
import datetime as dt
import decimal
import gzip
from io import StringIO
import json
import os
import threading
import time
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from flask import Flask
import pyodbc

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import date_format
from pyspark.sql.functions import from_json
from pyspark.sql.functions import lit
from pyspark.sql.functions import round
from pyspark.sql.functions import udf

from options_chain_pipeline.lib import env
from options_chain_pipeline.lib import ImpliedVolatilityEngine
from options_chain_pipeline.lib import MarketCalendar

# spark Stream Listener
from options_chain_pipeline.lib.spark import MyListener

# spark schema
from options_chain_pipeline.lib.spark import OptionsChainSchema

# mssql
from options_chain_pipeline.lib import MSSQLClient
from options_chain_pipeline.lib import MSSQLConfig

# term structure and rates
from options_chain_pipeline.lib import continuously_compounding_rate
from options_chain_pipeline.lib import SvenssonYieldTermStructure

# logging
from options_chain_pipeline.lib import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


LOG_LEVEL = "INFO"
logger = get_logger(
    "options_chain_pipeline.processor", level=LOG_LEVEL, fh=True, fh_level="DEBUG"
)

# Kafka configuration
KAFKA_BROKER = "localhost:9093"
CURRENT_DATE = dt.date.today().strftime('%Y%m%d')
TOPIC = f"option_chain_topic_{CURRENT_DATE}"
# Set the checkpoint location to HDFS
HADOOP_HOME = os.environ["HADOOP_HOME"]
CHECKPOINT_DIR = f"file:///{HADOOP_HOME}/data/hdfs/checkpoints/{CURRENT_DATE}"

# SQL Server configuration
SQL_PORT = "1433"
SQL_DATABASE_NAME = "options"
SQL_SCHEMA_NAME = "chains"
SQL_SERVER_URL = f"jdbc:sqlserver://{MSSQLConfig.PrimarySQLServer}:{MSSQLConfig.SQLServerPort};databaseName={SQL_DATABASE_NAME}"
SQL_TABLE = "OptionsDataStaging"
SQL_TABLE_NAME = f"{SQL_SCHEMA_NAME}.{SQL_TABLE}"
SQL_CONNECTION_PROPERTIES = {
    "user": MSSQLConfig.SQLServerUID,
    "password": MSSQLConfig.SQLServerPWD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "trustServerCertificate": str(
        MSSQLConfig.ConfiguredConnectionString.trust_server_cert
    ).lower(),
    # "batchsize": "50000",
    # "isolationLevel": "READ_COMMITTED",  # Ensure consistent read/write operations
    # "isolationLevel": "NONE",  # Ensure consistent read/write operations
    "rewriteBatchedStatements": "true",
    "useBulkCopyForBatchInsert": "true",  # NEW
}
STORED_PROCEDURE = "ProcessOptionsDataStaging"
STORED_PROCEDURE_NAME = (
    f"[{SQL_DATABASE_NAME}].[{SQL_SCHEMA_NAME}].[{STORED_PROCEDURE}]"
)

THREADS: List[threading.Thread] = []

# Transformation helpers
MARKET_CALENDAR = MarketCalendar()
"""Helper for calculating `t`, or time in years, excluding non-market
days, until expiration, of each option.  It is configured to
reference NYSE's market calendar.
"""
TERM_STRUCTURE = SvenssonYieldTermStructure.without_data()
"""Helper for calculating `r`, or the continuous, risk-free
interest rate yield for bonds maturing in `t` years, of each
option.
"""
IV_ENGINE = ImpliedVolatilityEngine()
"""Helper for calculating implied volatility.  Selects
appropriate pricing engine based on each option's `exerciseType`
(usually `"A" (American) or "E" (European)`).  Sometimes,
`exerciseType` is set to an empty string. In those cases, we default
to American (`"A"`)....for no good reason....
"""
LAST_INSERT_TIMESTAMP: Optional[float] = None
"""
Timestamp at which a batch was last inserted to MSSQL Server, defaults to None initially
"""

log4j_config_file = os.path.join(env.PROJECT_ROOT, "spark", "log4j.properties")
# FIXME set log4j log_file
log_dir = os.path.join(env.LOG_PATH, "spark", CURRENT_DATE, str(os.getpid()))


# Create Spark configuration
conf = SparkConf()
conf.set(
    "spark.driver.extraJavaOptions",
    f"-Dlog4j.configuration=file:{log4j_config_file} -Dlogfile.path={log_dir}",
)
conf.set(
    "spark.executor.extraJavaOptions",
    f"-Dlog4j.configuration=file:{log4j_config_file} -Dlogfile.path={log_dir}",
)

conf.set("spark.sql.broadcastTimeout", "2400")
conf.set("spark.executor.memory", "64g")
conf.set("spark.executor.cores", "4")
conf.set("spark.driver.memory", "64g")
conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

# Initialize Spark context and session
sc = SparkContext(conf=conf)


@udf(returnType=types.DoubleType())
def calc_t(dte: int, fetchTime: str) -> float:
    fetchTimeDt = dt.datetime.strptime(fetchTime, "%Y-%m-%d %H:%M:%S.%f")
    return MARKET_CALENDAR.set_current_time(fetchTimeDt).get_t(dte)


@udf(returnType=types.DoubleType())
def calc_r(t: float) -> float:
    """Compute the continuous, risk-free interest rate at time t"""
    return float(continuously_compounding_rate(TERM_STRUCTURE.get_rate(t)))


@udf(returnType=types.DoubleType())
def calc_q(dividendAmount: float, underlyingPrice: float) -> float:
    """Compute the continuous dividend yield"""
    if (dividendAmount <= 0.0) or (underlyingPrice <= 0.0):
        return 0.0
    return float(continuously_compounding_rate(dividendAmount / underlyingPrice))


@udf(returnType=types.DoubleType())
def calc_iv(
    mark: "decimal.Decimal",
    underlyingPrice: float,
    strikePrice: "decimal.Decimal",
    t: float,
    q: float,
    r: float,
    putCall: str,
    exerciseType: str,
) -> Optional[float]:
    """Compute implied volatility"""
    return IV_ENGINE.find(
        float(mark),
        underlyingPrice,
        float(strikePrice),
        t,
        q,
        r,
        putCall,
        exerciseType,
    )


@udf(returnType=types.StringType())
def get_extra(q: float, r: float, t: float) -> str:
    return json.dumps({"q": q, "r": r, "t": t})


# Define UDF to decompress gzip data
@udf(returnType=types.StringType())
def gzip_decompress(data):
    return gzip.decompress(data).decode('utf-8')


def broadcast_time_and_rate(
    all_df: "DataFrame",
    # all_df_count: int,
    # count: int,
    epoch_id: int,
) -> "DataFrame":
    start_time = time.perf_counter()
    try:
        logger.info(
            # f"Attempting to broadcast time_and_rate_df to {all_df_count:,} individual options rows for {count:,} options chains (epoch_id={epoch_id})"
            f"Attempting to broadcast time_and_rate_df (epoch_id={epoch_id})"
        )
        time_and_rate_df = all_df.select("fetchTime", "daysToExpiration").distinct()
        time_and_rate_df = time_and_rate_df.withColumn(
            "t", calc_t(col("daysToExpiration"), col("fetchTime"))
        )
        time_and_rate_df = time_and_rate_df.withColumn("r", calc_r(col("t")))
        time_and_rate_broadcast = broadcast(time_and_rate_df)
        all_df = all_df.join(
            time_and_rate_broadcast, on=["daysToExpiration", "fetchTime"], how="left"
        )
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        logger.info(
            # f"Successfully broadcasted time_and_rate_df to {all_df_count:,} individual options rows for {count:,} options chains in {elapsed:3f} seconds (epoch_id={epoch_id})"
            f"Successfully broadcasted time_and_rate_df in {elapsed:3f} seconds (epoch_id={epoch_id})"
        )
    except Exception as e:
        logger.error(
            f"Failed to broadcast join time_and_rate_df to all_df (epoch_id={epoch_id}): {e}"
        )
        logger.info(
            f"Calculating t and r by applying udfs to entire df (epoch_id={epoch_id})"
        )
        all_df = all_df.withColumn(
            "t", calc_t(col("daysToExpiration"), col("fetchTime"))
        )
        all_df = all_df.withColumn("r", calc_r(col("t")))

        end_time = time.perf_counter()
        elapsed = end_time - start_time
        logger.info(
            f"Calculated t and r without broadcasting in {elapsed:3f} seconds (epoch_id={epoch_id})"
        )
    return all_df


def broadcast_dividend_yield(
    all_df: "DataFrame",
    epoch_id: int,
) -> "DataFrame":
    try:
        logger.info(
            f"Attempting to broadcast dividend_yield_df (epoch_id={epoch_id})"
        )
        start_time = time.perf_counter()
        dividend_yield_df = all_df.select(
            "dividendAmount", "underlyingPrice"
        ).distinct()
        dividend_yield_df = dividend_yield_df.withColumn(
            "q", calc_q(col("dividendAmount"), col("underlyingPrice"))
        )
        dividend_yield_broadcast = broadcast(dividend_yield_df)
        all_df = all_df.join(
            dividend_yield_broadcast,
            on=["dividendAmount", "underlyingPrice"],
            how="left",
        )
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        logger.info(
            f"Successfully broadcasted dividend_yield_df in ~{elapsed:3f} seconds (epoch_id={epoch_id})"
        )
    except Exception as e:
        logger.error(
            f"Failed to broadcast join dividend_yield_df to all_df (epoch_id={epoch_id}): {e}"
        )
        logger.info(
            f"Calculating q by applying the udf to entire df (epoch_id={epoch_id})"
        )
        all_df = all_df.withColumn(
            "q", calc_q(col("dividendAmount"), col("underlyingPrice"))
        )
    return all_df


def flatten_chains(chains_df: "DataFrame", epoch_id: int, count: Optional[int] = None):
    start_time = time.perf_counter()

    calls_df = chains_df.selectExpr(
        "callExpDateMap",
        "underlyingPrice",
        "CAST(fetchTime as STRING)",
        "dividendAmount",
    )
    calls_df = calls_df.selectExpr(
        "explode(callExpDateMap) as (call_expiration_date, call_data)",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    calls_df = calls_df.selectExpr(
        "explode(call_data) as (call_strike_price, call_strike_data)",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    calls_df = calls_df.selectExpr(
        "explode(call_strike_data) as call_option",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    calls_df = calls_df.selectExpr(
        "call_option.*",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    puts_df = chains_df.selectExpr(
        "putExpDateMap",
        "underlyingPrice",
        "CAST(fetchTime as STRING)",
        "dividendAmount",
    )
    puts_df = puts_df.selectExpr(
        "explode(putExpDateMap) as (put_expiration_date, put_data)",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    puts_df = puts_df.selectExpr(
        "explode(put_data) as (put_strike_price, put_strike_data)",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    puts_df = puts_df.selectExpr(
        "explode(put_strike_data) as put_option",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    puts_df = puts_df.selectExpr(
        "put_option.*",
        "underlyingPrice",
        "fetchTime",
        "dividendAmount",
    )
    all_df = calls_df.union(puts_df).drop("optionDeliverablesList")
    end_time = time.perf_counter()
    elapsed = int((end_time - start_time) // 60.0)

    logger.info(
        f"Successfully flattened df in ~{elapsed} minutes (epoch_id={epoch_id})"
    )

    return all_df


def call_stored_procedure(epoch_id: int):
    try:
        logger.info(
            f"Executing stored procedure {STORED_PROCEDURE_NAME} (epoch_id={epoch_id})"
        )

        with MSSQLClient(
            MSSQLConfig.ConfiguredConnectionString,
        ) as sql_client:
            sql_client.execute(f"EXEC {STORED_PROCEDURE_NAME}")

    except pyodbc.Error as e:
        logger.error(
            f"Error calling stored procedure {STORED_PROCEDURE_NAME} (epoch_id={epoch_id}): {e}",
            exc_info=True,
        )

    finally:
        if (th := threading.current_thread()) in THREADS:
            THREADS.remove(th)


def process_batch(df: "DataFrame", epoch_id: int) -> None:

    start_time = time.perf_counter()
    
    # Deserialize and decompress the Kafka value
    df = df.withColumn("json", gzip_decompress(col("value")))

    chains_df = df.withColumn(
        "data", from_json(col("json"), OptionsChainSchema)
    ).selectExpr("data.*")

    all_df = flatten_chains(chains_df, epoch_id)
    all_df = broadcast_dividend_yield(all_df, epoch_id)
    all_df = broadcast_time_and_rate(all_df, epoch_id)
    all_df = all_df.withColumn(
        "iv_mark",
        calc_iv(
            col("mark"),
            col("underlyingPrice"),
            col("strikePrice"),
            col("t"),
            col("q"),
            col("r"),
            col("putCall"),
            col("exerciseType"),
        ),
    )
    all_df = all_df.withColumns(
        {
            "extra": get_extra(col("q"), col("r"), col("t")),
            "underlyingPrice": round(col("underlyingPrice"), 2).cast(
                types.DecimalType(10, 2)
            ),
        }
    )
    processed_rows = all_df.drop("q", "r", "t", "dividendAmount")

    def write_jdbc():
        try:
            logger.info(f"Attempting to write.jdbc (epoch_id={epoch_id})")

            global LAST_INSERT_TIMESTAMP
            if LAST_INSERT_TIMESTAMP is not None:
                start_time = LAST_INSERT_TIMESTAMP
            else:
                start_time = time.perf_counter()

            # Write data to SQL Server
            processed_rows.repartition("expirationDate").write.jdbc(
                SQL_SERVER_URL,
                SQL_TABLE_NAME,
                "append",
                SQL_CONNECTION_PROPERTIES,
            )
            end_time = LAST_INSERT_TIMESTAMP = time.perf_counter()

            elapsed = int((end_time - start_time) // 60.0)
            logger.info(f"Successfully wrote.jdbc in ~{elapsed} minutes (epoch_id={epoch_id})")
            th = threading.Thread(target=call_stored_procedure, args=(epoch_id,))
            THREADS.append(th)
            th.start()

        except Exception as e:
            logger.error(
                f"Failed to write to SQL (epoch_id={epoch_id}) {e}", exc_info=True
            )

            path = os.path.join(
                env.DATA_PATH,
                "spark_errors",
                CURRENT_DATE,
                str(epoch_id),
            )
            os.makedirs(path, exist_ok=True)
            file_name = f"{dt.datetime.now().timestamp()}.parquet"
            file_path = os.path.join(path, file_name)
            processed_rows.write.parquet(
                file_path,
                "append",
            )

        finally:
            if (th := threading.current_thread()) in THREADS:
                THREADS.remove(th)

    th = threading.Thread(target=write_jdbc)
    THREADS.append(th)
    th.start()
    end_time = time.perf_counter()
    elapsed = int((end_time - start_time) // 60)
    logger.info(f"Batch processed in ~{elapsed} minutes (epoch_id={epoch_id})")


def main():
    spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
    spark.conf.set("spark.default.parallelism", sc.defaultParallelism)

    # Add the custom listener to the Spark session
    spark.streams.addListener(MyListener(logger))
    spark.sparkContext.setLogLevel("INFO")

    # fmt: off
    try:
        sdf = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("kafka.fetch.message.max.bytes", "10485880")
            .option("kafka.max.request.size", "10485880")
            .option("subscribe", TOPIC)
            .option("maxOffsetsPerTrigger", 100)  # 100 offsets per trigger per second * 15 seconds processingTime = 1500 offsets per batch
            .option("startingOffsets", "earliest")
            .option("checkpointLocation", CHECKPOINT_DIR)
            .load()
        )

        sdf.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .foreachBatch(process_batch) \
            .queryName(f"processAndWriteQuery_{CURRENT_DATE}") \
            .trigger(processingTime='15 seconds') \
            .start() \
            .awaitTermination()
    # fmt: on
    finally:
        for th in THREADS:
            if th.is_alive() and th is not threading.main_thread():
                th.join()


app = Flask(__name__)


@app.route("/shutdown", methods=["POST"])
def shutdown():
    shutdown_hook()
    return "Shutting down..."


def shutdown_hook():
    global spark
    logger.info("Graceful shutdown initiated")
    for query in spark.streams.active:
        query.stop()
    spark.stop()
    logger.info("Graceful shutdown completed")


if __name__ == "__main__":
    spark = (
        SparkSession(sc)
        .builder.appName(f"OptionsDataProcessor_{CURRENT_DATE}")
        .config("spark.kafka.maxPartitionFetchBytes", 10_485_760)
        .config(
            "spark.streaming.kafka.maxRatePerPartition", 20
        )  # 20 * 10 partitions * 10 seconds processingTime = 2000 records per batch
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )

    flask_thread = threading.Thread(
        target=app.run, kwargs={'host': '0.0.0.0', 'port': 5000}
    )
    flask_thread.start()

    main()
