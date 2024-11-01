import re
import subprocess
from typing import List, Optional, Tuple, Type, Union, cast, overload
from typing_extensions import deprecated

from kafka import KafkaConsumer, KafkaProducer

from ..models import KafkaConsumerConfigs, KafkaProducerConfigs

__all__ = [
    "get_kafka_broker_version_from_logs",
    "get_default_consumer_configs",
    "get_default_producer_configs",
]


class DockerError(Exception):
    """Base docker exception"""


class BrokerApiVersionParserError(DockerError):
    """
    Raised when the return code of the subprocess that
    runs the`docker logs <container-name>` command is 0
    (i.e., no stderror) but the
    `get_kafka_broker_version_from_logs` function fails
    to parse the logs for the Broker API version
    """


class DockerCommandError(DockerError):
    """
    Raised when the return code of the subprocess that
    runs a`docker command is non-zero
    """


class DockerLogsCommandError(DockerCommandError):
    """
    Raised when the subprocess that runs the `docker
    logs <container-name>` command returns a non-zero
    return code
    """


class DockerBrokerApiVersionsCommandError(DockerCommandError):
    """
    Raised when the subprocess that runs the `docker
    exec <container-name> /bin/bash -c
    /usr/bin/kafka-broker-api-versions
    --bootstrap-server <bootstrap_server>` command
    returns a non-zero return code
    """


class DockerMayNotBeStartedError(DockerError):
    """Raised when the `docker logs <container-name>` raises a FileNotFoundError"""


def get_kafka_broker_version_from_logs(
    container_name="daily-kafka-1",
) -> Tuple[int, int, int]:
    """
    Retrieves the Kafka broker version by inspecting the Docker container logs.

    :param container_name: The name of the Kafka Docker container (default is 'daily-kafka-1').
    :return: The Kafka broker version as a tuple of 3 integers.
    """
    try:
        command = ["docker", "logs", container_name]

        # Execute the command using subprocess
        result = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        if result.returncode == 0:
            # Command was successful, process the output

            for line in result.stdout.splitlines():
                if "Kafka version" in line:
                    # Example log line: INFO Kafka version: 2.8.0 (commit id: abc123)
                    version_line = line.strip()
                    version_string = version_line.split("Kafka version:")[-1].strip()
                    return parse_version_string(version_string)

            # return "Unable to find the Kafka version in the logs."
            raise BrokerApiVersionParserError(
                "Unable to find the Kafka version in the logs."
            )

        else:
            # Command failed, output the error
            raise DockerLogsCommandError(
                f"Error running docker logs command: {result.stderr}"
            )

    except FileNotFoundError as e:
        raise DockerMayNotBeStartedError(
            "Docker not found. Please make sure Docker is installed and running."
        ) from e

    except Exception as e:
        raise DockerError(
            "An error occurred while retrieving the Kafka broker version from logs."
        ) from e


def parse_version_string(version_string):
    """
    Parses a version string like '7.3.10-ccs' and converts it into a tuple of integers (7, 3, 10).

    :param version_string: The version string to parse (e.g., '7.3.10-ccs').
    :return: A tuple of 3 integers representing the version (major, minor, patch).
    """
    # Use regular expression to capture the first three numeric parts of the version
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version_string)
    if match:
        major, minor, patch = match.groups()
        return int(major), int(minor), int(patch)
    else:
        raise ValueError(f"Invalid version format: {version_string}")


@deprecated(
    "This command works but it outputs a laundry list of different services / "
    "functionalities and what version(s) they support, not a singular broker "
    "api version we could pass to the constructors of KafkaProducer, "
    "KafkaConsumer, and KafkaAdminClient.  Use "
    "`get_kafka_broker_version_from_logs` instead."
)
def get_kafka_broker_version_from_api_versions_command(
    container_name="daily-kafka-1", bootstrap_server="localhost:9092"
):
    """
    Retrieves the Kafka broker API versions using the Kafka CLI tool inside a Docker container.
    Since Kafka version may not be printed, this focuses on listing API versions.

    :param container_name: The name of the Kafka Docker container (default is 'kafka').
    :param bootstrap_server: The Kafka broker's address (default is 'localhost:9092').
    :return: The API versions supported by the Kafka broker.
    """
    try:
        # Run the kafka-broker-api-versions.sh command inside the Docker container
        command = [
            "docker",
            "exec",
            container_name,  # Execute the following command inside the Docker container
            "/bin/bash",
            "-c",  # Run a bash command inside the container
            f"/usr/bin/kafka-broker-api-versions --bootstrap-server {bootstrap_server}",
        ]

        # Execute the command using subprocess
        result = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        if result.returncode == 0:
            # Command was successful, process the output

            print("Kafka API Versions:")
            print(result.stdout)

            # Parse the API versions from the output
            api_versions = {}
            for line in result.stdout.splitlines():
                if "(" in line and "usable" in line:
                    # Example line: Produce(0): 0 to 9 [usable: 9]
                    try:
                        api_name, versions = line.split(":")
                        api_name = api_name.strip()
                        versions_range = versions.split("[usable:")[0].strip()
                        api_versions[api_name] = versions_range
                    except Exception as e:
                        print(f"Failed to parse line: {line} due to {e}")

            # Return the parsed API versions
            return api_versions

        else:
            # Command failed, output the error
            raise DockerBrokerApiVersionsCommandError(
                f"Error running docker logs command: {result.stderr}"
            )

    except FileNotFoundError as e:
        raise DockerMayNotBeStartedError(
            "Docker not found. Please make sure Docker is installed and running."
        ) from e

    except Exception as e:
        raise DockerError(
            "An error occurred while retrieving the Kafka broker version from logs."
        ) from e


@overload
def _get_default_configs(
    cls: Type[KafkaConsumer],
    bootstrap_servers: Union[str, List[str]] = "localhost:9092",
    api_version: Optional[Tuple[int, int, int]] = None,
) -> KafkaConsumerConfigs: ...


@overload
def _get_default_configs(
    cls: Type[KafkaProducer],
    bootstrap_servers: Union[str, List[str]] = "localhost:9092",
    api_version: Optional[Tuple[int, int, int]] = None,
) -> KafkaProducerConfigs: ...


def _get_default_configs(
    cls: Union[Type[KafkaConsumer], Type[KafkaProducer]],
    bootstrap_servers: Union[str, List[str]] = "localhost:9092",
    api_version: Optional[Tuple[int, int, int]] = None,
) -> Union[KafkaConsumerConfigs, KafkaProducerConfigs]:
    """
    Utility function for getting the default configuurations
    for KafkaConsumer and KafkaProducer.

    :param cls: the Kafka client type
    :type cls: Union[Type[KafkaConsumer], Type[KafkaProducer]]
    :param bootstrap_servers: the bootstrap servers configuration
        formatted as "<host>:<port>", optional.
        Defaults to "localhost:9092".
    :type bootstrap_servers: Union[List[str], str]
    :param api_version: the broker api version, optional.
        Note: this value is NOT the same as `kafka.__version__`
        If not provided, it defaults to the version retrieved
        from `get_kafka_broker_version_from_logs`
    :raises TypeError: if cls is not Type[KafkaConsumer] or
        Type[KafkaProducer]
    :return: the default configuration
    :rtype: KafkaConsumerConfigs if cls is KafkaConsumer and
            KafkaProducerConfigs if cls is KafkaProducer
    """
    if cls is KafkaConsumer:
        default_configs = cast(
            KafkaConsumerConfigs, KafkaConsumer.DEFAULT_CONFIG.copy()
        )
    elif cls is KafkaProducer:
        default_configs = cast(
            KafkaProducerConfigs, KafkaProducer.DEFAULT_CONFIG.copy()
        )
    else:
        raise TypeError(f"Expected type KafkaConsumer or KafkaProducer, got {cls!r}")

    default_configs["bootstrap_servers"] = bootstrap_servers
    api_version = api_version or get_kafka_broker_version_from_logs()
    default_configs["api_version"] = api_version
    return default_configs


def get_default_consumer_configs(
    bootstrap_servers: Union[str, List[str]] = "localhost:9092",
    api_version: Optional[Tuple[int, int, int]] = None,
) -> KafkaConsumerConfigs:
    return _get_default_configs(KafkaConsumer)


def get_default_producer_configs(
    bootstrap_servers: Union[str, List[str]] = "localhost:9092",
    api_version: Optional[Tuple[int, int, int]] = None,
) -> KafkaProducerConfigs:
    return _get_default_configs(KafkaProducer)


if __name__ == "__main__":
    # Customize the container name if needed
    kafka_container = "daily-kafka-1"  # Replace with your actual container name

    # Get the Kafka broker's version by inspecting the logs
    version = get_kafka_broker_version_from_logs(container_name=kafka_container)

    if version:
        print(f"Kafka broker version: {version}")
    else:
        print(
            f"Failed to retrieve Kafka version from logs of container {kafka_container}."
        )

#     # Customize the container name and broker's address if needed
#     kafka_container = "daily-kafka-1"  # Replace with your actual container name
#     kafka_broker = "localhost:9092"

#     # Get the Kafka broker's API versions using the CLI tool inside Docker
#     api_versions = get_kafka_broker_api_version_docker(
#         container_name=kafka_container, bootstrap_server=kafka_broker
#     )

#     if api_versions:
#         print(f"Kafka broker at {kafka_broker} supports the following API versions:")
#         for api_name, version_range in api_versions.items():
#             print(f"{api_name}: {version_range}")
#     else:
#         print(
#             f"Failed to retrieve API versions from Kafka broker at {kafka_broker} in container {kafka_container}."
#         )
