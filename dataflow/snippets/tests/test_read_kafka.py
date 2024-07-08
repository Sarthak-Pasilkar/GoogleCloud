#  Copyright 2024 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from pathlib import Path
import time
from unittest.mock import patch
import uuid

import docker

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

import pytest

from ..read_kafka import read_from_kafka


BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'my-topic'


@pytest.fixture(scope='module', autouse=True)
def kafka_container() -> None:
    # Start a containerized Kafka server.
    docker_client = docker.from_env()
    container = docker_client.containers.run('apache/kafka:3.7.0', ports={'9092/tcp': 9092}, detach=True)
    try:
        create_topic()
        yield
    finally:
        container.stop()


def create_topic() -> None:
    # Try to create a Kafka topic. We might need to wait for the Kafka service to start.
    for _ in range(1, 10):
        try:
            client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)
            topics = []
            topics.append(NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1))
            client.create_topics(topics)
            break
        except ValueError:
            time.sleep(5)


def test_read_from_kafka(tmp_path: Path) -> None:
    file_name_prefix = f'{tmp_path}/output-{uuid.uuid4()}'
    file_name = f'{file_name_prefix}-00000-of-00001.txt'

    # Send some messages to Kafka
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    for i in range(0, 5):
        message = f'event-{i}'
        producer.send(TOPIC_NAME, message.encode())

    with patch("sys.argv", ["",
                            '--streaming',
                            '--allow_unsafe_triggers',
                            f'--topic={TOPIC_NAME}',
                            f'--bootstrap_server={BOOTSTRAP_SERVER}',
                            f'--output={file_name_prefix}']):
        read_from_kafka()

    # Verify the pipeline wrote events to the output file.
    with open(file_name, 'r') as f:
        text = f.read()
        for i in range(0, 5):
            assert f'event-{i}' in text


if __name__ == "__main__":
    test_read_from_kafka()
