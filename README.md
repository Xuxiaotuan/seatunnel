# Apache SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="SeaTunnel Logo" height="200px" align="right" />

[![Build Workflow](https://github.com/apache/seatunnel/actions/workflows/build_main.yml/badge.svg?branch=dev)](https://github.com/apache/seatunnel/actions/workflows/build_main.yml)
[![Join Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/apache/seatunnel)

## Overview
SeaTunnel is a multimodal, high-performance, distributed data integration tool, capable of synchronizing vast amounts of data daily. It's trusted by numerous companies for its efficiency and stability.

## Why Choose SeaTunnel
SeaTunnel addresses common data integration challenges:
- **Diverse Data Sources**: Seamlessly integrates with hundreds of evolving data sources.
- **Multimodal Data Integration**: Supports the integration of video, images, binary files, structured and unstructured text data.
- **Complex Synchronization Scenarios**: Supports various synchronization methods, including real-time, CDC, and full database synchronization.
- **Resource Efficiency**: Minimizes computing resources and JDBC connections for real-time synchronization.
- **Quality and Monitoring**: Provides data quality and monitoring to prevent data loss or duplication.

## Key Features
- **Diverse Connectors**: Offers support for over 160 connectors, with ongoing expansion.
- **Batch-Stream Integration**: Easily adaptable connectors simplify data integration management.
- **Distributed Snapshot Algorithm**: Ensures data consistency across synchronized data.
- **Multi-Engine Support**: Works with SeaTunnel Zeta Engine, Flink, and Spark.
- **JDBC Multiplexing and Log Parsing**: Efficiently synchronizes multi-tables and databases.
- **High Throughput and Low Latency**: Provides high-throughput data synchronization with low latency.
- **Real-Time Monitoring**: Offers detailed insights during synchronization.

## SeaTunnel Workflow
![SeaTunnel Workflow](docs/images/architecture_diagram.png)

Configure jobs, select execution engines, and parallelize data using Source Connectors. Easily develop and extend connectors to meet your needs.

## Supported Connectors
- [Source Connectors](https://seatunnel.apache.org/docs/connectors/source)
- [Sink Connectors](https://seatunnel.apache.org/docs/connectors/sink)
- [Transform Connectors](https://seatunnel.apache.org/docs/transforms)

## Getting Started
Download SeaTunnel from the [Official Website](https://seatunnel.apache.org/download).
Choose your runtime execution engine:
- [SeaTunnel Zeta Engine](https://seatunnel.apache.org/docs/getting-started/locally/quick-start-seatunnel-engine)
- [Spark](https://seatunnel.apache.org/docs/getting-started/locally/quick-start-spark)
- [Flink](https://seatunnel.apache.org/docs/getting-started/locally/quick-start-flink)

## Multimodal Data Integration
- Most data integration tools support structured and unstructured text data, and SeaTunnel does as well. Simply refer to the desired Source/Sink to use.
- For integrating video, images, and binary files with SeaTunnel, please refer to the documentation for detailed instructions.

## Apache SeaTunnel Tools
SeaTunnel Tools provides a range of peripheral tools, including Apache SeaTunnel Mcp Server, etc, please refer to [SeaTunnel Tools](https://github.com/apache/seatunnel-tools).

## Users
Companies and organizations worldwide use SeaTunnel for research, production, and commercial products. 
Explore real-world use cases of SeaTunnel, such as JP morgan, S7, JDT, Bytedance, Tencent Cloud. More use cases can be found on the [SeaTunnel Users](https://seatunnel.apache.org/user).

## Code of Conduct
Participate in this project in accordance with the Contributor Covenant [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

## Contributors
We appreciate all developers for their contributions. See the [List Of Contributors](https://github.com/apache/seatunnel/graphs/contributors).

## How to Compile
Refer to this [Setup](https://seatunnel.apache.org/docs/developer/setup) for compilation instructions.

## Local Flink 1.20.5 Python Transform Smoke Test

This branch contains a local verification case for the Python transform plugin on Flink 1.20.5. The job has two `FakeSource` inputs and one PostgreSQL JDBC input, three row-level Python transforms, and two `Console` sinks plus one PostgreSQL JDBC sink. The transform normalizes `name`, adds one to `age`, and propagates a value from `context["config"]`.

The example files are:

- [`python_transform_multi_source_multi_sink.conf`](seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/python_transform_multi_source_multi_sink.conf)
- [`python_transform.py`](seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/python_transform.py)

Build the relevant modules with the local Flink version:

```bash
./mvnw -pl \
  seatunnel-connectors-v2/connector-fake,\
  seatunnel-connectors-v2/connector-console,\
  seatunnel-connectors-v2/connector-jdbc,\
  seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-20,\
  seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter,\
  seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example \
  -am -Dflink.1.20.1.version=1.20.5 -DskipTests package
```

Set the Python execution policy for the local Flink runtime and make the PostgreSQL connection values available as `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, and `PG_PASSWORD`. Copy `python_transform.py` to the path used by `source_code_path` before submitting the job:

```bash
cp seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/python_transform.py \
  /tmp/seatunnel-python-transform.py

export FLINK_HOME=/Users/xujiawei/software/flink/flink-1.20.5
export SEATUNNEL_HOME=/path/to/seatunnel-runtime
export FLINK_CONF_DIR=$SEATUNNEL_HOME/flink-conf
```

The runtime directory must contain the Flink 1.20 starter under `starter/`, the `Fake`, `Console`, and `Jdbc` connector jars plus `seatunnel-transforms-v2.jar` under `connectors/`, the Flink 1.20 translation jar under `lib/`, and the PostgreSQL JDBC driver. Submit the job with:

```bash
$SEATUNNEL_HOME/bin/start-seatunnel-flink-20-connector-v2.sh \
  --master local --deploy-mode run \
  -c $SEATUNNEL_HOME/config/python_transform_multi_source_multi_sink.conf \
  --name PythonTransformSmoke
```

The local verification completed with Flink 1.20.5 and six input rows. PostgreSQL returned the transformed rows `301,Eve,28,eve,29,postgres` and `302,Frank,35,frank,36,postgres`. The branch also ran 24 Python transform unit tests successfully.

The current JDBC sink implementation derives the generated sink table from the JDBC source physical table when `generate_sink_sql = true`; this smoke test therefore writes the transformed columns back to the dedicated `seatunnel_python_input` test table. Use a separate test table or adapt the sink table resolution before using this example against business data.

## Contact Us
- Mail list: **dev@seatunnel.apache.org**. Subscribe by sending an email to `dev-subscribe@seatunnel.apache.org`.
- Slack: [Join SeaTunnel Slack](https://s.apache.org/seatunnel-slack)
- Twitter: [ASFSeaTunnel on Twitter](https://twitter.com/ASFSeaTunnel)

## Landscapes
SeaTunnel enriches the [CNCF CLOUD NATIVE Landscape](https://landscape.cncf.io/?landscape=observability-and-analysis&license=Apache+License+2.0).

## License
[Apache 2.0 License](LICENSE)

## Frequently Asked Questions

### 1. How do I install SeaTunnel?

Follow the [Local Deployment](https://seatunnel.apache.org/docs/getting-started/locally/deployment) on SeaTunnel website to get 
started quickly.
Please refer to the [Cluster Deployment](https://seatunnel.apache.org/docs/engines/zeta/hybrid-cluster-deployment)

### 2. Where can I find documentation and tutorials?
[Official Documentation](https://seatunnel.apache.org/docs) includes detailed guides and tutorials to help you get started.

### 3. Is there a community or support channel?
You can submit an issue on [GitHub Issues](https://github.com/apache/seatunnel/issues).
Join our Slack community [SeaTunnel Slack](https://s.apache.org/seatunnel-slack).
More information, please refer to [FAQ](https://seatunnel.apache.org/docs/faq). 

### 4. How can I contribute to SeaTunnel?
We welcome contributions! Please refer to our [Contribution Guidelines](https://seatunnel.apache.org/docs/developer/coding-guide) for details.
