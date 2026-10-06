# SeaTunnel Python Transform · Flink 1.20.5 验证分支

这个分支用于在本地验证 SeaTunnel Python transform plugin，以及在一个 Flink 1.20.5 Job 中组合多 Source、多 Sink、SQL JOIN、Python transform 和 Source/Sink 前后置动作。

当前分支：`codex/flink-multi-source-join-workbench`

本次只编译和验证以下连接器：Fake、HTTP、Console、PostgreSQL JDBC。其他连接器不在本分支的验证范围内。

## 验证目标

这里包含两个相互独立的案例：

| 案例 | Source / Transform / Sink | 目的 |
| --- | --- | --- |
| 多 Source、多 Sink 生命周期案例 | Fake、HTTP、PostgreSQL JDBC → Python transform → Console、PostgreSQL JDBC | 验证每行数据经过 Python 处理，以及 JDBC source/sink 的 `pre`、`post` 动作 |
| 多 Source SQL JOIN 案例 | 两路 FakeSource → `pre_transform` SQL `INNER JOIN` → Python transform → `post_transform` SQL → Console | 验证一个 SeaTunnel Flink Job 内由 SQL 组织多输入，并在 Python transform 前后继续处理数据 |

两个案例都使用 Flink 1.20.5、本地 `BATCH` 模式和单并行度，目标是验证实现链路，不是生产部署模板。

## 架构流程

### 多 Source、多 Sink 与生命周期动作

```mermaid
flowchart LR
    F1["FakeSource A"] --> T1["Python transform"] --> C1["Console sink"]
    F2["FakeSource B"] --> T2["Python transform"] --> C2["Console sink"]
    PG["PostgreSQL JDBC source"] --> T3["Python transform"] --> DB["PostgreSQL JDBC sink"]
    HTTP["HTTP source"] --> T4["Python transform"] --> C3["Console sink"]
    READY["source pre: jdbc_ready"] -.-> PG
    READY -.-> DB
    DB --> RECON["sink post: jdbc_recon"]
```

Flink 会先校验并执行所有 Source 的 `pre`，再执行所有 Sink 的 `pre`；这些动作完成后才初始化 Source、Transform 和 Sink connector。Job 成功后先执行所有 Sink 的 `post`，再执行所有 Source 的 `post`。配置路径分别是 `lifecycle.source.<plugin_output>.(pre|post)` 和 `lifecycle.sink.<plugin_input>.(pre|post)`。当前案例使用 `jdbc_ready` 检查 PostgreSQL 连通性和 `select 1`，使用 `jdbc_recon` 比较源表和目标表的行数及摘要；`jdbc_recon` 只允许放在 `post` 阶段，校验失败会让任务失败。

生命周期配置会在建图前校验目标是否真实存在、动作类型和必填连接参数。`jdbc_recon` 支持 `source_url/source_driver/source_username/source_password` 与 `sink_url/sink_driver/sink_username/sink_password` 分开配置；如果没有分开配置，则回退到公共的 `url/driver/username/password`。动作可设置正数 `query_timeout_seconds`，避免 readiness、SQL 或 recon 查询无限等待。

### SQL JOIN、Python transform 和后置 SQL

```mermaid
flowchart LR
    L["left_input\nFakeSource"] --> J["pre_transform SQL\nINNER JOIN"]
    R["right_input\nFakeSource"] --> J
    J --> P["Python transform\nper row"]
    P --> Q["post_transform SQL\nfilter / project"]
    Q --> S["Console sink"]
```

SQL 阶段复用 SeaTunnel 已创建的 Flink `StreamExecutionEnvironment`，整个链路只提交一个 Flink Job：

```text
Source → pre_transform SQL → Python Transform → post_transform SQL → Sink
```

每个嵌套 SQL 阶段都必须用 `plugin_input` 显式列出可见输入；增加第三路或更多输入时，增加 Source 并把它加入对应 SQL 阶段的 `plugin_input` 即可，输出仍然是一个 SeaTunnel 表。当前阶段只接受 Flink `BATCH`，避免把没有 watermark、状态 TTL 和恢复策略的无界 JOIN 当成可用的 Streaming 语义。

## 案例文件

- 生命周期配置：[python_transform_multi_source_multi_sink.conf](seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/python_transform_multi_source_multi_sink.conf)
- SQL JOIN 配置：[python_transform_two_source_inner_join.conf](seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/python_transform_two_source_inner_join.conf)
- Python 脚本：[python_transform.py](seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/python_transform.py)
- 本地 HTTP 服务：[http_source_server.py](seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/http_source_server.py)

Python transform 对每行执行以下规则：`name` 去空格并转小写，计算 `age + 1`，再从 `context["config"]` 读取 `source_tag`。

## SQL 阶段配置

SQL JOIN 案例使用显式的嵌套配置：

```hocon
sql {
  pre_transform {
    plugin_input = ["left_input", "right_input"]
    plugin_output = "joined_input"
    query = "SELECT ... FROM left_input l INNER JOIN right_input r ON l.id = r.id"
  }
  post_transform {
    plugin_input = "joined_python"
    plugin_output = "filtered_output"
    query = "SELECT ... FROM joined_python WHERE age_plus_one >= 30"
  }
}

transform {
  Python {
    plugin_input = "joined_input"
    plugin_output = "joined_python"
    # Python transform 配置省略
  }
}
```

旧的顶层 `sql { query = ... }` 配置仍然按 `pre_transform` 处理，并保留“使用所有上游输入”的兼容行为。嵌套 `pre_transform` 和 `post_transform` 必须显式声明 `plugin_input`；`post_transform` 必须声明在 `sql.post_transform` 下。

## PostgreSQL 测试表

连接信息通过环境变量提供，不写入配置文件或 Git：

```bash
export PG_HOST=100.82.226.63
export PG_PORT=30660
export PG_DATABASE=xxt
export PG_USER=root
export PG_PASSWORD='<your-password>'
```

准备生命周期案例使用的测试表和数据：

```sql
CREATE TABLE IF NOT EXISTS public.seatunnel_python_input (
  id INTEGER PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  age INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS public.seatunnel_python_output (
  id INTEGER PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  age INTEGER NOT NULL,
  normalized_name VARCHAR(128),
  age_plus_one INTEGER,
  source_tag VARCHAR(64)
);

TRUNCATE public.seatunnel_python_input, public.seatunnel_python_output;
INSERT INTO public.seatunnel_python_input (id, name, age)
VALUES (301, 'Eve', 28), (302, 'Frank', 35);
```

## 定向编译

只编译本案例所需的模块，不构建全仓库发行包：

```bash
./mvnw -pl \
  seatunnel-connectors-v2/connector-fake,\
  seatunnel-connectors-v2/connector-console,\
  seatunnel-connectors-v2/connector-http/connector-http-base,\
  seatunnel-connectors-v2/connector-jdbc,\
  seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-20,\
  seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter,\
  seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example \
  -am -Dflink.1.20.1.version=1.20.5 -DskipTests -Dskip.spotless=true package
```

Python transform 由 Flink worker 继承的 JVM 参数开启，并限制可执行解释器：

```yaml
env.java.opts.all: >-
  -Dseatunnel.transform.python.enabled=true
  -Dseatunnel.transform.python.allowed-executables=/opt/homebrew/bin/python3
```

`/opt/homebrew/bin/python3` 只是示例，必须替换为运行 Flink worker 的机器上允许的 Python 解释器绝对路径。

本次还执行了以下定向检查：

```bash
./mvnw -pl seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common \
  -Dskip.spotless=true \
  -Dtest=JobLifecycleExecutorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

./mvnw -pl seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter \
  -Dflink.1.20.1.version=1.20.5 \
  -Dskip.spotless=true \
  -Dtest=SqlExecuteProcessorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

./mvnw -pl seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common \
  -DskipTests spotless:check
```

结果：公共 Flink starter 的定向测试共 11 个通过（包含本次新增的生命周期校验和延迟初始化测试），公共模块 Spotless 通过，Flink 1.20 starter 的 `validate` 和 Spotless 通过。SQL 实现已移入 Flink 1.20 starter，公共 Flink starter 只通过反射按配置加载它。当前工作树无法完成 Flink 1.20 starter 的完整 Maven reactor 构建：本地缺少 `seatunnel-translation-flink-20:3.0.0-SNAPSHOT`，且基线的 `seatunnel-config-shade` 源码不完整；因此 SQL 9 个测试和最终运行包仍标记为 `evidence_incomplete`。

## 本地运行

两个案例都需要把 Python 脚本放到配置中的绝对路径：

```bash
cp seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/python_transform.py \
  /tmp/seatunnel-python-transform.py

export FLINK_HOME=/Users/xujiawei/software/flink/flink-1.20.5
export SEATUNNEL_HOME=/path/to/seatunnel-python-transform-runtime
export FLINK_CONF_DIR=$SEATUNNEL_HOME/flink-conf
```

运行目录需要包含 Flink 1.20 translation、Flink 20 starter、Python transform、Fake/HTTP/Console/JDBC connector、PostgreSQL JDBC driver 和对应配置文件。

运行多 Source、多 Sink 生命周期案例。在一个终端启动 HTTP 服务，在另一个终端提交 Job：

```bash
python3 seatunnel-examples/seatunnel-flink-examples/seatunnel-flink-20-example/src/main/resources/examples/http_source_server.py
```

另一个终端执行：

```bash
$SEATUNNEL_HOME/bin/start-seatunnel-flink-20-connector-v2.sh \
  --master local \
  --deploy-mode run \
  -c $SEATUNNEL_HOME/config/python_transform_multi_source_multi_sink.conf \
  --name PythonTransformLifecycleSmoke
```

运行 SQL JOIN、Python transform、后置 SQL 案例：

```bash
$SEATUNNEL_HOME/bin/start-seatunnel-flink-20-connector-v2.sh \
  --master local \
  --deploy-mode run \
  -c $SEATUNNEL_HOME/config/python_transform_two_source_inner_join.conf \
  --name PythonTransformPrePostSqlPoc
```

## 实际验证结果

### 生命周期案例

在本机 Flink 1.20.5、局域网 PostgreSQL 和本地 HTTP 服务上运行成功：

- Job exit code：`0`
- `SourceReceivedCount = 8`：Fake 4 行、PostgreSQL 2 行、HTTP 2 行
- `SinkWriteCount = 8`：三个 Console 分支和一个 PostgreSQL 分支
- PostgreSQL `jdbc_ready` 在 Job 前执行，`jdbc_recon` 在 Job 成功后执行
- PostgreSQL sink 结果：

```text
301,Eve,28,eve,29,postgres
302,Frank,35,frank,36,postgres
```

字段顺序为 `id, name, age, normalized_name, age_plus_one, source_tag`。

### SQL JOIN 案例

在本机 Flink 1.20.5 上运行成功：

- Job exit code：`0`
- JobID：`dcf6c4eef8530dde60da224fc2737d0c`
- `SourceReceivedCount = 4`、`SinkWriteCount = 1`
- 两路 FakeSource 各发送两行，`pre_transform` SQL 按 `id` 关联得到两行
- Python transform 增加 `normalized_name`、`age_plus_one`、`source_tag`
- `post_transform` SQL 过滤出一行

最终 Console 输出：

```text
302 | Frank | 35 | 302 | HR | 2 | frank | 36 | joined
```

字段顺序为 `id, name, age, right_id, right_name, right_age, normalized_name, age_plus_one, source_tag`。

## 私有包

二进制包不提交到 Git。完成本地 runtime 组装后，私有压缩包和校验文件约定放在：

```text
seatunnel-dist/target/seatunnel-dist-private-3.0.0-SNAPSHOT.tar.gz
seatunnel-dist/target/seatunnel-dist-private-3.0.0-SNAPSHOT.tar.gz.sha256
```

包内只放本案例所需的 Flink 1.20.5 运行文件、连接器、配置和 `config/http_source_server.py`。如果目标路径不存在，需要先完成定向构建和 runtime 组装；仓库本身不追踪该归档文件。

## 代码位置

- Flink 执行顺序和 SQL 阶段：[FlinkExecution.java](seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common/src/main/java/org/apache/seatunnel/core/starter/flink/execution/FlinkExecution.java)
- SQL 阶段解析和执行：[SqlExecuteProcessor.java](seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/main/java/org/apache/seatunnel/core/starter/flink/execution/SqlExecuteProcessor.java)
- SQL 阶段测试：[SqlExecuteProcessorTest.java](seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/src/test/java/org/apache/seatunnel/core/starter/flink/execution/SqlExecuteProcessorTest.java)

## 限制与未验证项

- 当前 SQL JOIN 验证使用 Flink `BATCH`、单并行度和有限的 FakeSource 数据。
- 当前 SQL 阶段在 `STREAMING` 模式会直接失败；要支持无界 JOIN，还需要明确 watermark、状态 TTL、checkpoint 和恢复语义。
- 本次没有覆盖 PostgreSQL CDC、时间窗口、状态 TTL、checkpoint 恢复、多级 JOIN 或大规模数据。
- `jdbc_recon` 当前比较单行聚合结果；生产环境仍需按业务主键、重复执行和失败恢复策略补充验收。
- Flink 1.20 以下版本、未列出的 connector 和全仓库发行包不在本分支构建范围内。
