# 事件说明

[English](events.md) | 简体中文

[文档目录](README.zh-CN.md)

读完本页，可以知道一次数据库修改会产生什么事件、每个输出字段是什么意思，以及 MINIMAL 和 FULL 有什么区别。无需先了解 Rust 类型。

## 从一次修改开始

假设来源 `east` 中的 `app.users` 表有以下记录，`id` 是无符号整数主键，`age` 允许为 NULL：

| id | name | age | status |
| --- | --- | --- | --- |
| 42 | Alice | 20 | active |

应用执行更新，并提交事务：

```sql
UPDATE app.users SET name = 'Bob', age = NULL WHERE id = 42;
```

mysql-cdc 会经过三个步骤：

1. **读取数据库日志**：mysql-replication 读取 MySQL 记录的行数据和事务标记，生成原始 binlog 事件。
2. **整理成变更通知**：mysql-canal 找出来源、库表、主键和记录过的字段，等事务提交后交付。
3. **输出通知**：Pikachu 默认把变更写成每行一条的 JSON。使用库时，可以自行处理提交的事务。

在 MINIMAL 模式下，这次更新的输出是：

```json
{
  "source": "east",
  "schema": "app",
  "table": "users",
  "operation": "update",
  "pk": { "type": "unsigned", "value": 42 },
  "changed": ["name", "age"]
}
```

这条通知表示：**east 来源的 app.users 表中，主键为 42 的记录发生了更新，日志记录了 name 和 age 两个字段。** 通知不包含 Bob 或 NULL 等非主键字段值；需要查看原始行值时，使用复制库的行镜像读取功能。

为便于阅读，本页将 JSON 展开显示；CLI 实际每条记录输出一行。

## 通用事件中的字段

通用事件是应用消费的变更通知。下面这些字段在 MINIMAL 和 FULL 下都存在，含义也相同。

| 字段 | 表示什么 | 示例 |
| --- | --- | --- |
| `source` | 配置中的来源名称，用于区分不同 MySQL 实例 | `east` |
| `schema` | 数据库名 | `app` |
| `table` | 表名 | `users` |
| `operation` | 新增、更新或删除 | `insert`、`update`、`delete` |
| `pk` | 用来定位这条记录的主键；主键变更时为新值 | 主键 42 |
| `previous_pk` | 主键变更前的值，仅在主键确实变更时提供 | 主键从 42 变成 43 时为 42 |
| `changed` | 日志在修改后的数据中记录了哪些字段，按表列顺序列出 | `name`、`age` |

主键同时携带类型和值。例如 `{"type":"unsigned","value":42}` 是无符号整数，`{"type":"text","value":"user-42"}` 是文本，`{"type":"decimal","value":"123.40"}` 是保留小数位的精确数值。有符号整数使用 `signed`。

**`changed` 不表示系统逐个比较新旧值后发现的差异。** 它表示 MySQL 在修改后的日志数据中记录了这些字段；字段值可能与修改前相同。

## MINIMAL 和 FULL 记录了什么

“行镜像”就是日志保存的那部分行数据。`before` 表示修改前的数据，`after` 表示修改后的数据。

仍以上面那次更新为例，原始日志中的信息可以这样理解。此表是内容说明，不是系统额外输出的一种格式：

| 数据 | MINIMAL：修改前 | MINIMAL：修改后 | FULL：修改前 | FULL：修改后 |
| --- | --- | --- | --- | --- |
| id | 42 | 未记录 | 42 | 42 |
| name | 未记录 | Bob | Alice | Bob |
| age | 未记录 | NULL | 20 | NULL |
| status | 未记录 | 未记录 | active | active |

对本系统支持的单列主键表，MINIMAL 更新前的数据用主键定位记录，修改后只含日志实际记录的列。FULL 的每份行镜像包含全部列。

两种模式的 **通用事件字段不变**，上例的区别是：

| 内容 | MINIMAL | FULL |
| --- | --- | --- |
| `operation` | `update` | `update` |
| `pk` | 无符号整数 42 | 无符号整数 42 |
| `changed` | `name`、`age` | `id`、`name`、`age`、`status` |
| 是否输出完整行值 | 否 | 否 |

因此，FULL 中 `status` 即使仍是 active，也会出现在 `changed` 中。它在完整的 after 镜像中被记录了。

模式在配置中明确选择。程序会检查上游设置与输入数据是否符合要求；它不会替 MySQL 修改配置，也不会给 MINIMAL 数据补齐缺失的值。原始复制读取不要求表有主键，其 MINIMAL 读取也可以接受完整镜像；上面的主键定位要求属于 Canal 的 CDC 规则。

### 未记录与 NULL 的区别

| 情况 | 含义 | 能否当作数据库中的空值 |
| --- | --- | --- |
| 字段未记录 | 这次日志没有携带该字段，无法据此知道它的值 | 不能 |
| 字段记录为 NULL | 日志明确表示这个字段是 SQL NULL | 可以 |
| 整份镜像不存在 | 这种操作没有修改前或修改后的记录 | 不能当作一行空值 |

上例 MINIMAL 中的 `status` 是“未记录”，`age` 的修改后值是“NULL”。二者不能混为一谈。

## 新增、更新、删除分别怎样表示

| 操作 | 修改前的数据 | 修改后的数据 | 通用事件的 `changed` |
| --- | --- | --- | --- |
| 新增 `insert` | 不存在 | 存在 | 修改后日志中实际记录的字段 |
| 更新 `update` | 存在 | 存在 | 修改后日志中实际记录的字段 |
| 删除 `delete` | 存在 | 不存在 | 空列表 |

“存在”不代表包含完整行；包含哪些字段取决于模式和该事件实际携带的数据。INSERT 也按实际记录的字段处理，不在消费端假定它必定包含所有列。

例如删除主键 42 的记录，输出为：

```json
{
  "source": "east",
  "schema": "app",
  "table": "users",
  "operation": "delete",
  "pk": { "type": "unsigned", "value": 42 },
  "changed": []
}
```

如果更新把主键从 42 改为 43，通知会同时提供新旧主键。下面展示 MINIMAL 模式下只修改主键的例子：

```json
{
  "source": "east",
  "schema": "app",
  "table": "users",
  "operation": "update",
  "pk": { "type": "unsigned", "value": 43 },
  "previous_pk": { "type": "unsigned", "value": 42 },
  "changed": ["id"]
}
```

## 原始 BinlogEvent 又包含什么

原始事件描述 MySQL 日志本身，除了行修改，还包含事务开始与提交、日志轮换和格式说明。一个 SQL 可以产生多个事件，一个行事件也可以包含多条记录。

每个原始事件都有三个共用部分，两种模式下都一样：

| 部分 | 含义 |
| --- | --- |
| 原始数据 `raw_data` | 从日志读取的完整事件字节，包含实际校验信息 |
| 事件头 `header` | 事件时间、类型、生产者 ID、长度、日志位置和标志 |
| 事件内容 `event` | 这是一条行修改、表说明、提交标记，还是其他事件 |

事件头中的公共字段如下。它们描述日志，不是业务行里的字段：

| 字段 | 含义 | 读者需要注意什么 |
| --- | --- | --- |
| `timestamp` | 生产者记录的时间，单位为 Unix 秒 | 不等于消费程序收到事件的时间 |
| `event_type` | 事件种类 | 行修改和提交标记是不同事件 |
| `server_id` | 生产者记录的服务器 ID | 不等于配置里的来源名称 `source` |
| `event_size` | 完整事件占用的字节数 | 包含头部及实际校验信息 |
| `log_pos` | 日志记录的位置，通常指向下一条事件 | 不等于已经消费确认的检查点 |
| `flags` | 协议标志 | 用于协议解析和诊断 |

常见事件可以按用途理解：

| 事件 | 作用 | 主要内容 |
| --- | --- | --- |
| 格式说明（FDE） | 告诉读取方怎样理解这个日志文件 | 生产者版本、事件布局、校验方式 |
| 表映射（TABLE_MAP） | 告诉读取方后面的行属于哪张表、怎样解释列 | 临时表编号、库表名、列类型信息 |
| 行修改（ROWS） | 记录新增、更新、删除 | 表编号、操作、修改前后数据 |
| 语句（QUERY） | 记录 SQL 语句或事务边界 | SQL、默认数据库等 |
| 事务提交（XID） | 确认事务已提交 | 事务标识 |
| 日志轮换（ROTATE） | 切换到下一个日志文件 | 下个文件名与起点 |
| GTID / 心跳 | 描述事务身份或维持复制连接 | 事务标识、相关元数据或日志文件名 |

MINIMAL/FULL 的内容差异发生在行修改的前后数据中；公共头和这些事件种类不会因此增加或减少。复制库可以读取包含 DDL 的语句事件，Canal 遇到不支持的语句会停止。

## MINIMAL 与 FULL 的原始事件内容

这里展开复制库新增的泛型行事件。`BinlogEvent<'a, Minimal>` 和 `BinlogEvent<'a, Full>` 是编译期区分的类型；差异沿 `EventData::Rows`、`RowsEvent<M>`、`Row<M>` 一直传到镜像类型。它们不是在同一事件上设置一个运行时模式字段。

### 行事件共用的内容

| 内容 | 访问方式 | 两种模式的含义 |
| --- | --- | --- |
| 完整日志字节 | `event.raw_data` | 原始头、body 和实际 checksum |
| 公共事件头 | `event.header` | 前文列出的时间、事件码、服务器 ID、大小、位点和标志 |
| 行事件载荷 | `event.event` 中的 `EventData::Rows(rows)` | `RowsEvent<'a, M>`，保留相同的模式参数 |
| 临时表编号 | `rows.table_id` | 关联该来源之前的 TABLE_MAP |
| 行事件标志和操作 | `rows.flags`、`rows.kind` | `RowsKind::Write`、`Update` 或 `Delete` |
| 编码中的镜像数据 | `rows.encoded_images` | 借用的 extra-data、列位图及字段值字节，尚不是逐行对象 |
| 历史表定义 | `rows.table()?`、`rows.columns()?` | 库名、表名和全部列定义；由 parser 自动绑定 TABLE_MAP |
| 解码后的行操作 | `rows.rows()?` | 迭代 `Result<Row<'a, M>>`；每个 `Row.operation` 持有适用的前后镜像 |

以下仍使用 `UPDATE app.users SET name = 'Bob', age = NULL WHERE id = 42`。表列索引依次为 `0:id`、`1:name`、`2:age`、`3:status`，其中 name 和 status 为 VARCHAR，age 为可空 INT。为展示列名，假设 TABLE_MAP 已携带列名元数据；树中的值按可读形式展示，不是 `dump()` 输出或额外的 JSON 格式。

### MINIMAL

类型为 `BinlogEvent<'_, Minimal>`，每个已解码行的操作类型为 `RowOperation<SparseImage<'_>>`：

```text
BinlogEvent<Minimal>
└── event: EventData::Rows(RowsEvent<Minimal>)
    ├── table(): app.users
    ├── columns(): [id, name, age, status]
    └── rows(): Row<Minimal>
        └── operation: RowOperation::Update
            ├── before: SparseImage
            │   ├── column_indices(): [0]
            │   └── fields(): id=42
            └── after: SparseImage
                ├── column_indices(): [1, 2]
                └── fields(): name=Bob, age=NULL
```

`SparseImage` 保存日志实际记录的字段；`columns()` 仍包含四列定义，`column_count()` 仍为 4，不会因为只记录了两列而变成 2。`column_indices()` 是表列的绝对索引，不是压缩后的字段序号。

| 对这次更新的访问 | 返回内容 |
| --- | --- |
| `before.field(0)?` | `Some(Field)`，值为整数 42 |
| `before.field(1)?` | `None`，修改前的 name 未记录 |
| `after.field(0)?` | `None`，修改后的 id 未记录 |
| `after.field(1)?` | `Some(Field)`，值为 `RowValue::Bytes(b"Bob")` |
| `after.field(2)?` | `Some(Field)`，值为 `RowValue::Null` |
| `after.field(3)?` | `None`，修改后的 status 未记录 |
| `after.field(4)` | `Err(Error::Invalid(_))`，索引超出表列范围 |

`None` 只表示这份镜像没有记录该合法列；列定义仍能从 `columns()[index]` 查询，值不会被填为 NULL。原始 MINIMAL 读取器也接受完整镜像，因此上表描述的是这条具体日志，不是所有 MINIMAL 事件都固定缺少这些列。

### FULL

类型为 `BinlogEvent<'_, Full>`，每个已解码行的操作类型为 `RowOperation<CompleteImage<'_>>`：

```text
BinlogEvent<Full>
└── event: EventData::Rows(RowsEvent<Full>)
    ├── table(): app.users
    ├── columns(): [id, name, age, status]
    └── rows(): Row<Full>
        └── operation: RowOperation::Update
            ├── before: CompleteImage
            │   └── fields(): id=42, name=Alice, age=20, status=active
            └── after: CompleteImage
                └── fields(): id=42, name=Bob, age=NULL, status=active
```

每份实际存在的 `CompleteImage` 必须包含全部四列。`field(index)` 返回 `Result<Field>`，范围内字段不会因为未记录而返回 `None`：

| 对这次更新的访问 | 返回内容 |
| --- | --- |
| `before.field(1)?` | `Field`，值为 `RowValue::Bytes(b"Alice")` |
| `after.field(0)?` | `Field`，值为整数 42 |
| `after.field(1)?` | `Field`，值为 `RowValue::Bytes(b"Bob")` |
| `after.field(2)?` | `Field`，值为 `RowValue::Null` |
| `after.field(3)?` | `Field`，值为 `RowValue::Bytes(b"active")`，即使值未变 |
| `after.field(4)` | `Err(Error::Invalid(_))`，索引超出表列范围 |

FULL 遇到缺列的镜像会返回 `Error::Unsupported`，不会把稀疏数据伪装成完整行。SQL NULL 仍然是一个已记录字段的值，不是字段缺失。

### 三种行操作与两种镜像类型

| 操作 | `Row<Minimal>.operation` | `Row<Full>.operation` |
| --- | --- | --- |
| INSERT | `Insert { after: SparseImage }` | `Insert { after: CompleteImage }` |
| UPDATE | `Update { before: SparseImage, after: SparseImage }` | `Update { before: CompleteImage, after: CompleteImage }` |
| DELETE | `Delete { before: SparseImage }` | `Delete { before: CompleteImage }` |

表中省略 `RowOperation::` 前缀。INSERT 没有 before，DELETE 没有 after；UPDATE 必须同时包含两份镜像。这些差异由枚举变体表达，不通过一对可能同时为空的字段表示。MINIMAL INSERT 按实际列位图读取，不能假定它总是完整行。

### 每个已记录字段包含什么

两种模式都返回 `Field { column: &ColumnInfo, value: RowValue }`：

| 内容 | 字段或方法 | 说明 |
| --- | --- | --- |
| 列索引 | `column.index` | 从零开始的表列绝对索引 |
| 列名 | `column.name` | `Option<Box<str>>`，未记录时为 `None` |
| 列类型与布局 | `column.definition` | `wire_type()`、`column_type()`、`nullable()`、`metadata()` |
| 数字符号 | `column.unsigned` | 可选元数据；缺失时不猜测 signed/unsigned |
| 字符排序规则 | `column.collation_id` | 可选 collation ID |
| 主键元数据 | `column.primary_key`、`primary_key_prefix` | 可选成员关系及前缀长度；不增加原始读取的主键要求 |
| 此次记录的值 | `value` | `RowValue::Null`、`Integer`、`Bytes`、`Decimal` 等 |

例如 after 中的 name 字段同时提供索引 1、可用的列名及类型定义，以及借用的 `RowValue::Bytes(b"Bob")`。整数通过 `RowValue::Integer` 保留原始宽度，按已知符号属性使用 `signed()` / `unsigned()`；文本是借用字节，DECIMAL 保留精度与 scale。

**FULL 表示镜像完整，不表示元数据齐全。** 两种模式的列名、符号、collation 和主键属性都只来自日志；可选属性缺失时保持 `None`，不会查询当前数据库来补齐。不同事件共享历史列定义，字段值借用原始事件缓冲区。`into_owned()` / `view()` 保留模式、列定义和实际 checksum。

`EventData::RowsStatementEnd` 是单独的语句结束哨兵，没有表列和行镜像。FDE、QUERY、XID、ROTATE 等控制事件在两种模式下保持相同的内容。

### 转换到 Canal 后交付什么

上述原始事件中的 `name=Bob`、`age=NULL` 等字段值可以由复制库读取。Canal 的 `Change` / `OwnedChange` 仍只交付表、操作、精确主键、可选旧主键及 `changed` 列索引，不附带 `SparseImage`、`CompleteImage` 或完整行值。上例 MINIMAL 的 `changed` 是 `[1, 2]`，FULL 为 `[0, 1, 2, 3]`；CLI 再将其映射为字段名。

按类型匹配 `EventData::Rows`、读取字段以及处理借用生命周期的代码见 [Rust 事件 API 参考](event-api.zh-CN.md#按模式读取具体行操作)。

## 什么时候收到事件，失败后会怎样

使用 Canal 或 Pikachu 时，应用在事务提交后才收到变更。事务回滚则丢弃这批变更；不属于白名单的行不会输出。库的事务处理器仍会收到完全过滤的已提交事务，其中变更列表为空。

处理器成功后才保存检查点。若处理器已产生外部副作用，而程序在保存检查点前退出，重启后可能再次交付这批数据。消费方需要能安全处理重复通知。CLI 刷新标准输出不等于下游已经持久保存数据。

同一来源按日志顺序交付，同一个事务不会被其他来源的事务打断。不同 MySQL 实例之间没有全局时间顺序。

直接读取原始 BinlogEvent 时，看到行修改不代表事务已经提交。需要提交后交付的应用应使用 Canal 的事务消费接口。

## 支持范围与停止条件

CDC 要求单列主键，支持整数、精确 DECIMAL，以及 utf8、utf8mb3、utf8mb4、ascii、latin1 的 CHAR/VARCHAR。联合主键、无主键和未支持的主键类型会报错。大整数消费方应使用能保留精度的 JSON 解析器。

遇到表结构变更（DDL）、不支持的语句或格式、损坏/截断日志、缺失主键、字段元数据不匹配，程序会停止。未确认的事务不会推进磁盘检查点。NOT NULL 列出现 NULL 也会报错，不会因为它不是主键而忽略。

DDL 不会自动刷新表结构并继续。恢复会使用检查点保存的历史表结构解释历史日志。不要删除检查点来跳过未处理的数据。

## 继续阅读

- [配置与运行](configuration.zh-CN.md)：选择模式、准备 MySQL、恢复和回放。
- [系统如何工作](architecture.zh-CN.md)：分层、事务和多来源的配合。
- [Rust 事件 API 参考](event-api.zh-CN.md)：完整字段、泛型接口、访问方式与借用生命周期。
