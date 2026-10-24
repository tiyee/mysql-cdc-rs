# 文档目录

[English](README.md) | 简体中文

## 使用系统

从[系统介绍与示例](../README.zh-CN.md)开始；理解通知后，再配置自己的数据库。

| 我想了解 | 阅读 |
| --- | --- |
| 修改一条数据会收到什么，MINIMAL/FULL 有什么区别 | [事件说明](events.zh-CN.md) |
| 新增的 `BinlogEvent<Minimal>` / `BinlogEvent<Full>` 具体包含哪些字段和值 | [MINIMAL JSON](events.zh-CN.md#minimal-json) / [FULL JSON](events.zh-CN.md#full-json) |
| 三层各做什么，为什么提交后才收到通知 | [系统如何工作](architecture.zh-CN.md) |
| 怎样连接 MySQL、选择表、消费和恢复 | [配置与运行](configuration.zh-CN.md) |

## 开发和维护

需要接入 Rust 库或修改实现时，再查阅这些文档。

| 文档 | 内容 |
| --- | --- |
| [Rust 事件 API 参考](event-api.zh-CN.md) | 完整字段、类型、泛型、借用生命周期及底层连接接口 |
| [开发指南](development.zh-CN.md) | 源码目录、实现数据流、性能设计与插件扩展 |
| [测试指南](testing.zh-CN.md) | 本地检查、专用 MySQL 测试与覆盖范围 |
| [发布指南](releasing.md) | 首次发布、CI、打包和 OIDC |
