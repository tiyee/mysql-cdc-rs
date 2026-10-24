# Documentation

English | [简体中文](README.zh-CN.md)

## Use the system

Start with the [system overview and examples](../README.md). Understand the notifications, then connect your database.

| I want to understand | Read |
| --- | --- |
| What a data change produces and how MINIMAL/FULL differ | [Understanding events](events.md) |
| What the three layers do and why delivery waits for commit | [How the system works](architecture.md) |
| How to connect MySQL, select tables, consume and recover | [Configuration and operation](configuration.md) |

## Develop and maintain

Consult these references when integrating Rust libraries or changing the implementation.

| Reference | Contents |
| --- | --- |
| [Rust event API](event-api.md) | Complete fields, types, generics, borrowing lifetimes and low-level connections |
| [Development](development.md) | Source layout, implementation flow, performance and plugins |
| [Testing](testing.md) | Local checks, dedicated MySQL tests and coverage |
| [Releases](releasing.md) (Chinese) | Initial release, CI, packaging and OIDC |
