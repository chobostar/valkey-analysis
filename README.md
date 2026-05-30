# Valkey Analysis

Глубокий анализ внутренней архитектуры **Valkey 9.1** — форка Redis OSS 7.2.4, развиваемого сообществом под лицензией BSD.

Репозиторий содержит детальные технические документы, описывающие устройство Valkey на уровне исходного кода, а также отчёт о готовности Kubernetes-операторов для продакшена.

---

## Структура

### `internals/` — Устройство Valkey

Технические документы, основанные на разборе исходного кода Valkey 9.1 (`src/server.c`, `src/replication.c`, `src/cluster_legacy.c` и др.).

| Файл | Тема | Краткое содержание |
|---|---|---|
| [`01-architecture.md`](internals/01-architecture.md) | Архитектура ядра | Процесс, event loop (ae), модель потоков, обработка команд, управление памятью (zmalloc, eviction, defrag, lazyfree), персистентность (RDB/AOF), copy-on-write |
| [`02-replication.md`](internals/02-replication.md) | Репликация | Протокол PSYNC, replication backlog, diskless и dual-channel репликация, продвижение реплики, chain replication, сценарии отказов |
| [`03-clustering.md`](internals/03-clustering.md) | Кластеризация и шардирование | 16384 хеш-слотов, gossip-протокол, обнаружение отказов (PFAIL→FAIL), миграция слотов (традиционная и атомарная 9.0+), редиректы (MOVED/ASK/TRYAGAIN), фейловер, multi-DB cluster mode |
| [`04-major-versions.md`](internals/04-major-versions.md) | Эволюция версий | Redis 7.2 → Valkey 8.0 → 8.1 → 9.0 → 9.1. Таблица фич, breaking changes, deprecated API, рекомендации по апгрейду |
| [`05-self-healing-and-admin.md`](internals/05-self-healing-and-admin.md) | Самовосстановление и эксплуатация | Что лечится автоматически, что требует вмешательства администратора, runbooks (отказ ноды, OOM, репликация, персистентность), мониторинг и чеклисты |
| [`overview-replication-and-sharding.md`](internals/overview-replication-and-sharding.md) | Обзор репликации и шардирования | Сводный документ, объединяющий ключевые аспекты репликации и кластеризации в одном месте |

### `reports/` — Аналитические отчёты

| Файл | Описание |
|---|---|
| [`valkey-operators-production-readiness-report.md`](reports/valkey-operators-production-readiness-report.md) | Сравнительный анализ трёх Kubernetes-операторов для Valkey (valkey-io, SAP, Hyperspike). Оценка готовности для управляемого сервиса на 1000 разработчиков. Включает deep-dive по каждому оператору, матрицу фич, стратегию интеграции с Crossplane и перечень ручных runbooks |

---

## Ключевые темы

- **Однопоточный event loop** с возможностью оффлоада I/O на отдельные потоки (lock-free MPSC очереди в 9.1)
- **Асинхронная репликация** с общим backlog'ом и поддержкой partial resync (PSYNC)
- **Автоматическое шардирование** через 16384 хеш-слота с gossip-протоколом для обнаружения топологии
- **Атомарная миграция слотов** (Valkey 9.0+) — замена поключевой MIGRATE на snapshot-based перенос
- **Два механизма персистентности**: RDB (снапшоты) и AOF (лог команд), работающие одновременно
- **Copy-on-write оптимизации** через `madvise(MADV_DONTNEED)` в дочерних процессах

---

## Версия

Документация основана на **Valkey 9.1.0** (май 2026).
