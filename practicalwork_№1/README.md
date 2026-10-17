# Локальный Lakehouse: TPC-H Q18, ELT и сравнение GZIP/ZSTD

**Автор:** Попова Софья Сергеевна  
**Практическая работа №1, вариант 3:** TPC-H Q18 «Large Volume Customer»  
**Масштаб:** `sf1`

## Цель и архитектура

Развернуть локальный Lakehouse, выполнить ELT по модели Bronze → Silver → Gold и сравнить размер данных, время записи и скорость Q18 для Parquet с GZIP и ZSTD.

```text
TPC-H sf1 (Trino)
    │ CTAS
    ▼
Bronze (Iceberg/Parquet, MinIO) ──► Silver (large_orders) ──► Gold (q18_top100)
    ▲                                      ▲                      ▲
    └────────── Trino SQL ─────────────────┴──────────────────────┘
                       │
                  Lakekeeper (Iceberg REST Catalog)
                       │
                 PostgreSQL (метаданные каталога)

Superset SQL Lab ──► Trino ──► Bronze
```

Сервисы: MinIO (S3-хранилище), PostgreSQL (метаданные Lakekeeper), Lakekeeper (Iceberg REST Catalog), Trino 476 (SQL и TPC-H), Superset (проверка чтения Bronze). Данные хранятся в Docker-томах `minio-data`, `pg-data`, `superset-data`.

## Состав репозитория

```text
.
├── docker-compose.yml
├── catalog/
│   ├── bronze.properties
│   ├── silver.properties
│   ├── gold.properties
│   └── tpch.properties
├── create-silver.json
├── create-gold.json
├── README.md
└── отчет.md
```

Файлы `create-silver.json` и `create-gold.json` использовались при настройке складов Lakekeeper. **Не публикуйте реальные секреты:** учётные данные в примере предназначены исключительно для локального учебного стенда.

## Запуск

Требуются Docker Desktop и Docker Compose. В PowerShell из каталога проекта:

```powershell
docker compose up -d
docker compose ps
```

Адреса: MinIO Console — http://localhost:9001, Lakekeeper — http://localhost:8181/ui, Trino — http://localhost:8080, Superset — http://localhost:8088.

**Первичная настройка:** один `docker compose up -d` запускает сервисы, но **не создаёт автоматически** склады Lakekeeper и таблицы. В Lakekeeper необходимо настроить три склада `bronze`, `silver`, `gold` с S3-compatible MinIO endpoint `http://minio:9000`, бакетами с соответствующими именами, регионом `us-east-1` и path-style доступом. В MinIO создать бакеты `bronze`, `silver`, `gold`. Склады Silver и Gold в выполненном эксперименте создавались через API Lakekeeper с JSON-конфигурациями из репозитория. Конкретные API-запросы первичной настройки в репозитории не автоматизированы; при повторном развёртывании потребуется настроить склады и сверить параметры `catalog/*.properties`.

Для остановки без удаления данных: `docker compose stop`; для продолжения: `docker compose start`. **Не использовать `docker compose down -v`: эта команда удаляет Docker-тома.**

## ELT: Bronze → Silver → Gold

Команды ниже выполняются в **VS Code → PowerShell**. Для базового Bronze используется ZSTD; параметр `compression_codec` — сессионное свойство каталога Iceberg в Trino, а не свойство `WITH` таблицы.

### Bronze — исходные таблицы TPC-H

```powershell
docker exec trino trino --session bronze.compression_codec=ZSTD --execute "CREATE TABLE bronze.lakehouse.customer WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.customer"
docker exec trino trino --session bronze.compression_codec=ZSTD --execute "CREATE TABLE bronze.lakehouse.orders WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.orders"
docker exec trino trino --session bronze.compression_codec=ZSTD --execute "CREATE TABLE bronze.lakehouse.lineitem WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.lineitem"
```

Строк: `customer` — 150 000; `orders` — 1 500 000; `lineitem` — 6 001 215.

### Silver — агрегат заказов с большим количеством товаров

```powershell
docker exec trino trino --execute "CREATE TABLE silver.lakehouse.large_orders WITH (format = 'PARQUET') AS SELECT orderkey, SUM(quantity) AS total_quantity FROM bronze.lakehouse.lineitem GROUP BY orderkey HAVING SUM(quantity) > 300"
```

Результат: **57 строк**. Это предварительная агрегация и фильтрация Q18.

### Gold — витрина Q18

```powershell
docker exec trino trino --execute "CREATE TABLE gold.lakehouse.q18_top100 WITH (format = 'PARQUET') AS SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, s.total_quantity FROM silver.lakehouse.large_orders s JOIN bronze.lakehouse.orders o ON s.orderkey = o.orderkey JOIN bronze.lakehouse.customer c ON o.custkey = c.custkey ORDER BY o.totalprice DESC, o.orderdate LIMIT 100"
```

Результат: **57 строк** (критерию `SUM(quantity) > 300` удовлетворяют 57 заказов). Витрина содержит `name`, `custkey`, `orderkey`, `orderdate`, `totalprice`, `total_quantity`.

## Методика бенчмарка

**Данные:** TPC-H `sf1`; формат Parquet; сравниваются три таблицы `customer`, `orders`, `lineitem` без партиционирования. Базовые таблицы — ZSTD. Для GZIP созданы копии с суффиксом `_gzip`:

```powershell
docker exec trino trino --session bronze.compression_codec=GZIP --execute "CREATE TABLE bronze.lakehouse.customer_gzip WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.customer"
docker exec trino trino --session bronze.compression_codec=GZIP --execute "CREATE TABLE bronze.lakehouse.orders_gzip WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.orders"
docker exec trino trino --session bronze.compression_codec=GZIP --execute "CREATE TABLE bronze.lakehouse.lineitem_gzip WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.lineitem"
```

**Скорость Q18:** полный запрос с двумя чтениями `lineitem` (внешний JOIN и подзапрос с HAVING), JOIN `orders` и `customer`, `GROUP BY`, `ORDER BY`, `LIMIT 100`; выполнялся через `EXPLAIN ANALYZE` по три раза на каждом наборе таблиц. Считывалось значение `Execution` из вывода Trino. Для ZSTD использовались `bronze.lakehouse.customer/orders/lineitem`, для GZIP — соответствующие `_gzip`.

```sql
EXPLAIN ANALYZE
SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice,
       SUM(l.quantity) AS total_quantity
FROM bronze.lakehouse.customer c
JOIN bronze.lakehouse.orders o ON c.custkey = o.custkey
JOIN bronze.lakehouse.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
    SELECT orderkey
    FROM bronze.lakehouse.lineitem
    GROUP BY orderkey
    HAVING SUM(quantity) > 300
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;
```

Для GZIP заменить в этом SQL три имени таблиц на `customer_gzip`, `orders_gzip`, `lineitem_gzip` (включая таблицу в подзапросе).

**Размер:** `mc du --recursive` по каталогам `data` соответствующих таблиц в бакете Bronze, без файлов метаданных Iceberg. Размеры округлены инструментом до MiB. Тестовые копии `customer_zstd` и `write_test_*` в сумму размера трёх таблиц не включались.

**Запись:** один запуск `EXPLAIN ANALYZE CREATE TABLE ... AS SELECT * FROM tpch.sf1.customer` с сессионными кодеками GZIP и ZSTD, по 150 000 строк; измерено `Execution`. Это разовые замеры, а не статистическая оценка.

| Показатель | GZIP | ZSTD |
|---|---:|---:|
| Размер трёх таблиц Bronze | ≈ 185,7 MiB | ≈ 189,8 MiB |
| Q18: запуск 1 | 5,55 с | 2,97 с |
| Q18: запуск 2 | 3,51 с | 2,08 с |
| Q18: запуск 3 | 2,23 с | 2,52 с |
| **Q18: среднее** | **3,76 с** | **2,52 с** |
| Запись `customer` (один запуск) | 3,71 с | 1,71 с |

В данном окружении GZIP обеспечил чуть меньший размер файлов, а ZSTD — лучшее среднее время Q18 и более быстрое создание тестовой таблицы. Время зависит от кэша, нагрузки ноутбука и других условий, поэтому результаты не следует обобщать на все системы.

## Schema Evolution и Time Travel

```sql
CREATE TABLE silver.lakehouse.schema_demo WITH (format = 'PARQUET') AS
SELECT custkey, name FROM bronze.lakehouse.customer LIMIT 5;

ALTER TABLE silver.lakehouse.schema_demo ADD COLUMN comment VARCHAR;
-- Все 5 строк сохраняются, в новом столбце сначала NULL.

INSERT INTO silver.lakehouse.schema_demo (custkey, name, comment)
VALUES (999999, 'Test Customer', 'Added after snapshot');

SELECT COUNT(*) FROM silver.lakehouse.schema_demo; -- 6
SELECT COUNT(*) FROM silver.lakehouse.schema_demo
FOR VERSION AS OF 5394195565075771969; -- 5 в выполненном эксперименте
```

`snapshot_id` относится к конкретной выполненной таблице: при новом развёртывании его нужно получить заново из метаданных Iceberg, а не копировать прежнее значение. Пример чтения метаданных: `SELECT snapshot_id, committed_at, operation FROM silver.lakehouse."schema_demo$snapshots"` (при передаче через PowerShell/Docker потребуются корректные кавычки).

## Дополнительный инструмент: Superset

Superset запускается из `docker-compose.yml` на порту 8088. Для связи с Trino нужны пакеты `trino` и `sqlalchemy-trino`; команда запуска сервиса устанавливает их через `uv` в Python-окружение Superset. После первичной инициализации Superset и создания локального администратора в веб-интерфейсе создано подключение:

```text
trino://admin@trino:8080/bronze/lakehouse
```

В SQL Lab выполнено:

```sql
SELECT COUNT(*) AS customer_count FROM bronze.lakehouse.customer;
```

**Фактический результат:** `150000`. Таким образом, Superset прочитал Bronze через Trino и Iceberg. Учётная запись Superset и конфигурация Lakekeeper на чистом развёртывании требуют отдельной первичной настройки.

