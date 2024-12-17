# ДЗ Продажи и рекламы компании
### Выполнение задачи будет происходить на CLICKHOUSE!
У нас имеются 2 таблицы в Системе1 и одна таблица в Системе2

```sql
CREATE TABLE sistema1.category (
    product_id UInt64,
    product_name String,
    category_id UInt8,
    category_name String
) ENGINE = ReplacingMergeTree
ORDER BY product_id;


CREATE TABLE sistema1.sales (
    sale_date Date,
    product_id UInt64,
    order_id UInt64,
    sale_amount UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY (sale_date, product_id, order_id);


CREATE TABLE sistema2.advertising (
    ad_date Date,
    product_id UInt64,
    ad_amount UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ad_date)
ORDER BY (ad_date, product_id)
```
