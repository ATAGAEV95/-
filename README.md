# ДЗ Продажи и рекламы компании
### Выполнение задачи будет происходить на CLICKHOUSE!
У нас имеются таблицы category и sales в Системе1 и таблица advertising в Системе2.  
Таблицы category и sales ежедневно пополняются данными за вчерашний день, имеют исторические данные и партицированны по месяцам для лучшей производительности.  
Таблица advertising ежедневно пополняются новыми данными,  а так же принадлежность существующего товара категории может смениться. Имеет движок ReplacingMergeTree который выполняет удаление дублирующихся записей с одинаковым значением ключа сортировки.  

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

Мы добавляем в таблицу category новую колонку insert_date которая по умолчанию ставит дату вставки данных. Так мы сможем отслеживать обновление данных. 

```sql
ALTER TABLE sistema1.category 
ADD COLUMN insert_date Date DEFAULT now();
```
