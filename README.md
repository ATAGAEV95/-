# ДЗ Продажи и рекламы компании
### Выполнение задачи будет происходить на CLICKHOUSE!
У нас имеются таблицы `category` и `sales` в Системе1 и таблица `advertising` в Системе2.  
Таблицы `category` и `sales` ежедневно пополняются данными за вчерашний день, имеют исторические данные и партицированны по месяцам для лучшей производительности.  
Таблица `advertising` ежедневно пополняются новыми данными,  а так же принадлежность существующего товара категории может смениться. Имеет движок `ReplacingMergeTree` который выполняет удаление дублирующихся записей с одинаковым значением ключа сортировки.  

```sql
CREATE DATABASE sistema1;
CREATE DATABASE sistema2;

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

Мы добавляем в таблицу `category` новую колонку `insert_date` которая по умолчанию записывает дату вставки данных. Так мы сможем отслеживать обновление данных. 

```sql
ALTER TABLE sistema1.category 
ADD COLUMN insert_date Date DEFAULT now();
```
Создаем Витрину Данных где соединяем три таблицы

```sql
SELECT sa.sale_date, sa.order_id, sa.product_id, c.category_id, c.product_name,
	c.category_name, sa.sale_amount, sa.ad_amount
FROM (
	SELECT  IF(s.sale_date = toDateTime64('1970-01-01', 9), a.ad_date, s.sale_date) AS sale_date, 
		s.order_id, IF(s.product_id = 0, a.product_id, s.product_id) as product_id,
		s.sale_amount, a.ad_amount
	FROM sistema1.sales s 
	FULL JOIN sistema2.advertising a ON
		s.sale_date = a.ad_date AND s.product_id = a.product_id
	ORDER BY sale_date, s.order_id) AS sa
JOIN sistema1.category c ON
	sa.product_id = c.product_id;
```

![ДЗ](https://github.com/user-attachments/assets/60b44180-89aa-47e1-8ecf-d2cd5921462c)

