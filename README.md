# mgqb 
# Golang Mongodb bson查询流式构造器
##### 实现了一些常用的查询 
##### 已实现Pipeline,Lookup,Match,SetWindowFields,Accumulator,AddFields,Group，Project
##### 待实现Bucket,BucketAuto,CollStats,Facet,GeoNear,GraphLookup,IndexStats,LstSession,Merge,PlanCacheStats,Redact,UnionWith


1. Pipeline 示例

```sql
-- page2 sql:
SELECT
	COUNT( o.orderId ) AS orderCount,
	COUNT( od.`count` ) AS saleCount,
	count( od.bookId ) AS bookCount,
	Sum( od.amount ) AS amount,
	COUNT( b.typeId ) AS bookTypeCount,
	a.NAME 
FROM
	orders AS o
	LEFT JOIN orders_detail AS od ON od.orderId = o.orderId
	LEFT JOIN books AS b ON od.bookId = books.bookId
	LEFT JOIN `authors` AS a ON a.authorId = od.authorId 
WHERE
	o.createTime BETWEEN '2015-01-01' 
	AND '2015-03-30' 
GROUP BY
	b.authorId 
	LIMIT 10  OFFSET 10
```
```sql
-- count sql:
SELECT
	COUNT(	b.authorId  )   
FROM
	orders AS o
	LEFT JOIN orders_detail AS od ON od.orderId = o.orderId
	LEFT JOIN books AS b ON od.bookId = books.bookId
	LEFT JOIN `authors` AS a ON a.authorId = od.authorId 
WHERE
	o.createTime BETWEEN '2015-01-01' 
	AND '2015-03-30' 
GROUP BY
	b.authorId 
```
3. Find 示例

#####
PS 更多查看单元测试


