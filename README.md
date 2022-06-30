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
    `authors` as a
        LEFT JOIN `books` AS b ON a.authorId = b.authorId
        LEFT JOIN `orders_detail` AS od ON b.bookId = od.bookId
        LEFT JOIN `orders` AS a ON od.orderId = od.orderId
WHERE
    o.createTime BETWEEN '2015-01-01'
        AND '2023-01-01'
GROUP BY
    b.authorId
ORDER BY
    COUNT( o.orderId ) DESC ,Sum( od.amount ) DESC
    LIMIT 3  OFFSET 3
```
```javascript
//bson query
db.authors.aggregate( [
    {
        "$lookup": {
            "from": "books",
            "localField": "authorId",
            "foreignField": "authorId",
            "pipeline": [
                {
                    "$lookup": {
                        "from": "orders_detail",
                        "localField": "bookId",
                        "foreignField": "bookId",
                        "pipeline": [
                            {
                                "$lookup": {
                                    "from": "orders",
                                    "localField": "order",
                                    "foreignField": "order",
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "dataTime": {
                                                    "$gte": {
                                                        "$date": {
                                                            "$numberLong": "1420070400000"
                                                        }
                                                    },
                                                    "$lt": {
                                                        "$date": {
                                                            "$numberLong": "1672531200000"
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "$group": {
                                                "_id": null,
                                                "orderCount": {
                                                    "$sum": 1
                                                }
                                            }
                                        }
                                    ],
                                    "as": "o_docs"
                                }
                            },
                            {
                                "$project": {
                                    "orderCount": {
                                        "$first": "$o_docs.orderCount"
                                    },
                                    "_id": 1,
                                    "bookId": 1,
                                    "bookName": 1,
                                    "count": 1,
                                    "money": 1,
                                    "type": 1,
                                    "order": 1
                                }
                            },
                            {
                                "$group": {
                                    "saleCount": {
                                        "$sum": "$count"
                                    },
                                    "saleAmount": {
                                        "$sum": "$money"
                                    },
                                    "_id": "$bookId",
                                    "orderCount": {
                                        "$sum": "$orderCount"
                                    }
                                }
                            }
                        ],
                        "as": "od_docs"
                    }
                },
                {
                    "$project": {
                        "orderCount": {
                            "$first": "$od_docs.orderCount"
                        },
                        "saleCount": {
                            "$first": "$od_docs.saleCount"
                        },
                        "saleAmount": {
                            "$first": "$od_docs.saleAmount"
                        },
                        "_id": 1,
                        "author": 1,
                        "authorId": 1,
                        "bookId": 1,
                        "bookName": 1,
                        "money": 1,
                        "od_docs": 1,
                        "type": 1
                    }
                },
                {
                    "$group": {
                        "_id": null,
                        "orderCount": {
                            "$sum": "$orderCount"
                        },
                        "saleCount": {
                            "$sum": "$saleCount"
                        },
                        "saleAmount": {
                            "$sum": "$saleAmount"
                        },
                        "types": {
                            "$addToSet": "$type"
                        },
                        "bookCount": {
                            "$sum": 1
                        }
                    }
                },
                {
                    "$lookup": {
                        "from": "orders_detail",
                        "localField": "bookId",
                        "foreignField": "bookId",
                        "pipeline": [
                            {
                                "$lookup": {
                                    "from": "orders",
                                    "localField": "order",
                                    "foreignField": "order",
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "dataTime": {
                                                    "$gte": {
                                                        "$date": {
                                                            "$numberLong": "1420070400000"
                                                        }
                                                    },
                                                    "$lt": {
                                                        "$date": {
                                                            "$numberLong": "1672531200000"
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "$group": {
                                                "_id": null,
                                                "orderCount": {
                                                    "$sum": 1
                                                }
                                            }
                                        }
                                    ],
                                    "as": "o_docs"
                                }
                            },
                            {
                                "$project": {
                                    "orderCount": {
                                        "$first": "$o_docs.orderCount"
                                    },
                                    "_id": 1,
                                    "bookId": 1,
                                    "bookName": 1,
                                    "count": 1,
                                    "money": 1,
                                    "type": 1,
                                    "order": 1
                                }
                            },
                            {
                                "$group": {
                                    "saleCount": {
                                        "$sum": "$count"
                                    },
                                    "saleAmount": {
                                        "$sum": "$money"
                                    },
                                    "_id": "$bookId",
                                    "orderCount": {
                                        "$sum": "$orderCount"
                                    }
                                }
                            },
                            {
                                "$lookup": {
                                    "from": "orders",
                                    "localField": "order",
                                    "foreignField": "order",
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "dataTime": {
                                                    "$gte": {
                                                        "$date": {
                                                            "$numberLong": "1420070400000"
                                                        }
                                                    },
                                                    "$lt": {
                                                        "$date": {
                                                            "$numberLong": "1672531200000"
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "$group": {
                                                "_id": null,
                                                "orderCount": {
                                                    "$sum": 1
                                                }
                                            }
                                        },
                                        {
                                            "$match": {
                                                "dataTime": {
                                                    "$gte": {
                                                        "$date": {
                                                            "$numberLong": "1420070400000"
                                                        }
                                                    },
                                                    "$lt": {
                                                        "$date": {
                                                            "$numberLong": "1672531200000"
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "$group": {
                                                "_id": null,
                                                "orderCount": {
                                                    "$sum": 1
                                                }
                                            }
                                        }
                                    ],
                                    "as": "o_docs"
                                }
                            },
                            {
                                "$project": {
                                    "orderCount": {
                                        "$first": "$o_docs.orderCount"
                                    },
                                    "_id": 1,
                                    "bookId": 1,
                                    "bookName": 1,
                                    "count": 1,
                                    "money": 1,
                                    "type": 1,
                                    "order": 1
                                }
                            },
                            {
                                "$group": {
                                    "saleCount": {
                                        "$sum": "$count"
                                    },
                                    "saleAmount": {
                                        "$sum": "$money"
                                    },
                                    "_id": "$bookId",
                                    "orderCount": {
                                        "$sum": "$orderCount"
                                    }
                                }
                            }
                        ],
                        "as": "od_docs"
                    }
                },
                {
                    "$project": {
                        "orderCount": {
                            "$first": "$od_docs.orderCount"
                        },
                        "saleCount": {
                            "$first": "$od_docs.saleCount"
                        },
                        "saleAmount": {
                            "$first": "$od_docs.saleAmount"
                        },
                        "_id": 1,
                        "author": 1,
                        "authorId": 1,
                        "bookId": 1,
                        "bookName": 1,
                        "money": 1,
                        "od_docs": 1,
                        "type": 1
                    }
                },
                {
                    "$group": {
                        "_id": null,
                        "orderCount": {
                            "$sum": "$orderCount"
                        },
                        "saleCount": {
                            "$sum": "$saleCount"
                        },
                        "saleAmount": {
                            "$sum": "$saleAmount"
                        },
                        "types": {
                            "$addToSet": "$type"
                        },
                        "bookCount": {
                            "$sum": 1
                        }
                    }
                }
            ],
            "as": "b_docs"
        }
    },
    {
        "$project": {
            "orderCount": {
                "$first": "$b_docs.orderCount"
            },
            "saleCount": {
                "$first": "$b_docs.saleCount"
            },
            "saleAmount": {
                "$first": "$b_docs.saleAmount"
            },
            "bookCount": {
                "$first": "$b_docs.bookCount"
            },
            "types": {
                "$size": "$b_docs.types"
            },
            "author": 1
        }
    },
    {
        "$skip": 3
    },
    {
        "$limit": 3
    }
])
```
```golang
beginTime, _ := time.Parse("2006-01-02", "2015-01-01")
	endTime, _ := time.Parse("2006-01-02", "2023-01-01")
	ordersPipeline := Pipeline().Lookup(
		Lookup().From("orders").As("o_docs").LocalField("order").ForeignField("order").
			Pipeline(
				Pipeline().
					SetMatch(
						MatchWo(
							"dataTime",
							WO(WhereOperators.GTE, primitive.NewDateTimeFromTime(beginTime)),
							WO(WhereOperators.LT, primitive.NewDateTimeFromTime(endTime)),
						),
					).
					Group(
						Group().Field("_id", nil).FieldCount("orderCount"),
					),
			),
	).
		ProjectAny("orderCount", bson.M{"$first": "$o_docs.orderCount"}).
		Project1("_id", "bookId", "bookName", "count", "money", "type", "order").
		Group(
			Group().
				FieldSimple("_id", "$bookId").
				FieldSum("orderCount", "$orderCount").
				FieldSum("saleCount", "$count").
				FieldSum("saleAmount", "$money"),
		)

	ordersDetailPipeline := Pipeline().Lookup(
		Lookup().From("orders_detail").As("od_docs").
			LocalField("bookId").
			ForeignField("bookId").
			Pipeline(
				ordersPipeline,
			),
	).
		ProjectAny("orderCount", bson.M{"$first": "$od_docs.orderCount"}).
		ProjectAny("saleCount", bson.M{"$first": "$od_docs.saleCount"}).
		ProjectAny("saleAmount", bson.M{"$first": "$od_docs.saleAmount"}).
		Project1("_id", "author", "authorId", "bookId", "bookName", "money", "od_docs", "type").
		Group(
			Group().FieldId().
				FieldSum("orderCount", "$orderCount").
				FieldSum("saleCount", "$saleCount").
				FieldSum("saleAmount", "$saleAmount").
				FieldAddToSet("types", "$type").
				FieldCount("bookCount"),
		)

	booksPipeline := Pipeline().
		Lookup(
			Lookup().From("books").As("b_docs").
				LocalField("authorId").
				ForeignField("authorId").
				Pipeline(ordersDetailPipeline),
		).
		ProjectFirst("orderCount", "$b_docs.orderCount").
		ProjectFirst("saleCount", "$b_docs.saleCount").
		ProjectFirst("saleAmount", "$b_docs.saleAmount").
		ProjectFirst("bookCount", "$b_docs.bookCount").
		ProjectSize("types", "$b_docs.types").
		Project1("author")
	countCus, err := conn.Database("test").Collection("authors").
		Aggregate(context.Background(), booksPipeline.Clone().Group(Group().FieldId().FieldCount("count")).DS())
```
3. Find 示例

#####
PS 更多查看单元测试


