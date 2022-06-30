package main

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
	"time"
)

func Test_setWindowFields1(t *testing.T) {
	t.Run("Test_setWindowFields1", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("cakeSales").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 0, "type": "chocolate", "orderDate": "2020-05-18T14:10:30Z", "state": "CA", "price": 13, "quantity": 120 },
   { "_id": 1, "type": "chocolate", "orderDate": "2021-03-20T11:30:05Z", "state": "WA", "price": 14, "quantity": 140 },
   { "_id": 2, "type": "vanilla", "orderDate": "2021-01-11T06:31:15Z", "state": "CA", "price": 12, "quantity": 145 },
   { "_id": 3, "type": "vanilla", "orderDate": "2020-02-08T13:13:23Z", "state": "WA", "price": 13, "quantity": 104 },
   { "_id": 4, "type": "strawberry", "orderDate": "2019-05-18T16:09:01Z", "state": "CA", "price": 41, "quantity": 162 },
   { "_id": 5, "type": "strawberry", "orderDate": "2019-01-08T06:12:03Z", "state": "WA", "price": 43, "quantity": 134 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("cakeSales").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("cakeSales").
			Aggregate(context.Background(), Pipeline().
				SetWindowFields(
					SetWindowFields().
						SortAsc("orderDate").
						AddOutput(
							AddFields().Sum("cumulativeQuantityForState", "$quantity", "unbounded", "current"),
						).
						PartitionByField("$state")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 6 {
			t.Fatal(errors.New("res length not 6"))
		}
		fmt.Println(res)
	})

}

func Test_setWindowFields2(t *testing.T) {
	t.Run("Test_setWindowFields2", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("cakeSales").Drop(context.Background())
		var docs []map[string]interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 0, "type": "chocolate", "orderDate": "2020-05-18T14:10:30Z", "state": "CA", "price": 13, "quantity": 120 },
   { "_id": 1, "type": "chocolate", "orderDate": "2021-03-20T11:30:05Z", "state": "WA", "price": 14, "quantity": 140 },
   { "_id": 2, "type": "vanilla", "orderDate": "2021-01-11T06:31:15Z", "state": "CA", "price": 12, "quantity": 145 },
   { "_id": 3, "type": "vanilla", "orderDate": "2020-02-08T13:13:23Z", "state": "WA", "price": 13, "quantity": 104 },
   { "_id": 4, "type": "strawberry", "orderDate": "2019-05-18T16:09:01Z", "state": "CA", "price": 41, "quantity": 162 },
   { "_id": 5, "type": "strawberry", "orderDate": "2019-01-08T06:12:03Z", "state": "WA", "price": 43, "quantity": 134 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		docs2 := make([]interface{}, 0)
		for _, doc := range docs {
			tt, _ := time.Parse("2006-01-02T15:04:05Z", doc["orderDate"].(string))
			doc["orderDate"] = tt
			docs2 = append(docs2, doc)
		}
		_, err2 := conn.Database("test").Collection("cakeSales").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("cakeSales").
			Aggregate(context.Background(), Pipeline().
				SetWindowFields(
					SetWindowFields().
						SortAsc("orderDate").
						AddOutput(
							AddFields().Avg("averageQuantity", "$quantity", -1, 0),
						).
						PartitionBySimple("$year", "$orderDate")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 6 {
			t.Fatal(errors.New("res length not 6"))
		}
		fmt.Println(res)
	})

}

func Test_setWindowFields3(t *testing.T) {
	t.Run("Test_setWindowFields3", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("cakeSales").Drop(context.Background())
		var docs []map[string]interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 0, "type": "chocolate", "orderDate": "2020-05-18T14:10:30Z", "state": "CA", "price": 13, "quantity": 120 },
   { "_id": 1, "type": "chocolate", "orderDate": "2021-03-20T11:30:05Z", "state": "WA", "price": 14, "quantity": 140 },
   { "_id": 2, "type": "vanilla", "orderDate": "2021-01-11T06:31:15Z", "state": "CA", "price": 12, "quantity": 145 },
   { "_id": 3, "type": "vanilla", "orderDate": "2020-02-08T13:13:23Z", "state": "WA", "price": 13, "quantity": 104 },
   { "_id": 4, "type": "strawberry", "orderDate": "2019-05-18T16:09:01Z", "state": "CA", "price": 41, "quantity": 162 },
   { "_id": 5, "type": "strawberry", "orderDate": "2019-01-08T06:12:03Z", "state": "WA", "price": 43, "quantity": 134 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		docs2 := make([]interface{}, 0)
		for _, doc := range docs {
			tt, _ := time.Parse("2006-01-02T15:04:05Z", doc["orderDate"].(string))
			doc["orderDate"] = tt
			docs2 = append(docs2, doc)
		}
		_, err2 := conn.Database("test").Collection("cakeSales").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("cakeSales").
			Aggregate(context.Background(), Pipeline().
				SetWindowFields(
					SetWindowFields().
						SortAsc("orderDate").
						AddOutput(
							AddFields().Avg("cumulativeQuantityForYear", "$quantity", -1, 0).
								Max("maximumQuantityForYear", "$quantity", "unbounded", "unbounded")).
						PartitionBySimple("$year", "$orderDate")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 6 {
			t.Fatal(errors.New("res length not 6"))
		}
		fmt.Println(res)
	})

}

func Test_setWindowFields4(t *testing.T) {
	t.Run("Test_setWindowFields4", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("cakeSales").Drop(context.Background())
		var docs []map[string]interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 0, "type": "chocolate", "orderDate": "2020-05-18T14:10:30Z", "state": "CA", "price": 13, "quantity": 120 },
   { "_id": 1, "type": "chocolate", "orderDate": "2021-03-20T11:30:05Z", "state": "WA", "price": 14, "quantity": 140 },
   { "_id": 2, "type": "vanilla", "orderDate": "2021-01-11T06:31:15Z", "state": "CA", "price": 12, "quantity": 145 },
   { "_id": 3, "type": "vanilla", "orderDate": "2020-02-08T13:13:23Z", "state": "WA", "price": 13, "quantity": 104 },
   { "_id": 4, "type": "strawberry", "orderDate": "2019-05-18T16:09:01Z", "state": "CA", "price": 41, "quantity": 162 },
   { "_id": 5, "type": "strawberry", "orderDate": "2019-01-08T06:12:03Z", "state": "WA", "price": 43, "quantity": 134 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		docs2 := make([]interface{}, 0)
		for _, doc := range docs {
			tt, _ := time.Parse("2006-01-02T15:04:05Z", doc["orderDate"].(string))
			doc["orderDate"] = tt
			docs2 = append(docs2, doc)
		}
		_, err2 := conn.Database("test").Collection("cakeSales").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("cakeSales").
			Aggregate(context.Background(), Pipeline().
				SetWindowFields(
					SetWindowFields().
						SortAsc("price").
						AddOutput(
							AddFields().Avg("quantityFromSimilarOrders", "$quantity").
								Rang("quantityFromSimilarOrders", -10, 10)).
						PartitionByField("$state")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 6 {
			t.Fatal(errors.New("res length not 6"))
		}
		fmt.Println(res)
	})

}

func Test_setWindowFields5(t *testing.T) {
	t.Run("Test_setWindowFields5", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("cakeSales").Drop(context.Background())
		var docs []map[string]interface{}
		err := bson.UnmarshalExtJSON([]byte(`
[
  { "_id": 0, "type": "chocolate", "orderDate": "2020-05-18T14:10:30Z", "state": "CA", "price": 13, "quantity": 120 },
  { "_id": 1, "type": "chocolate", "orderDate": "2021-03-20T11:30:05Z", "state": "WA", "price": 14, "quantity": 140 },
  { "_id": 2, "type": "vanilla", "orderDate": "2021-01-11T06:31:15Z", "state": "CA", "price": 12, "quantity": 145 },
  { "_id": 3, "type": "vanilla", "orderDate": "2020-02-08T13:13:23Z", "state": "WA", "price": 13, "quantity": 104 },
  { "_id": 4, "type": "strawberry", "orderDate": "2019-05-18T16:09:01Z", "state": "CA", "price": 41, "quantity": 162 },
  { "_id": 5, "type": "strawberry", "orderDate": "2019-01-08T06:12:03Z", "state": "WA", "price": 43, "quantity": 134 }
] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		docs2 := make([]interface{}, 0)
		for _, doc := range docs {
			tt, _ := time.Parse("2006-01-02T15:04:05Z", doc["orderDate"].(string))
			doc["orderDate"] = tt
			docs2 = append(docs2, doc)
		}
		_, err2 := conn.Database("test").Collection("cakeSales").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("cakeSales").
			Aggregate(context.Background(), Pipeline().
				SetWindowFields(
					SetWindowFields().
						SortAsc("orderDate").
						AddOutput(
							AddFields().Push("recentOrders", "$orderDate").
								Rang("recentOrders", "unbounded", 10).
								Unit("recentOrders", "month")).
						PartitionByField("$state")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 6 {
			t.Fatal(errors.New("res length not 6"))
		}
		fmt.Println(res)
	})

}

func Test_setWindowFields6(t *testing.T) {
	t.Run("Test_setWindowFields6", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("cakeSales").Drop(context.Background())
		var docs []map[string]interface{}
		err := bson.UnmarshalExtJSON([]byte(`
[
  { "_id": 0, "type": "chocolate", "orderDate": "2020-05-18T14:10:30Z", "state": "CA", "price": 13, "quantity": 120 },
  { "_id": 1, "type": "chocolate", "orderDate": "2021-03-20T11:30:05Z", "state": "WA", "price": 14, "quantity": 140 },
  { "_id": 2, "type": "vanilla", "orderDate": "2021-01-11T06:31:15Z", "state": "CA", "price": 12, "quantity": 145 },
  { "_id": 3, "type": "vanilla", "orderDate": "2020-02-08T13:13:23Z", "state": "WA", "price": 13, "quantity": 104 },
  { "_id": 4, "type": "strawberry", "orderDate": "2019-05-18T16:09:01Z", "state": "CA", "price": 41, "quantity": 162 },
  { "_id": 5, "type": "strawberry", "orderDate": "2019-01-08T06:12:03Z", "state": "WA", "price": 43, "quantity": 134 }
] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		docs2 := make([]interface{}, 0)
		for _, doc := range docs {
			tt, _ := time.Parse("2006-01-02T15:04:05Z", doc["orderDate"].(string))
			doc["orderDate"] = tt
			docs2 = append(docs2, doc)
		}
		_, err2 := conn.Database("test").Collection("cakeSales").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("cakeSales").
			Aggregate(context.Background(), Pipeline().
				SetWindowFields(
					SetWindowFields().
						SortAsc("orderDate").
						AddOutput(
							AddFields().Push("recentOrders", "$orderDate").
								Rang("recentOrders", "unbounded", -10).
								Unit("recentOrders", "month")).
						PartitionByField("$state")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 6 {
			t.Fatal(errors.New("res length not 6"))
		}
		fmt.Println(res)
	})

}
