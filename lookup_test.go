package main

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_lookup1(t *testing.T) {
	t.Run("Test_lookup1", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("orders").Drop(context.Background())
		conn.Database("test").Collection("inventory").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id" : 1, "item" : "almonds", "price" : 12, "quantity" : 2 },
   { "_id" : 2, "item" : "pecans", "price" : 20, "quantity" : 1 },
   { "_id" : 3  }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("orders").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
   { "_id" : 1, "sku" : "almonds", "description": "product 1", "instock" : 120 },
   { "_id" : 2, "sku" : "bread", "description": "product 2", "instock" : 80 },
   { "_id" : 3, "sku" : "cashews", "description": "product 3", "instock" : 60 },
   { "_id" : 4, "sku" : "pecans", "description": "product 4", "instock" : 70 },
   { "_id" : 5, "sku": null, "description": "Incomplete" },
   { "_id" : 6 }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 = conn.Database("test").Collection("inventory").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("orders").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("inventory").
					LocalField("item").
					ForeignField("sku").
					As("inventory_docs")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 3 {
			t.Fatal(errors.New("res length not 3"))
		}
		fmt.Println(res)
	})

}

func Test_lookup2(t *testing.T) {
	t.Run("Test_lookup2", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("classes").Drop(context.Background())
		conn.Database("test").Collection("members").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 1, "title": "Reading is ...", "enrollmentlist": [ "giraffe2", "pandabear", "artie" ], "days": ["M", "W", "F"] },
   { "_id": 2, "title": "But Writing ...", "enrollmentlist": [ "giraffe1", "artie" ], "days": ["T", "F"] }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("classes").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 1, "name": "artie", "joined": "2016-05-01", "status": "A" },
   { "_id": 2, "name": "giraffe", "joined": "2017-05-01", "status": "D" },
   { "_id": 3, "name": "giraffe1", "joined": "2017-10-01", "status": "A" },
   { "_id": 4, "name": "panda", "joined": "2018-10-11", "status": "A" },
   { "_id": 5, "name": "pandabear", "joined": "2018-12-01", "status": "A" },
   { "_id": 6, "name": "giraffe2", "joined": "2018-12-01", "status": "D" }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 = conn.Database("test").Collection("members").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("classes").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("members").
					LocalField("enrollmentlist").
					ForeignField("name").
					As("enrollee_info")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 {
			t.Fatal(errors.New("res length not 2"))
		}
		fmt.Println(res)
	})

}

func Test_lookup3(t *testing.T) {
	t.Run("Test_lookup3", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("orders").Drop(context.Background())
		conn.Database("test").Collection("items").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id" : 1, "item" : "almonds", "price" : 12, "quantity" : 2 },
   { "_id" : 2, "item" : "pecans", "price" : 20, "quantity" : 1 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("orders").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : 1, "item" : "almonds", "description": "almond clusters", "instock" : 120 },
  { "_id" : 2, "item" : "bread", "description": "raisin and nut bread", "instock" : 80 },
  { "_id" : 3, "item" : "pecans", "description": "candied pecans", "instock" : 60 }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 = conn.Database("test").Collection("items").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("orders").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("items").
					LocalField("item").
					ForeignField("item").
					As("fromItems")).
				ReplaceRoot(bson.M{"$mergeObjects": []interface{}{bson.M{"$arrayElemAt": []interface{}{"$fromItems", 0}}, "$$ROOT"}}).
				Project0("fromItems").DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 {
			t.Fatal(errors.New("res length not 2"))
		}
		fmt.Println(res)
	})

}

func Test_lookup4(t *testing.T) {
	t.Run("Test_lookup4", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("orders").Drop(context.Background())
		conn.Database("test").Collection("warehouses").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : 1, "item" : "almonds", "price" : 12, "ordered" : 2 },
  { "_id" : 2, "item" : "pecans", "price" : 20, "ordered" : 1 },
  { "_id" : 3, "item" : "cookies", "price" : 10, "ordered" : 60 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("orders").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : 1, "stock_item" : "almonds", "warehouse": "A", "instock" : 120 },
  { "_id" : 2, "stock_item" : "pecans", "warehouse": "A", "instock" : 80 },
  { "_id" : 3, "stock_item" : "almonds", "warehouse": "B", "instock" : 60 },
  { "_id" : 4, "stock_item" : "cookies", "warehouse": "B", "instock" : 40 },
  { "_id" : 5, "stock_item" : "cookies", "warehouse": "A", "instock" : 80 }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 = conn.Database("test").Collection("warehouses").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}

		cus, err := conn.Database("test").Collection("orders").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("warehouses").
					Let("order_item", "$item").Let("order_qty", "$ordered").
					Pipeline(Pipeline().
						SetMatchRaw(bson.D{{"$expr",
							bson.D{{"$and",
								[]bson.D{{{"$eq",
									[]string{"$stock_item", "$$order_item"}}},
									{{"$gte", []string{"$instock", "$$order_qty"}}}}}}}}).
						Project0("stock_item").Project0("_id")).
					As("stockdata")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 3 {
			t.Fatal(errors.New("res length not 3"))
		}
		fmt.Println(res)
	})

}

func Test_lookup5(t *testing.T) {
	t.Run("Test_lookup5", func(t *testing.T) {
		conn := initTestConn()
		//conn.Database("test").Collection("absences").Drop(context.Background())
		//conn.Database("test").Collection("holidays").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
{ "_id" : 1, "student" : "Ann Aardvark", "sickdays": ["2018-05-01","2018-08-23"] },
   { "_id" : 2, "student" : "Zoe Zebra", "sickdays": ["2018-05-01","2018-08-23"] }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		//simple, err2 := conn.Database("test").Collection("absences").InsertMany(context.Background(), docs)
		//if err2 != nil {
		//	t.Fatal(err2)
		//}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
 { "_id" : 1, "year": 2018, "name": "New Years", "date": "2018-01-01" },
   { "_id" : 2, "year": 2018, "name": "Pi Day", "date": "2018-03-14" },
   { "_id" : 3, "year": 2018, "name": "Ice Cream Day", "date": "2018-07-15" },
   { "_id" : 4, "year": 2017, "name": "New Years", "date": "2017-01-01" },
   { "_id" : 5, "year": 2017, "name": "Ice Cream Day", "date": "2017-07-16" }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		//simple, err2 = conn.Database("test").Collection("holidays").InsertMany(context.Background(), docs2)
		//if err2 != nil {
		//	t.Fatal(err2)
		//}

		cus, err := conn.Database("test").Collection("absences").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("holidays").
					Pipeline(Pipeline().
						SetMatchRaw(bson.D{{"year", 2018}}).
						Project0("_id").ProjectString("date", bson.M{"name": "$name", "date": "$date"}).
						ReplaceRoot("$date")).
					As("holidays")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 {
			t.Fatal(errors.New("res length not 2"))
		}
		fmt.Println(res)
	})

}

func Test_lookup6(t *testing.T) {
	t.Run("Test_lookup6", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("restaurants").Drop(context.Background())
		conn.Database("test").Collection("orders").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   {
      "_id": 1,
      "name": "American Steak House",
      "food": [ "filet", "sirloin" ],
      "beverages": [ "beer", "wine" ]
   },
   {
      "_id": 2,
      "name": "Honest John Pizza",
      "food": [ "cheese pizza", "pepperoni pizza" ],
      "beverages": [ "soda" ]
   }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("restaurants").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
   {
      "_id": 1,
      "item": "filet",
      "restaurant_name": "American Steak House"
   },
   {
      "_id": 2,
      "item": "cheese pizza",
      "restaurant_name": "Honest John Pizza",
      "drink": "lemonade"
   },
   {
      "_id": 3,
      "item": "cheese pizza",
      "restaurant_name": "Honest John Pizza",
      "drink": "soda"
   }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 = conn.Database("test").Collection("orders").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}

		cus, err := conn.Database("test").Collection("orders").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("restaurants").
					LocalField("restaurant_name").
					ForeignField("name").
					Let("orders_drink", "$drink").
					Pipeline(Pipeline().
						SetMatchRaw(bson.D{{"$expr",
							bson.D{{"$in",
								[]string{"$$orders_drink", "$beverages"}}}}})).
					As("matches")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 3 {
			t.Fatal(errors.New("res length not 3"))
		}
		fmt.Println(res)
	})

}
