package mgqb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_accumulator1(t *testing.T) {
	t.Run("Test_accumulator1", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("books").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
[
  { "_id" : 8751, "title" : "The Banquet", "author" : "Dante", "copies" : 2 },
  { "_id" : 8752, "title" : "Divine Comedy", "author" : "Dante", "copies" : 1 },
  { "_id" : 8645, "title" : "Eclogues", "author" : "Dante", "copies" : 2 },
  { "_id" : 7000, "title" : "The Odyssey", "author" : "Homer", "copies" : 10 },
  { "_id" : 7020, "title" : "Iliad", "author" : "Homer", "copies" : 10 }
]`), &docs)
		conn.Database("test").Collection("books").InsertMany(context.Background(), docs)
		_accumulator := Accumulator()
		_accumulator.
			Init(`function() { return { count: 0, sum: 0 }}`).
			Accumulate(`function(state, numCopies) { return {count: state.count + 1,sum: state.sum + numCopies}}`).
			AccumulateArgs([]interface{}{"$copies"}).
			Merge(`function(state1, state2) {   return {  count: state1.count + state2.count,sum: state1.sum + state2.sum}}`).
			Finalize(`function(state) { return (state.sum / state.count)  }`).
			Lang("js")
		g := Group().FieldSimple("_id", "$author").Accumulator("avgCopies", _accumulator)
		cus, err := conn.Database("test").Collection("books").Aggregate(context.Background(), Pipeline().Group(g).DS())
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
		g = Group().FieldSimple("_id", "$author").Field("avgCopies", bson.M{"$avg": "$copies"})
		cus, err = conn.Database("test").Collection("books").Aggregate(context.Background(), Pipeline().Group(g).DS())
		if err != nil {
			t.Fatal(err)
		}
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

func Test_accumulator2(t *testing.T) {
	t.Run("Test_accumulator2", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("books").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
[
  { "_id" : 1, "name" : "Food Fury", "city" : "Bettles", "cuisine" : "American" },
  { "_id" : 2, "name" : "Meal Macro", "city" : "Bettles", "cuisine" : "Chinese" },
  { "_id" : 3, "name" : "Big Crisp", "city" : "Bettles", "cuisine" : "Latin" },
  { "_id" : 4, "name" : "The Wrap", "city" : "Onida", "cuisine" : "American" },
  { "_id" : 5, "name" : "Spice Attack", "city" : "Onida", "cuisine" : "Latin" },
  { "_id" : 6, "name" : "Soup City", "city" : "Onida", "cuisine" : "Chinese" },
  { "_id" : 7, "name" : "Crave", "city" : "Pyote", "cuisine" : "American" },
  { "_id" : 8, "name" : "The Gala", "city" : "Pyote", "cuisine" : "Chinese" }
]`), &docs)
		conn.Database("test").Collection("books").InsertMany(context.Background(), docs)
		_accumulator := Accumulator()
		_accumulator.
			Init(`function(city, userProfileCity) {return {max: city === userProfileCity ? 3 : 1,restaurants: []}}`).
			InitArgs([]interface{}{"$city", "Pyote"}).
			Accumulate(`function(state, restaurantName) {
			  if (state.restaurants.length < state.max) {
				state.restaurants.push(restaurantName);
			  }
			  return state;
			}`).
			AccumulateArgs(bson.A{"$name"}).
			Merge(`function(state1, state2) {
			  return {
				max: state1.max,
				restaurants: state1.restaurants.concat(state2.restaurants).slice(0, state1.max)
			  }
			}`).
			Finalize(`function(state) {return state.restaurants}`).
			Lang("js")
		g := Group().Field("_id", bson.M{"city": "$city"}).Accumulator("restaurants", _accumulator)
		cus, err := conn.Database("test").Collection("books").Aggregate(context.Background(), Pipeline().Group(g).DS())
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
