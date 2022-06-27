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
	})

}
