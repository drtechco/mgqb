package mgqb

import (
	"context"
	"encoding/json"
	"fmt"
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
		_accumulator := &accumulator{}
		_accumulator.Accumulate(`
				function(state, numCopies) {  // Define how to update the state
				  return {
					count: state.count + 1,
					sum: state.sum + numCopies
				  }
				}
				`).
			AccumulateArgs([]interface{}{"$copies"})
		g := Group().FieldSimple("_id", "$author").Accumulator("avgCopies", _accumulator)
		cus, err := conn.Database("db").Aggregate(context.Background(), Pipeline().Group(g).DS())
		if err != nil {
			t.Fatal(err)
		}
		for cus.Next(context.Background()) {
			fmt.Print(cus.Current)
		}
	})

}
