package mgqb

import "go.mongodb.org/mongo-driver/bson"

type group struct {
	accumulators   map[string]*accumulator
	accumulatorRaw map[string]bson.M
	count          bson.M
	mergeObjects   bson.M
	fields         bson.M
}

func Group() *group {
	return &group{
		fields:         make(bson.M),
		accumulators:   make(map[string]*accumulator),
		accumulatorRaw: make(map[string]bson.M),
	}
}

func (g *group) Accumulator(field string, acc *accumulator) *group {
	g.accumulators[field] = acc
	return g
}

func (g *group) AccumulatorRaw(field string, acc bson.M) *group {
	g.accumulatorRaw[field] = acc
	return g
}

func (g *group) Count(field string, c bson.M) *group {
	g.count = bson.M{field: bson.M{"$count": c}}
	return g
}

func (g *group) MergeObjects(field string, c interface{}) *group {
	g.mergeObjects = bson.M{field: bson.M{"mergeObjects": c}}
	return g
}

func (g *group) Field(field string, c bson.M) *group {
	g.fields[field] = c
	return g
}

func (g *group) FieldSimple(field string, c string) *group {
	g.fields[field] = c
	return g
}

func (g *group) D() bson.D {
	d := make(bson.D, 0)

	for k, v := range g.fields {
		d = append(d, bson.E{Key: k, Value: v})
	}
	if g.accumulatorRaw == nil {
		g.accumulatorRaw = make(map[string]bson.M)
	}
	if g.accumulators != nil {
		for accField, _accumulator := range g.accumulators {
			if _, ex := g.accumulatorRaw[accField]; ex {
				g.accumulatorRaw[accField] = _accumulator.M()
			}

		}
	}
	if len(g.accumulatorRaw) > 0 {
		for field, acc := range g.accumulatorRaw {
			d = append(d, bson.E{Key: field, Value: bson.E{Key: "$accumulator", Value: acc}})
		}
	}
	if g.count != nil {
		d = append(d, bson.E{Key: "$count", Value: g.count})
	}
	if len(g.mergeObjects) > 0 {
		for k, v := range g.mergeObjects {
			d = append(d, bson.E{Key: k, Value: v})
		}
	}
	return d
}
