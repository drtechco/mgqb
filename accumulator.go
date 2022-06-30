package mgqb

import "go.mongodb.org/mongo-driver/bson"

type accumulator struct {
	init           string
	initArgs       bson.A
	accumulate     string
	accumulateArgs bson.A
	merge          string
	finalize       string
	lang           string
}

func Accumulator() *accumulator {
	return &accumulator{
		initArgs:       make(bson.A, 0),
		accumulateArgs: make(bson.A, 0),
	}
}

func (a *accumulator) Init(code string) *accumulator {
	a.init = code
	return a
}

func (a *accumulator) InitArgs(args bson.A) *accumulator {
	a.initArgs = args
	return a
}

func (a *accumulator) Accumulate(code string) *accumulator {
	a.accumulate = code
	return a
}

func (a *accumulator) AccumulateArgs(args bson.A) *accumulator {
	a.accumulateArgs = args
	return a
}

func (a *accumulator) Merge(code string) *accumulator {
	a.merge = code
	return a
}

func (a *accumulator) Finalize(code string) *accumulator {
	a.finalize = code
	return a
}

func (a *accumulator) Lang(lang string) *accumulator {
	a.lang = lang
	return a
}

func (a *accumulator) M() bson.M {
	return bson.M{
		"init":           a.init,
		"initArgs":       a.initArgs,
		"accumulate":     a.accumulate,
		"accumulateArgs": a.accumulateArgs,
		"merge":          a.merge,
		"finalize":       a.finalize,
		"lang":           a.lang,
	}
}

func (a *accumulator) D() bson.D {
	return bson.D{{
		"init", a.init},
		{"initArgs", a.initArgs},
		{"accumulate", a.accumulate},
		{"accumulateArgs", a.accumulateArgs},
		{"merge", a.merge},
		{"finalize", a.finalize},
		{"lang", a.lang},
	}
}
