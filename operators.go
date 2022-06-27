package mgqb

type operator string

type operators struct {
	addToSet       string
	avg            string
	count          string
	covariancePop  string
	covarianceSamp string
	derivative     string
	expMovingAvg   string
	integral       string
	max            string
	min            string
	push           string
	stdDevSamp     string
	stdDevPop      string
	sum            string
}

var Operators = operators{
	addToSet:       "$addToSet",
	avg:            "$avg",
	count:          "$count",
	covariancePop:  "$covariancePop",
	covarianceSamp: "$covarianceSamp",
	derivative:     "$derivative",
	expMovingAvg:   "$expMovingAvg",
	integral:       "$integral",
	max:            "$max",
	min:            "$min",
	push:           "$push",
	stdDevSamp:     "$stdDevSamp",
	stdDevPop:      "$stdDevPop",
	sum:            "$stdDevPop",
}
