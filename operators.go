package main

type windowOperator string

type windowOperators struct {
	AddToSet       windowOperator
	Avg            windowOperator
	Count          windowOperator
	CovariancePop  windowOperator
	CovarianceSamp windowOperator
	Derivative     windowOperator
	ExpMovingAvg   windowOperator
	Integral       windowOperator
	Max            windowOperator
	Min            windowOperator
	Push           windowOperator
	StdDevSamp     windowOperator
	StdDevPop      windowOperator
	Sum            windowOperator
}

var WindowOperators = windowOperators{
	AddToSet:       "$addToSet",
	Avg:            "$avg",
	Count:          "$count",
	CovariancePop:  "$covariancePop",
	CovarianceSamp: "$covarianceSamp",
	Derivative:     "$derivative",
	ExpMovingAvg:   "$expMovingAvg",
	Integral:       "$integral",
	Max:            "$max",
	Min:            "$min",
	Push:           "$push",
	StdDevSamp:     "$stdDevSamp",
	StdDevPop:      "$stdDevPop",
	Sum:            "$stdDevPop",
}

type whereOperator string

type whereOperators struct {
	EQ     whereOperator
	GT     whereOperator
	GTE    whereOperator
	IN     whereOperator
	LT     whereOperator
	LTE    whereOperator
	NE     whereOperator
	NIN    whereOperator
	Regex  whereOperator
	Exists whereOperator
}

var WhereOperators = whereOperators{
	EQ:     "$eq",
	GT:     "$gt",
	GTE:    "$gte",
	IN:     "$in",
	LT:     "$lt",
	LTE:    "$lte",
	NE:     "$ne",
	NIN:    "$nin",
	Regex:  "$regex",
	Exists: "$exists",
}
