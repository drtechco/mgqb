package mgqb

import (
	"go.mongodb.org/mongo-driver/bson"
)

type match struct {
	context bson.D
}

type wo struct {
	Operator whereOperator
	Value    interface{}
}

func WO(operator whereOperator,
	value interface{}) *wo {
	return &wo{Operator: operator, Value: value}
}

func MatchWo(field string, w ...*wo) *match {
	m := &match{}
	m.AndWo(field, w...)
	return m
}

func Match(field string, operator whereOperator, val interface{}) *match {
	m := &match{}
	m.And(field, operator, val)
	return m
}

func (m *match) And(field string, operator whereOperator, val interface{}) *match {
	if m.context == nil {
		m.context = bson.D{{field, bson.D{{string(operator), val}}}}
	} else {
		m.context = append(m.context, bson.E{Key: field, Value: bson.D{{string(operator), val}}})
	}
	return m
}

func (m *match) AndWo(field string, w ...*wo) *match {
	v := make(bson.D, 0)
	for _, wo := range w {
		v = append(v, bson.E{Key: string(wo.Operator), Value: wo.Value})
	}
	if m.context == nil {
		m.context = bson.D{{field, v}}
	} else {
		m.context = append(m.context, bson.E{Key: field, Value: bson.D{{field, v}}})
	}
	return m
}

func (m *match) AndWo2(field string, w ...*wo) *match {
	v := make(bson.D, 0)
	for _, wo := range w {
		v = append(v, bson.E{Key: string(wo.Operator), Value: wo.Value})
	}
	if m.context == nil {
		m.context = bson.D{{field, v}}
	} else {
		m.context = append(m.context, bson.E{Key: field, Value: v})
	}
	return m
}

func (m *match) AndM(m2 *match) *match {
	m.context = append(m.context, m2.context...)
	return m
}

func (m *match) Or(field string, operator whereOperator, val interface{}) *match {
	m.context = bson.D{{"$or", bson.A{m.context, bson.M{field: bson.D{{string(operator), val}}}}}}
	return m
}

func (m *match) OrWo(field string, w ...*wo) *match {
	v := make(bson.D, 0)
	for _, wo := range w {
		v = append(v, bson.E{Key: string(wo.Operator), Value: wo.Value})
	}
	m.context = bson.D{{"$or", bson.A{m.context, bson.D{{Key: field, Value: v}}}}}
	return m
}

func (m *match) OrM(m2 *match) *match {
	m.context = bson.D{{"$or", bson.A{m.context, m2.context}}}
	return m
}

func (m *match) Nor(field string, operator whereOperator, val interface{}) *match {
	m.context = bson.D{{"$nor", bson.A{m.context, bson.M{field: bson.D{{string(operator), val}}}}}}
	return m
}

func (m *match) NorWo(field string, w ...*wo) *match {
	v := make(bson.D, 0)
	for _, wo := range w {
		v = append(v, bson.E{Key: string(wo.Operator), Value: wo.Value})
	}
	m.context = bson.D{{"$nor", bson.A{m.context, bson.M{field: v}}}}
	return m
}

func (m *match) NorM(m2 *match) *match {
	m.context = bson.D{{"$nor", bson.A{m.context, m2.context}}}
	return m
}

func (m *match) D() bson.D {
	if BSON_LOGGER {
		d, e := bson.MarshalExtJSON(bson.D{{"match", m.context}}, false, false)
		if e != nil {
			Error_Log(e)
		} else {
			Trace_Log(string(d))
		}
	}
	return m.context
}

//
//func (m *match) Not(field string, operator whereOperator, val interface{}) {
//	m.wheres.PushFront(bson.D{{field, bson.D{{string(operator), val}}}})
//}
