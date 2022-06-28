package mgqb

import (
	"go.mongodb.org/mongo-driver/bson"
)

type match struct {
	context bson.D
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

func (m *match) AndM(m2 *match) *match {
	m.context = append(m.context, m2.context...)
	return m
}

func (m *match) Or(field string, operator whereOperator, val interface{}) *match {
	m.context = bson.D{{"$or", bson.A{m.context, bson.E{Key: field, Value: bson.D{{string(operator), val}}}}}}
	return m
}

func (m *match) OrM(m2 *match) *match {
	m.context = bson.D{{"$or", bson.A{m.context, m2.context}}}
	return m
}

func (m *match) Nor(field string, operator whereOperator, val interface{}) *match {
	m.context = bson.D{{"$nor", bson.A{m.context, bson.E{Key: field, Value: bson.D{{string(operator), val}}}}}}
	return m
}

func (m *match) NorM(m2 *match) *match {
	m.context = bson.D{{"$nor", bson.A{m.context, m2.context}}}
	return m
}

func (m *match) D() bson.D {
	return m.context
}

//
//func (m *match) Not(field string, operator whereOperator, val interface{}) {
//	m.wheres.PushFront(bson.D{{field, bson.D{{string(operator), val}}}})
//}
