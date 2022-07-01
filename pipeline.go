package mgqb

import (
	"go.mongodb.org/mongo-driver/bson"
)

type pipeline struct {
	addFields          []*addFields
	addFieldsRaw       []bson.D
	matchRaw           bson.D
	match              *match
	skip               bson.D
	limit              bson.D
	count              bson.D
	sort               bson.D
	unwind             []bson.D
	project            bson.D
	lookupRaw          []bson.D
	lookup             []*lookup
	setWindowFields    *setWindowFields
	setWindowFieldsRaw bson.D
	group              *group
	groupRaw           bson.D
	replaceRoot        interface{}
	unset              []string
	sortByCount        string
}

func Pipeline() *pipeline {
	return &pipeline{
		sort:         make(bson.D, 0),
		project:      make(bson.D, 0),
		groupRaw:     make(bson.D, 0),
		addFields:    make([]*addFields, 0),
		addFieldsRaw: make([]bson.D, 0),
		unwind:       make([]bson.D, 0),
		lookupRaw:    make([]bson.D, 0),
		lookup:       make([]*lookup, 0),
	}
}

//func (r  pipeline)GetMatch() *match {
//	if r.match == nil {
//		r.match = Ma()
//	}
//	return r.match
//}

func (r *pipeline) SetMatchRaw(b bson.D) *pipeline {
	r.matchRaw = b
	return r
}

func (r *pipeline) SetMatch(b *match) *pipeline {
	r.match = b
	return r
}

func (r *pipeline) Skip(skip int64) *pipeline {
	r.skip = bson.D{{"$skip", skip}}
	return r
}

func (r *pipeline) Limit(limit int64) *pipeline {
	r.limit = bson.D{{"$limit", limit}}
	return r
}

func (r *pipeline) Count(field string) *pipeline {
	if field == "" {
		field = "totalDocuments"
	}
	r.count = bson.D{{"$count", field}}
	return r
}

func (r *pipeline) SortDesc(field string) *pipeline {

	r.sort = append(r.sort, bson.E{Key: field, Value: -1})
	return r
}

func (r *pipeline) SortAsc(field string) *pipeline {
	r.sort = append(r.sort, bson.E{Key: field, Value: 1})
	return r
}

func (r *pipeline) Unwind(path string, includeArrayIndex string, preserveNullAndEmptyArrays bool) *pipeline {
	r.unwind = append(r.unwind, bson.D{{"$unwind", bson.D{
		{"path", path},
		{"includeArrayIndex", includeArrayIndex},
		{"preserveNullAndEmptyArrays", preserveNullAndEmptyArrays},
	}}})
	return r
}

func (r *pipeline) UnwindSimple(exp ...string) *pipeline {
	for _, e := range exp {
		r.unwind = append(r.unwind, bson.D{{"$unwind", e}})
	}
	return r
}

func (r *pipeline) Project1(fields ...string) *pipeline {
	for _, field := range fields {
		r.project = append(r.project, bson.E{Key: field, Value: 1})
	}
	return r
}

func (r *pipeline) Project0(fields ...string) *pipeline {
	for _, field := range fields {
		r.project = append(r.project, bson.E{Key: field, Value: 1})
	}
	return r
}

//func (r  pipeline)ProjectSub(field string, m bson.M) *pipeline {
//	r.project = append(r.project, bson.E{Key: field, Value: m})
//	return r
//}

func (r *pipeline) ProjectAny(field string, v interface{}) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: v})
	return r
}

func (r *pipeline) ProjectFirst(field string, collField string) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: bson.M{"$first": collField}})
	return r
}

func (r *pipeline) ProjectLast(field string, collField string) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: bson.M{"$first": collField}})
	return r
}

func (r *pipeline) ProjectArrayElemAt(field string, collField string, index int) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: bson.M{"$arrayElemAt": bson.A{collField, index}}})
	return r
}

func (r *pipeline) ProjectSize(field string, collField string) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: bson.M{"$size": collField}})
	return r
}

func (r *pipeline) LookupRaw(ls ...bson.D) *pipeline {
	r.lookupRaw = append(r.lookupRaw, ls...)
	return r
}
func (r *pipeline) Lookup(ls ...*lookup) *pipeline {
	r.lookup = append(r.lookup, ls...)
	return r
}

func (r *pipeline) GroupRaw(g bson.D) *pipeline {
	r.groupRaw = g
	return r
}

func (r *pipeline) Group(g *group) *pipeline {
	r.group = g
	return r
}

func (r *pipeline) AddFields(addField *addFields) *pipeline {
	r.addFields = append(r.addFields, addField)
	return r
}

func (r *pipeline) AddFieldsRaw(addField bson.D) *pipeline {
	r.addFieldsRaw = append(r.addFieldsRaw, addField)
	return r
}

func (r *pipeline) ReplaceRoot(v interface{}) *pipeline {
	r.replaceRoot = v
	return r
}

func (r *pipeline) AddUnset(fields ...string) *pipeline {
	r.unset = append(r.unset, fields...)
	return r
}

func (r *pipeline) SetWindowFields(swf *setWindowFields) *pipeline {
	r.setWindowFields = swf
	return r
}

func (r *pipeline) SetWindowFieldsRaw(swf bson.D) *pipeline {
	r.setWindowFieldsRaw = swf
	return r
}

func (r *pipeline) SortByCount(field string) *pipeline {
	r.sortByCount = field
	return r
}

func (r *pipeline) Clone() *pipeline {
	n := &pipeline{
		addFields:          r.addFields,
		addFieldsRaw:       r.addFieldsRaw,
		matchRaw:           r.matchRaw,
		match:              r.match,
		skip:               r.skip,
		limit:              r.limit,
		count:              r.count,
		sort:               r.sort,
		unwind:             r.unwind,
		project:            r.project,
		lookupRaw:          r.lookupRaw,
		lookup:             r.lookup,
		setWindowFields:    r.setWindowFields,
		setWindowFieldsRaw: r.setWindowFieldsRaw,
		group:              r.group,
		groupRaw:           r.groupRaw,
		replaceRoot:        r.replaceRoot,
		unset:              r.unset,
		sortByCount:        r.sortByCount,
	}
	return n
}

func (r *pipeline) DS() []bson.D {
	res := make([]bson.D, 0)

	if r.match != nil {
		r.matchRaw = r.match.D()
	}

	if r.matchRaw != nil {
		res = append(res, bson.D{{"$match", r.matchRaw}})
	}

	if r.count != nil {
		res = append(res, r.count)
	}
	lookupRaw := make([]bson.D, 0)
	if len(r.lookup) > 0 {
		for _, l := range r.lookup {
			lookupRaw = append(lookupRaw, l.D())
		}
	}

	if len(r.lookupRaw) > 0 {
		for _, l := range r.lookupRaw {
			lookupRaw = append(lookupRaw, l)
		}
	}

	if len(lookupRaw) > 0 {
		for _, l := range lookupRaw {
			res = append(res, bson.D{{"$lookup", l}})
		}
	}

	if len(r.project) > 0 {
		res = append(res, bson.D{{"$project", r.project}})
	}

	if r.replaceRoot != nil {
		res = append(res, bson.D{{"$replaceRoot", bson.D{{"newRoot", r.replaceRoot}}}})
	}

	if r.setWindowFields != nil {
		r.setWindowFieldsRaw = r.setWindowFields.D()
	}

	if r.setWindowFieldsRaw != nil {
		res = append(res, bson.D{{"$setWindowFields", r.setWindowFieldsRaw}})
	}

	if len(r.sort) > 0 {
		res = append(res, bson.D{{"$sort", r.sort}})
	}

	if r.sortByCount != "" {
		res = append(res, bson.D{{"$sortByCount", r.sortByCount}})
	}

	if len(r.unset) > 0 {
		res = append(res, bson.D{{"$unset", r.unset}})
	}

	if r.unwind != nil {
		for _, u := range r.unwind {
			res = append(res, u)
		}
	}
	addFieldsRaw := make([]bson.D, 0)
	if len(r.addFields) > 0 {
		for _, field := range r.addFields {
			addFieldsRaw = append(addFieldsRaw, field.D())
		}
	}
	if len(r.addFieldsRaw) > 0 {
		for _, d := range r.addFieldsRaw {
			addFieldsRaw = append(addFieldsRaw, d)

		}
	}

	if len(addFieldsRaw) > 0 {
		for _, d := range addFieldsRaw {
			res = append(res, bson.D{{"$addFields", d}})
		}
	}
	groupRaw := make(bson.D, 0)
	if r.group != nil {
		for _, d := range r.group.DS() {
			groupRaw = append(groupRaw, d)
		}
	}
	if len(r.groupRaw) > 0 {
		for _, d := range r.groupRaw {
			groupRaw = append(groupRaw, d)
		}
	}

	if len(groupRaw) > 0 {
		res = append(res, bson.D{{"$group", groupRaw}})
	}

	if r.skip != nil {
		res = append(res, r.skip)
	}
	if r.limit != nil {
		res = append(res, r.limit)
	}

	if BSON_LOGGER {

		d, e := bson.MarshalExtJSON(bson.D{{"pipeline", res}}, false, false)
		if e != nil {
			Error_Log(e)
		} else {
			Trace_Log(string(d))
		}

	}
	return res
}

func (r *pipeline) M() []bson.M {
	return nil
}
