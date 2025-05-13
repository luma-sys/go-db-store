package utils

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func CreateDatePeriodFilter(start, end time.Time) bson.M {
	if start.IsZero() && end.IsZero() {
		return nil
	}

	filter := bson.M{}
	if !start.IsZero() {
		filter["$gte"] = primitive.NewDateTimeFromTime(start)
	}
	if !end.IsZero() {
		filter["$lte"] = primitive.NewDateTimeFromTime(end)
	}

	return filter
}

func CreateDatePeriodFilterMap(start, end time.Time) bson.M {
	if start.IsZero() && end.IsZero() {
		return nil
	}

	filter := make(map[string]any)

	if !start.IsZero() {
		filter["$gte"] = primitive.NewDateTimeFromTime(start)
	}

	if !end.IsZero() {
		filter["$lte"] = primitive.NewDateTimeFromTime(end)
	}

	return filter
}

func CreateInFilter(values []string) bson.M {
	if len(values) == 0 {
		return nil
	}

	return bson.M{"$in": values}
}

func CreateInFilterWithField(field string, values []string) bson.M {
	if len(values) == 0 {
		return nil
	}

	return bson.M{
		field: bson.M{"$in": values},
	}
}

func CreateLikeFilter(value string) bson.M {
	return bson.M{"$regex": value, "$options": "i"}
}

func CreateLikeFilters(value string, fields []string) []bson.D {
	if len(fields) == 0 {
		return nil
	}

	var search []bson.D

	for _, field := range fields {
		search = append(search, bson.D{{
			Key:   field,
			Value: CreateLikeFilter(value),
		}})
	}

	return search
}
