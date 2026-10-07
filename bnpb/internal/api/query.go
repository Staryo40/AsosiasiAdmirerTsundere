package api

import (
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

const (
	viewSummary = "summary"
	viewFull    = "full"
	roleMedia   = "media"
	roleField   = "field"
	roleOps     = "ops"
)

var cursorPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type parsedQuery struct {
	view     string
	upstream string
}

func parseListQuery(rawQuery string) (parsedQuery, error) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return parsedQuery{}, errors.New("query string is invalid")
	}
	allowed := map[string]bool{
		"source": true, "hazard_type": true, "severity": true,
		"occurred_from": true, "occurred_to": true,
		"limit": true, "cursor": true, "view": true,
	}
	for name, items := range values {
		if !allowed[name] || len(items) != 1 || items[0] == "" {
			return parsedQuery{}, errors.New("query contains an unknown, duplicate or empty parameter")
		}
	}
	view, err := parseView(values)
	if err != nil {
		return parsedQuery{}, err
	}
	if value := values.Get("source"); value != "" && value != "BMKG" && value != "PVMBG" {
		return parsedQuery{}, errors.New("source is invalid")
	}
	if value := values.Get("hazard_type"); value != "" && value != "SEISMIC" && value != "VOLCANIC" {
		return parsedQuery{}, errors.New("hazard_type is invalid")
	}
	if value := values.Get("severity"); value != "" && value != "NORMAL" && value != "WASPADA" && value != "SIAGA" && value != "AWAS" {
		return parsedQuery{}, errors.New("severity is invalid")
	}
	if values.Get("source") == "BMKG" && values.Get("hazard_type") == "VOLCANIC" || values.Get("source") == "PVMBG" && values.Get("hazard_type") == "SEISMIC" {
		return parsedQuery{}, errors.New("source and hazard_type are inconsistent")
	}
	from, err := optionalTime(values.Get("occurred_from"))
	if err != nil {
		return parsedQuery{}, errors.New("occurred_from is invalid")
	}
	to, err := optionalTime(values.Get("occurred_to"))
	if err != nil {
		return parsedQuery{}, errors.New("occurred_to is invalid")
	}
	if from != nil && to != nil && from.After(*to) {
		return parsedQuery{}, errors.New("occurred_from must not be after occurred_to")
	}
	if value := values.Get("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 500 {
			return parsedQuery{}, errors.New("limit must be between 1 and 500")
		}
	}
	if value := values.Get("cursor"); value != "" && (len(value) > 2048 || !cursorPattern.MatchString(value)) {
		return parsedQuery{}, errors.New("cursor is invalid")
	}
	values.Del("view")
	return parsedQuery{view: view, upstream: values.Encode()}, nil
}

func parseDetailQuery(rawQuery string) (parsedQuery, error) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return parsedQuery{}, errors.New("query string is invalid")
	}
	for name, items := range values {
		if name != "view" || len(items) != 1 || items[0] == "" {
			return parsedQuery{}, errors.New("detail accepts only one non-empty view parameter")
		}
	}
	view, err := parseView(values)
	return parsedQuery{view: view}, err
}

func parseView(values url.Values) (string, error) {
	view := values.Get("view")
	if view == "" {
		return viewSummary, nil
	}
	if view != viewSummary && view != viewFull {
		return "", errors.New("view must be summary or full")
	}
	return view, nil
}

func optionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
