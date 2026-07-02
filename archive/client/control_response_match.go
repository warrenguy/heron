package client

import "github.com/warrenguy/heron/archive/codecs"

func matchControlResponseFields(
	fields codecs.ControlResponseFields,
	controlSessionID int64,
	correlationID int64,
	filterSession bool,
) (codecs.ControlResponseFields, error, bool) {
	if filterSession && fields.ControlSessionID != controlSessionID {
		return codecs.ControlResponseFields{}, nil, false
	}
	if fields.CorrelationID != correlationID {
		return codecs.ControlResponseFields{}, nil, false
	}
	if err := MapControlResponse(fields); err != nil {
		return codecs.ControlResponseFields{}, err, true
	}
	return fields, nil, true
}
