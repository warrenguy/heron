package archive

import "github.com/warrenguy/heron/archive/codecs"

func matchControlResponseFields(
	fields codecs.ControlResponseFields,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error, bool) {
	if fields.ControlSessionID != controlSessionID {
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
