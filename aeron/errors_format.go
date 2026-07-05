package aeron

import "fmt"

func formatPublicationError(
	publicationRegID int64,
	streamID, sessionID int32,
	detail string,
) string {
	return fmt.Sprintf(
		"aeron: publication %d (stream %d session %d) %s",
		publicationRegID,
		streamID,
		sessionID,
		detail,
	)
}
