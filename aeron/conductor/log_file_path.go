package conductor

import (
	"errors"
	"path/filepath"
)

var (
	ErrLogFileNameEmpty             = errors.New("conductor: log file name is empty")
	ErrLogFileNameNotASCII          = errors.New("conductor: log file name is not ASCII")
	ErrLogFilePathNotAbsolute       = errors.New("conductor: log file path must be absolute")
	ErrLogFileOutsideMediaDriverDir = errors.New(
		"conductor: log file path outside media driver directory",
	)
	ErrMediaDriverDirRequired = errors.New(
		"conductor: media driver directory is required for image lifecycle",
	)
	ErrLogBufferMapFailed = errors.New("conductor: failed to map log buffer")
)

func canonicalLogFileName(logFileName string) string {
	return filepath.Clean(logFileName)
}

func resolveConstrainedPath(path string) (string, error) {
	clean := canonicalLogFileName(path)
	resolved, err := filepath.EvalSymlinks(clean)
	if err == nil {
		return resolved, nil
	}

	tail := []string{filepath.Base(clean)}
	current := filepath.Dir(clean)
	for {
		resolvedParent, parentErr := filepath.EvalSymlinks(current)
		if parentErr == nil {
			return filepath.Join(append([]string{resolvedParent}, tail...)...), nil
		}
		tail = append([]string{filepath.Base(current)}, tail...)
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		current = parent
	}
}

func validateLogFileName(logFileName, mediaDriverDir string) error {
	_, err := resolveLogFileKey(logFileName, mediaDriverDir)
	return err
}

func resolveLogFileMediaDriverDir(mediaDriverDir string) (string, error) {
	clean := canonicalLogFileName(mediaDriverDir)
	if !filepath.IsAbs(clean) {
		abs, err := filepath.Abs(clean)
		if err != nil {
			return "", err
		}
		clean = abs
	}
	return resolveConstrainedPath(clean)
}

func resolveLogFileKey(logFileName, mediaDriverDir string) (string, error) {
	if mediaDriverDir == "" {
		return resolveLogFileKeyWithoutMediaDriverDir(logFileName)
	}
	resolvedDir, err := resolveLogFileMediaDriverDir(mediaDriverDir)
	if err != nil {
		return "", ErrLogFileOutsideMediaDriverDir
	}
	return resolveLogFileKeyWithResolvedMediaDriverDir(logFileName, resolvedDir)
}

func validateLogFileNameFields(logFileName string) error {
	if logFileName == "" {
		return ErrLogFileNameEmpty
	}
	for i := range len(logFileName) {
		if logFileName[i] > 0x7F {
			return ErrLogFileNameNotASCII
		}
	}
	return nil
}

func resolveLogFileKeyWithResolvedMediaDriverDir(
	logFileName string,
	resolvedMediaDriverDir string,
) (string, error) {
	if err := validateLogFileNameFields(logFileName); err != nil {
		return "", err
	}

	clean := canonicalLogFileName(logFileName)
	if !filepath.IsAbs(clean) {
		return "", ErrLogFilePathNotAbsolute
	}

	resolvedLog, err := resolveConstrainedPath(clean)
	if err != nil {
		return "", ErrLogFileOutsideMediaDriverDir
	}

	rel, err := filepath.Rel(resolvedMediaDriverDir, resolvedLog)
	if err != nil {
		return "", ErrLogFileOutsideMediaDriverDir
	}
	if !filepath.IsLocal(rel) {
		return "", ErrLogFileOutsideMediaDriverDir
	}
	return resolvedLog, nil
}

func resolveLogFileKeyWithoutMediaDriverDir(logFileName string) (string, error) {
	if err := validateLogFileNameFields(logFileName); err != nil {
		return "", err
	}
	return canonicalLogFileName(logFileName), nil
}
