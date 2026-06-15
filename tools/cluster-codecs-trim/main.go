package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/warrenguy/heron/tools/cluster-codecs-trim/trim"
)

var resolveSpecsDir = defaultSpecsDir

func main() {
	os.Exit(run())
}

func run() int {
	if len(os.Args) != 2 {
		_, _ = fmt.Fprintf(os.Stderr, "usage: %s <aeron-cluster-codecs.xml>\n", os.Args[0])
		return 2
	}

	specsDir, err := resolveSpecsDir()
	if err != nil {
		return fail(err)
	}

	inputPath := os.Args[1]
	src, err := os.Open(inputPath)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = src.Close() }()

	root, err := trim.Parse(src)
	if err != nil {
		return fail(err)
	}

	outRoot, err := trim.Trim(root)
	if err != nil {
		return fail(err)
	}

	outPath := filepath.Join(specsDir, "cluster-codecs-client.xml")
	tmpPath := fmt.Sprintf("%s.tmp.%d", outPath, os.Getpid())
	out, err := os.Create(tmpPath)
	if err != nil {
		return fail(err)
	}

	if err := trim.Write(out, outRoot); err != nil {
		return closeAndAbort(out, tmpPath, err)
	}
	if err := out.Sync(); err != nil {
		return closeAndAbort(out, tmpPath, err)
	}
	if err := out.Close(); err != nil {
		return abortTmp(tmpPath, err)
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return abortTmp(tmpPath, err)
	}

	fmt.Printf("wrote %s\n", outPath)
	return 0
}

func abortTmp(tmpPath string, err error) int {
	_ = os.Remove(tmpPath)
	return fail(err)
}

func closeAndAbort(out *os.File, tmpPath string, err error) int {
	_ = out.Close()
	return abortTmp(tmpPath, err)
}

func fail(err error) int {
	_, _ = fmt.Fprintln(os.Stderr, err)
	return 1
}

func defaultSpecsDir() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("resolve specs dir")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "specs")), nil
}
