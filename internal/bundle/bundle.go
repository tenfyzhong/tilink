package bundle

import (
	"archive/zip"
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
)

//go:embed assets
var assets embed.FS

var ErrNotBuilt = errors.New("Lambda worker is not packaged; run make build")

// Worker returns the release's Linux ARM64 Lambda deployment archive.
func Worker() ([]byte, error) { return readWorker(assets) }

func readWorker(source fs.FS) ([]byte, error) {
	data, err := fs.ReadFile(source, "assets/worker.zip")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotBuilt
	}
	if err != nil {
		return nil, fmt.Errorf("read Lambda bundle: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("invalid Lambda archive: %w", err)
	}
	if len(zr.File) != 1 || zr.File[0].Name != "bootstrap" || zr.File[0].Mode().Perm()&0111 == 0 {
		return nil, errors.New("Lambda archive must contain one executable bootstrap")
	}
	r, err := zr.File[0].Open()
	if err != nil {
		return nil, fmt.Errorf("open Lambda bootstrap: %w", err)
	}
	defer r.Close()
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil || string(magic[:]) != "\x7fELF" {
		return nil, errors.New("Lambda bootstrap is not a Linux executable")
	}
	return data, nil
}
