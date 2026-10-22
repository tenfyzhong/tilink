package bundle

import (
	"archive/zip"
	"bytes"
	"errors"
	"strings"
	"testing"
	"testing/fstest"
)

func TestMissingWorkerExplainsBuild(t *testing.T) {
	_, err := readWorker(fstest.MapFS{})
	if !errors.Is(err, ErrNotBuilt) || !strings.Contains(err.Error(), "make build") {
		t.Fatalf("expected actionable missing bundle error, got %v", err)
	}
}

func TestWorkerRequiresExecutableBootstrap(t *testing.T) {
	for _, tc := range []struct {
		name       string
		entry      string
		executable bool
		wantOK     bool
	}{
		{"valid", "bootstrap", true, true},
		{"wrong filename", "worker", true, false},
		{"not executable", "bootstrap", false, false},
		{"path traversal", "../bootstrap", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			zw := zip.NewWriter(&out)
			h := &zip.FileHeader{Name: tc.entry, Method: zip.Deflate}
			if tc.executable {
				h.SetMode(0755)
			} else {
				h.SetMode(0644)
			}
			w, err := zw.CreateHeader(h)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = w.Write([]byte("\x7fELFexample"))
			if err := zw.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := readWorker(fstest.MapFS{"assets/worker.zip": &fstest.MapFile{Data: out.Bytes()}})
			if (err == nil) != tc.wantOK {
				t.Fatalf("result error = %v, want success %v", err, tc.wantOK)
			}
			if tc.wantOK && !bytes.Equal(data, out.Bytes()) {
				t.Fatal("bundle changed")
			}
		})
	}
}

func TestCorruptWorkerIsRejected(t *testing.T) {
	_, err := readWorker(fstest.MapFS{"assets/worker.zip": &fstest.MapFile{Data: []byte("not a zip")}})
	if err == nil {
		t.Fatal("accepted corrupt worker")
	}
}
