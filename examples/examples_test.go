package examples_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tenfyzhong/tilink/internal/model"
)

func TestExamplesMatchConfigurationSchema(t *testing.T) {
	paths, err := filepath.Glob("*.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("find examples: %v", err)
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := model.DecodeConfig(file); err != nil {
				t.Fatal(err)
			}
		})
	}
}
