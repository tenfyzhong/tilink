package state

import (
	"errors"
	"github.com/tenfyzhong/tilink/internal/model"
	"os"
	"testing"
)

func TestAtomicRevisionAndPermissions(t *testing.T) {
	s := New(t.TempDir())
	v := &model.State{Config: model.Config{Name: "demo"}}
	if err := s.Save(v, 0); err != nil {
		t.Fatal(err)
	}
	if v.Revision != 1 {
		t.Fatal(v.Revision)
	}
	stale := *v
	if err := s.Save(v, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(&stale, 1); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	info, _ := os.Stat(s.path("demo"))
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
	if _, err := s.Load("../escape"); err == nil {
		t.Fatal("unsafe path")
	}
}
func TestSecretPropertyRejected(t *testing.T) {
	s := New(t.TempDir())
	v := &model.State{Config: model.Config{Name: "demo"}, Resources: []model.Resource{{Properties: map[string]string{"password": "x"}}}}
	if s.Save(v, 0) == nil {
		t.Fatal("secret persisted")
	}
}
func TestRejectUnsafeStatePaths(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	target := dir + "/target"
	if err := os.WriteFile(target, []byte(`{"config":{"name":"demo"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, s.path("demo")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load("demo"); err == nil {
		t.Fatal("followed state symlink")
	}
	if err := os.Symlink(target, dir+"/demo.lock"); err != nil {
		t.Fatal(err)
	}
	if unlock, err := s.Lock("demo"); err == nil {
		unlock()
		t.Fatal("followed lock symlink")
	}
}
func TestRejectSharedDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0777); err != nil {
		t.Fatal(err)
	}
	if unlock, err := New(dir).Lock("demo"); err == nil {
		unlock()
		t.Fatal("accepted shared state directory")
	}
}
