package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/tenfyzhong/tilink/internal/model"
	"golang.org/x/sys/unix"
)

var ErrConflict = errors.New("state revision conflict")

type Store struct{ Dir string }

func New(dir string) *Store              { return &Store{Dir: dir} }
func (s *Store) path(name string) string { return filepath.Join(s.Dir, name+".json") }
func (s *Store) secureDir() error {
	info, err := os.Lstat(s.Dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0022 != 0 {
		return fmt.Errorf("state directory must be an owner-only directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("state directory owner mismatch")
	}
	if info.Mode().Perm()&0077 != 0 {
		return os.Chmod(s.Dir, 0700)
	}
	return nil
}
func secureFile(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !ok || int(stat.Uid) != os.Geteuid() {
		f.Close()
		return nil, fmt.Errorf("state file must be regular, owner-only and owned by the current user")
	}
	return f, nil
}
func (s *Store) Lock(name string) (func(), error) {
	if !model.ValidName(name) {
		return nil, fmt.Errorf("invalid connection name")
	}
	if err := os.MkdirAll(s.Dir, 0700); err != nil {
		return nil, err
	}
	if err := s.secureDir(); err != nil {
		return nil, err
	}
	f, err := secureFile(filepath.Join(s.Dir, name+".lock"), unix.O_CREAT|unix.O_RDWR)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("connection is locked: %w", err)
	}
	return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
}
func (s *Store) Load(name string) (*model.State, error) {
	if !model.ValidName(name) {
		return nil, fmt.Errorf("invalid connection name")
	}
	if err := s.secureDir(); err != nil {
		return nil, err
	}
	f, err := secureFile(s.path(name), unix.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var v model.State
	if err = json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	if v.Config.Name != name {
		return nil, fmt.Errorf("state identity mismatch")
	}
	return &v, nil
}
func (s *Store) List() ([]model.State, error) {
	entries, err := os.ReadDir(s.Dir)
	if os.IsNotExist(err) {
		return []model.State{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := []model.State{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			v, err := s.Load(strings.TrimSuffix(e.Name(), ".json"))
			if err != nil {
				return nil, err
			}
			out = append(out, *v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Config.Name < out[j].Config.Name })
	return out, nil
}
func (s *Store) Save(v *model.State, expected int64) error {
	unlock, err := s.Lock(v.Config.Name)
	if err != nil {
		return err
	}
	defer unlock()
	return s.SaveLocked(v, expected)
}
func (s *Store) SaveLocked(v *model.State, expected int64) error {
	if !model.ValidName(v.Config.Name) {
		return fmt.Errorf("invalid connection name")
	}
	prior, err := s.Load(v.Config.Name)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	actual := int64(0)
	if prior != nil {
		actual = prior.Revision
	}
	if actual != expected {
		return ErrConflict
	}
	for _, r := range v.Resources {
		if err := model.CheckProperties(r.Properties); err != nil {
			return err
		}
	}
	for _, o := range v.Operations {
		if err := model.CheckProperties(o.Step.Properties); err != nil {
			return err
		}
	}
	copy := *v
	copy.Version = 1
	copy.Revision = actual + 1
	copy.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(copy, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.Dir, ".state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(append(data, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), s.path(v.Config.Name)); err != nil {
		return err
	}
	dir, err := os.Open(s.Dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return err
	}
	*v = copy
	return nil
}
