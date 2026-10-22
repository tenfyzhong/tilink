package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/tenfyzhong/tilink/internal/model"
	"github.com/tenfyzhong/tilink/internal/state"
)

type Provider interface {
	Plan(context.Context, model.Config) ([]model.Step, error)
	Apply(context.Context, model.Config, model.Step, []model.Resource) (model.Resource, error)
	Refresh(context.Context, model.Config, model.Resource) (model.Resource, error)
	Delete(context.Context, model.Config, model.Resource) error
}
type StepPreparer interface {
	PrepareStep(context.Context, model.Config, model.Step, []model.Resource) (model.Step, error)
}
type Engine struct {
	Store     *state.Store
	Providers map[string]Provider
}
type DestroyPlan struct {
	ConnectionID string           `json:"connection_id"`
	Revision     int64            `json:"revision"`
	Fingerprint  string           `json:"fingerprint"`
	Resources    []model.Resource `json:"resources"`
	Preserved    []model.Resource `json:"preserved"`
	Blockers     []model.Finding  `json:"blockers,omitempty"`
}
type Confirmer interface {
	ConfirmPlan(DestroyPlan) (bool, error)
	ConfirmID(string) (string, error)
}

var ErrCancelled = errors.New("destruction cancelled")
var ErrWaitingAction = errors.New("operator action required")
var ErrNotReady = errors.New("resource is not ready; retry apply after resolving its status")
var ErrStalePlan = errors.New("destruction plan changed; confirm again")

func stable(parts ...string) string {
	b, _ := json.Marshal(parts)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:16])
}
func (e *Engine) Plan(ctx context.Context, c model.Config) (model.Plan, error) {
	p := model.Plan{}
	if err := c.Validate(); err != nil {
		return p, err
	}
	if e.Store != nil {
		saved, err := e.Store.Load(c.Name)
		if err == nil {
			p.ConnectionID = saved.ConnectionID
		} else if !os.IsNotExist(err) {
			return p, err
		}
	}
	keys := make([]string, 0, len(e.Providers))
	for k := range e.Providers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		steps, err := e.Providers[k].Plan(ctx, c)
		if err != nil {
			return p, err
		}
		p.Steps = append(p.Steps, steps...)
	}
	sorted, err := order(p.Steps)
	p.Steps = sorted
	return p, err
}
func order(steps []model.Step) ([]model.Step, error) {
	by := map[string]model.Step{}
	for _, s := range steps {
		if s.Key == "" {
			return nil, fmt.Errorf("empty step key")
		}
		if _, ok := by[s.Key]; ok {
			return nil, fmt.Errorf("duplicate step %s", s.Key)
		}
		by[s.Key] = s
	}
	seen := map[string]int{}
	out := []model.Step{}
	var visit func(string) error
	visit = func(k string) error {
		if seen[k] == 2 {
			return nil
		}
		if seen[k] == 1 {
			return fmt.Errorf("dependency cycle at %s", k)
		}
		s, ok := by[k]
		if !ok {
			return fmt.Errorf("missing dependency %s", k)
		}
		seen[k] = 1
		for _, d := range s.DependsOn {
			if err := visit(d); err != nil {
				return err
			}
		}
		seen[k] = 2
		out = append(out, s)
		return nil
	}
	for _, s := range steps {
		if err := visit(s.Key); err != nil {
			return nil, err
		}
	}
	return out, nil
}
func (e *Engine) save(v *model.State) error { return e.Store.SaveLocked(v, v.Revision) }
func (e *Engine) Apply(ctx context.Context, c model.Config) (*model.State, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	unlock, err := e.Store.Lock(c.Name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	v, err := e.Store.Load(c.Name)
	if os.IsNotExist(err) {
		var identity [16]byte
		if _, err := rand.Read(identity[:]); err != nil {
			return nil, fmt.Errorf("generate connection identity: %w", err)
		}
		v = &model.State{Version: 1, Config: c, ConnectionID: hex.EncodeToString(identity[:]), Status: "applying", Resources: []model.Resource{}}
		if err = e.save(v); err != nil {
			return v, err
		}
	} else if err != nil {
		return nil, err
	}
	if v.Status == "destroyed" {
		return v, fmt.Errorf("destroyed connection cannot be reapplied; use a new name")
	}
	compatibleConfig := v.Config
	hasTiDBHistory := false
	for _, r := range v.Resources {
		if r.Provider == "tidb" {
			hasTiDBHistory = true
		}
	}
	for _, o := range v.Operations {
		if o.Step.Provider == "tidb" {
			hasTiDBHistory = true
		}
	}
	if !hasTiDBHistory && compatibleConfig.TiDB.DataflowBaseURL == "" {
		compatibleConfig.TiDB.DataflowBaseURL = c.TiDB.DataflowBaseURL
	}
	if !model.CompatibleUpdate(compatibleConfig, c) {
		return v, fmt.Errorf("configuration replacement is unsupported; existing resources would be orphaned")
	}
	v.Config = c
	p, err := e.Plan(ctx, c)
	if err != nil {
		return v, err
	}
	v.Findings = nil
	for _, s := range p.Steps {
		existing := -1
		for i, r := range v.Resources {
			if r.Key == s.Key && r.Status != "deleted" {
				existing = i
				break
			}
		}
		retryRecorded := false
		if existing >= 0 {
			for _, o := range v.Operations {
				if o.ID == v.Resources[existing].OperationID && o.Status == "recorded" && o.Error != "" {
					retryRecorded = true
				}
			}
		}
		if existing >= 0 && !retryRecorded {
			r := v.Resources[existing]
			provider, ok := e.Providers[r.Provider]
			if !ok {
				return v, fmt.Errorf("unknown provider %s", r.Provider)
			}
			observed, refreshErr := provider.Refresh(ctx, c, r)
			if refreshErr != nil {
				return v, refreshErr
			}
			observed.ObservedAt = time.Now().UTC()
			v.Resources[existing] = observed
			if observed.Status != "deleted" && !ready(observed.Status) && observed.Status != "pending_acceptance" {
				v.Status = "incomplete"
				if err = e.save(v); err != nil {
					return v, err
				}
				return v, ErrNotReady
			}
			if observed.Status != "deleted" {
				for i := range v.Operations {
					if v.Operations[i].ID == r.OperationID {
						v.Operations[i].Status = "done"
						v.Operations[i].Error = ""
					}
				}
				if err = e.save(v); err != nil {
					return v, err
				}
				continue
			}
		}
		if s.Manual != "" {
			v.Status = "waiting_action"
			v.Findings = append(v.Findings, model.Finding{Code: "manual_action", Severity: "warning", ResourceKey: s.Key, Message: s.Manual, NextStep: s.Manual})
			if err = e.save(v); err != nil {
				return v, err
			}
			return v, ErrWaitingAction
		}
		provider, ok := e.Providers[s.Provider]
		if !ok {
			return v, fmt.Errorf("unknown provider %s", s.Provider)
		}
		s.OperationID = stable(v.ConnectionID, s.Key)
		if s.Properties == nil {
			s.Properties = map[string]string{}
		}
		s.Properties["connection_id"] = v.ConnectionID
		oi := -1
		for i, o := range v.Operations {
			if o.ID == s.OperationID {
				oi = i
				break
			}
		}
		if oi >= 0 {
			prior := v.Operations[oi]
			if prior.Status != "rejected" {
				for key, value := range prior.Step.Properties {
					s.Properties[key] = value
				}
			}
			s.Properties["prior_operation_status"] = prior.Status
		}
		if oi < 0 || v.Operations[oi].Status == "rejected" {
			if preparer, ok := provider.(StepPreparer); ok {
				prepared, prepareErr := preparer.PrepareStep(ctx, c, s, v.Resources)
				if prepareErr != nil {
					return v, prepareErr
				}
				s = prepared
			}
		}
		if oi < 0 {
			v.Operations = append(v.Operations, model.Operation{ID: s.OperationID, Step: s, Status: "pending"})
			oi = len(v.Operations) - 1
		}
		v.Operations[oi].Step = s
		v.Status = "applying"
		if err = e.save(v); err != nil {
			return v, err
		}
		r, applyErr := provider.Apply(ctx, c, s, v.Resources)
		if applyErr != nil {
			if r.ID != "" {
				r.Key = s.Key
				r.Provider = s.Provider
				r.Kind = s.Kind
				r.OperationID = s.OperationID
				r.DependsOn = s.DependsOn
				r.ObservedAt = time.Now().UTC()
				if r.Status == "" {
					r.Status = "pending"
				}
				if existing >= 0 {
					v.Resources[existing] = r
				} else {
					v.Resources = append(v.Resources, r)
				}
			}
			v.Status = "incomplete"
			v.Operations[oi].Status = "uncertain"
			var mutation interface{ MutationUncertain() bool }
			if errors.As(applyErr, &mutation) && !mutation.MutationUncertain() {
				v.Operations[oi].Status = "rejected"
			}
			if r.ID != "" || existing >= 0 {
				v.Operations[oi].Status = "recorded"
			}
			v.Operations[oi].Error = "provider operation failed; reconcile before retry"
			if err = e.save(v); err != nil {
				return v, err
			}
			return v, applyErr
		}
		r.Key = s.Key
		r.Provider = s.Provider
		r.Kind = s.Kind
		r.OperationID = s.OperationID
		r.DependsOn = s.DependsOn
		r.ObservedAt = time.Now().UTC()
		if r.Ownership != "created" && r.Ownership != "modified" && r.Ownership != "reused" {
			return v, fmt.Errorf("provider returned unknown ownership for %s", s.Key)
		}
		if existing >= 0 {
			v.Resources[existing] = r
		} else {
			v.Resources = append(v.Resources, r)
		}
		v.Operations[oi].Status = "done"
		v.Operations[oi].Error = ""
		if !ready(r.Status) && r.Status != "pending_acceptance" {
			v.Status = "incomplete"
			if err = e.save(v); err != nil {
				return v, err
			}
			return v, ErrNotReady
		}
		if err = e.save(v); err != nil {
			return v, err
		}
	}
	if err = e.refresh(ctx, v); err != nil {
		v.Status = "incomplete"
		if saveErr := e.save(v); saveErr != nil {
			return v, saveErr
		}
		return v, err
	}
	for _, step := range p.Steps {
		var current *model.Resource
		for i := range v.Resources {
			if v.Resources[i].Key == step.Key {
				current = &v.Resources[i]
			}
		}
		if current == nil || !ready(current.Status) {
			v.Status = "incomplete"
			if err = e.save(v); err != nil {
				return v, err
			}
			return v, ErrNotReady
		}
	}
	v.Status = "ready"
	return v, e.save(v)
}
func (e *Engine) refresh(ctx context.Context, v *model.State) error {
	for i, r := range v.Resources {
		if r.Status == "deleted" {
			continue
		}
		p, ok := e.Providers[r.Provider]
		if !ok {
			return fmt.Errorf("unknown provider %s", r.Provider)
		}
		next, err := p.Refresh(ctx, v.Config, r)
		if err != nil {
			return fmt.Errorf("refresh %s: %w", r.Key, err)
		}
		next.ObservedAt = r.ObservedAt
		v.Resources[i] = next
	}
	return nil
}
func (e *Engine) Refresh(ctx context.Context, name string) (*model.State, error) {
	v, err := e.Store.Load(name)
	if err != nil {
		return nil, err
	}
	err = e.refresh(ctx, v)
	return v, err
}
func fingerprint(v *model.State) string {
	b, _ := json.Marshal(v.Resources)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (e *Engine) destroyPlan(v *model.State) (DestroyPlan, error) {
	p := DestroyPlan{ConnectionID: v.ConnectionID, Revision: v.Revision, Fingerprint: fingerprint(v)}
	for _, operation := range v.Operations {
		if operation.Status != "done" && operation.Status != "recorded" && operation.Status != "rejected" {
			p.Blockers = append(p.Blockers, model.Finding{Code: "uncertain_operation", Severity: "error", ResourceKey: operation.Step.Key, Message: "Re-run apply to reconcile the unfinished operation before destruction"})
		}
	}
	steps := []model.Step{}
	by := map[string]model.Resource{}
	for _, r := range v.Resources {
		if r.Status == "deleted" {
			continue
		}
		by[r.Key] = r
		deps := []string{}
		for _, d := range r.DependsOn {
			for _, candidate := range v.Resources {
				if candidate.Key == d && candidate.Status != "deleted" {
					deps = append(deps, d)
				}
			}
		}
		steps = append(steps, model.Step{Key: r.Key, DependsOn: deps})
		if r.Ownership == model.Reused {
			p.Preserved = append(p.Preserved, r)
		} else if r.Ownership != model.Created && r.Ownership != model.Modified {
			p.Blockers = append(p.Blockers, model.Finding{Code: "unknown_ownership", Severity: "error", ResourceKey: r.Key, Message: "Cannot prove resource ownership"})
		}
	}
	var protect func(string, map[string]bool)
	protect = func(key string, seen map[string]bool) {
		if seen[key] {
			return
		}
		seen[key] = true
		r, ok := by[key]
		if !ok {
			return
		}
		if r.Ownership == model.Created || r.Ownership == model.Modified {
			p.Blockers = append(p.Blockers, model.Finding{Code: "shared_dependency", Severity: "error", ResourceKey: key, Message: "A preserved resource depends on this managed resource"})
		}
		for _, dep := range r.DependsOn {
			protect(dep, seen)
		}
	}
	for _, r := range p.Preserved {
		for _, dep := range r.DependsOn {
			protect(dep, map[string]bool{})
		}
	}
	ordered, err := order(steps)
	if err != nil {
		return p, err
	}
	for i := len(ordered) - 1; i >= 0; i-- {
		r := by[ordered[i].Key]
		if r.Ownership != model.Reused {
			p.Resources = append(p.Resources, r)
		}
	}
	return p, nil
}
func (e *Engine) Destroy(ctx context.Context, name string, c Confirmer) (*model.State, error) {
	v, err := e.Refresh(ctx, name)
	if err != nil {
		return v, err
	}
	p, err := e.destroyPlan(v)
	if err != nil {
		return v, err
	}
	if len(p.Blockers) > 0 {
		v.Findings = p.Blockers
		return v, fmt.Errorf("cleanup blocked; inspect the reported resource findings")
	}
	yes, err := c.ConfirmPlan(p)
	if err != nil {
		return v, err
	}
	if !yes {
		return v, ErrCancelled
	}
	id, err := c.ConfirmID(v.ConnectionID)
	if err != nil {
		return v, err
	}
	if id != v.ConnectionID {
		return v, ErrCancelled
	}
	unlock, err := e.Store.Lock(name)
	if err != nil {
		return v, err
	}
	defer unlock()
	latest, err := e.Store.Load(name)
	if err != nil {
		return v, err
	}
	if latest.Revision != p.Revision {
		return latest, ErrStalePlan
	}
	if err = e.refresh(ctx, latest); err != nil {
		return latest, err
	}
	if fingerprint(latest) != p.Fingerprint {
		return latest, ErrStalePlan
	}
	v = latest
	v.Status = "destroying"
	v.Findings = nil
	if err = e.save(v); err != nil {
		return v, err
	}
	failed := map[string]bool{}
	var failures []error
	var blockDependencies func(string)
	blockDependencies = func(key string) {
		if failed[key] {
			return
		}
		failed[key] = true
		for _, r := range v.Resources {
			if r.Key == key {
				for _, d := range r.DependsOn {
					blockDependencies(d)
				}
			}
		}
	}
	for _, r := range p.Resources {
		if failed[r.Key] {
			v.Findings = append(v.Findings, model.Finding{Code: "cleanup_dependency_blocked", Severity: "error", ResourceKey: r.Key, Message: "Preserved because a dependent resource could not be deleted"})
			continue
		}
		provider, ok := e.Providers[r.Provider]
		if !ok {
			blockDependencies(r.Key)
			failures = append(failures, fmt.Errorf("missing provider %s", r.Provider))
			continue
		}
		if err = provider.Delete(ctx, v.Config, r); err != nil {
			blockDependencies(r.Key)
			failures = append(failures, fmt.Errorf("delete %s: %w", r.Key, err))
			v.Findings = append(v.Findings, model.Finding{Code: "cleanup_failed", Severity: "error", ResourceKey: r.Key, Message: "Resource could not be removed", NextStep: "Resolve the provider error and run destroy again"})
			continue
		}
		for i := range v.Resources {
			if v.Resources[i].Key == r.Key {
				v.Resources[i].Status = "deleted"
			}
		}
		if err = e.save(v); err != nil {
			return v, err
		}
	}
	if len(failures) > 0 {
		v.Status = "cleanup_incomplete"
	} else {
		v.Status = "destroyed"
	}
	if err = e.save(v); err != nil {
		return v, err
	}
	return v, errors.Join(failures...)
}

func ready(status string) bool {
	return status == "ready" || status == "available" || status == "active" || status == "accepted"
}
