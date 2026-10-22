package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tenfyzhong/tilink/internal/engine"
	"github.com/tenfyzhong/tilink/internal/model"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

// Service is the runtime boundary between commands and cloud workflows.
type Service interface {
	Plan(context.Context, model.Config) (model.Plan, error)
	Apply(context.Context, model.Config) (*model.State, error)
	Refresh(context.Context, string) (*model.State, error)
	Destroy(context.Context, string, engine.Confirmer) (*model.State, error)
	Doctor(context.Context, string) ([]model.Finding, error)
	Verify(context.Context, string) (model.Verification, error)
	Reconcile(context.Context, string) (*model.State, error)
}
type StateStore interface {
	Load(string) (*model.State, error)
	List() ([]model.State, error)
}
type Inventory struct {
	AccountID     string
	VPCIDs        []string
	SubnetIDs     []string
	DatabaseHosts []string
}
type Options struct {
	Engine     Service
	Store      StateStore
	Open       func(string) (Service, StateStore, error)
	In         io.Reader
	Out        io.Writer
	Err        io.Writer
	IsTerminal func() bool
	StateDir   string
	Discover   func(context.Context, model.AWSConfig) (Inventory, error)
}
type application struct {
	Options
	reader *bufio.Reader
	json   bool
}

func New(opts Options) *cobra.Command {
	if opts.In == nil {
		opts.In = os.Stdin
	}
	if opts.Out == nil {
		opts.Out = os.Stdout
	}
	if opts.Err == nil {
		opts.Err = os.Stderr
	}
	if opts.IsTerminal == nil {
		opts.IsTerminal = func() bool { f, ok := opts.In.(*os.File); return ok && term.IsTerminal(int(f.Fd())) }
	}
	if opts.StateDir == "" {
		base := os.Getenv("XDG_STATE_HOME")
		if base == "" {
			home, _ := os.UserHomeDir()
			base = filepath.Join(home, ".local", "state")
		}
		opts.StateDir = filepath.Join(base, "tilink")
	}
	a := &application{Options: opts, reader: bufio.NewReader(opts.In)}
	root := &cobra.Command{Use: "tilink", Short: "Create, inspect, diagnose, and remove TiDB Cloud private connections", Long: "tilink manages AWS PrivateLink connections for applications, DM sources, and TiCDC destinations. Configuration and cloud resource ownership are recorded locally; show works offline. Replication tasks are managed separately.", SilenceUsage: true, SilenceErrors: true}
	root.SetIn(opts.In)
	root.SetOut(opts.Out)
	root.SetErr(opts.Err)
	root.PersistentFlags().StringVar(&a.StateDir, "state-dir", opts.StateDir, "Directory containing connection state and cleanup records")
	root.PersistentFlags().BoolVar(&a.json, "json", false, "Write structured JSON output without interactive prompts")
	initCmd := &cobra.Command{Use: "init", Short: "Interactively write a connection YAML file with credential references", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		file, _ := cmd.Flags().GetString("file")
		return a.init(cmd.Context(), file)
	}}
	initCmd.Flags().StringP("file", "f", "tilink.yaml", "New configuration file (existing files are preserved)")
	root.AddCommand(initCmd)
	for _, name := range []string{"plan", "apply"} {
		name := name
		command := &cobra.Command{Use: name, Short: map[string]string{"plan": "Preview connection changes without creating cloud resources", "apply": "Create or resume a connection from its configuration"}[name], Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
			path, _ := cmd.Flags().GetString("file")
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			config, err := model.DecodeConfig(f)
			if err != nil {
				return err
			}
			if err = a.open(); err != nil {
				return err
			}
			if name == "plan" {
				p, err := a.Engine.Plan(cmd.Context(), config)
				if err != nil {
					return err
				}
				return a.output(p)
			}
			s, err := a.Engine.Apply(cmd.Context(), config)
			if s != nil {
				if outErr := a.output(s); outErr != nil {
					return outErr
				}
			}
			if err != nil {
				return err
			}
			if s != nil && s.Status != "ready" && s.Status != "verified" {
				return fmt.Errorf("connection workflow is %s; inspect findings and resume apply", s.Status)
			}
			return nil
		}}
		command.Flags().StringP("file", "f", "tilink.yaml", "Connection YAML configuration")
		root.AddCommand(command)
	}
	show := &cobra.Command{Use: "show [name]", Short: "Display saved configuration, generated files, resource ownership, and cleanup", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if err := a.open(); err != nil {
			return err
		}
		var states []model.State
		if len(args) == 1 {
			s, err := a.Store.Load(args[0])
			if err != nil {
				return err
			}
			states = []model.State{*s}
		} else {
			var err error
			states, err = a.Store.List()
			if err != nil {
				return err
			}
		}
		refresh, _ := cmd.Flags().GetBool("refresh")
		if refresh {
			for i := range states {
				s, err := a.Engine.Refresh(cmd.Context(), states[i].Config.Name)
				if err != nil {
					return err
				}
				states[i] = *s
			}
		}
		if states == nil {
			states = []model.State{}
		}
		return a.show(states, refresh)
	}}
	show.Flags().Bool("refresh", false, "Read current cloud resource state before displaying the inventory")
	root.AddCommand(show)
	for _, name := range []string{"status", "doctor", "verify", "reconcile", "destroy"} {
		name := name
		command := &cobra.Command{Use: name + " NAME", Short: map[string]string{"status": "Refresh and display resource readiness", "doctor": "Explain network findings and corrective actions", "verify": "Run connection checks and report uncovered paths", "reconcile": "Invoke the deployed target maintenance function", "destroy": "Remove managed resources after two terminal confirmations"}[name], Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			if name == "destroy" {
				if !a.IsTerminal() {
					return errors.New("destroy requires an interactive terminal")
				}
				if a.json {
					return errors.New("destroy requires interactive confirmation; --json is not supported")
				}
			}
			if err := a.open(); err != nil {
				return err
			}
			ctx := cmd.Context()
			id := args[0]
			switch name {
			case "status":
				s, err := a.Engine.Refresh(ctx, id)
				if err != nil {
					return err
				}
				return a.output(s)
			case "doctor":
				findings, err := a.Engine.Doctor(ctx, id)
				if err != nil {
					return err
				}
				if findings == nil {
					findings = []model.Finding{}
				}
				if err = a.output(findings); err != nil {
					return err
				}
				for _, f := range findings {
					if f.Severity == "error" {
						return errors.New("diagnostics found errors")
					}
				}
				return nil
			case "verify":
				v, err := a.Engine.Verify(ctx, id)
				if err != nil {
					return err
				}
				if err = a.output(v); err != nil {
					return err
				}
				if v.Status != "passed" && v.Status != "verified" {
					return fmt.Errorf("verification is %s", v.Status)
				}
				return nil
			case "reconcile":
				s, err := a.Engine.Reconcile(ctx, id)
				if err != nil {
					return err
				}
				return a.output(s)
			case "destroy":
				s, err := a.Engine.Destroy(ctx, id, a)
				if s != nil {
					if outputErr := a.output(s); outputErr != nil {
						return outputErr
					}
				}
				return err
			}
			return nil
		}}
		root.AddCommand(command)
	}
	return root
}
func (a *application) open() error {
	if a.Engine != nil && a.Store != nil {
		return nil
	}
	if a.Open != nil {
		var err error
		a.Engine, a.Store, err = a.Open(a.StateDir)
		return err
	}
	if a.Store != nil {
		return nil
	}
	return errors.New("runtime is not configured")
}
func (a *application) output(value any) error {
	if a.json {
		enc := json.NewEncoder(a.Out)
		enc.SetIndent("", "  ")
		return enc.Encode(value)
	}
	enc := yaml.NewEncoder(a.Out)
	enc.SetIndent(2)
	defer enc.Close()
	return enc.Encode(value)
}
func (a *application) show(states []model.State, refresh bool) error {
	if a.json {
		return a.output(states)
	}
	for _, s := range states {
		fmt.Fprintf(a.Out, "Connection: %s (%s)\nStatus: %s\nAccount: %s  Region: %s\nSaved at: %s\n", s.Config.Name, s.ConnectionID, s.Status, s.Config.AWS.AccountID, s.Config.AWS.Region, s.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"))
		fmt.Fprintf(a.Out, "State file: %s\n", filepath.Join(a.StateDir, s.Config.Name+".json"))
		if !refresh {
			fmt.Fprintln(a.Out, "View: saved state (use --refresh for current cloud observations)")
		}
		fmt.Fprintln(a.Out, "Configuration:")
		if err := a.output(s.Config); err != nil {
			return err
		}
		fmt.Fprintln(a.Out, "Resources:")
		for _, r := range s.Resources {
			cleanup := "delete managed resource"
			if r.Ownership == model.Reused {
				cleanup = "preserve external resource"
			}
			if r.Ownership == model.Modified {
				cleanup = "revert exact unchanged managed additions"
			}
			fmt.Fprintf(a.Out, "  %s: %s (%s; %s)\n    Status: %s  Observed: %s\n    Dependencies: %s\n    Cleanup: %s\n", r.Key, r.ID, r.Kind, r.Ownership, r.Status, r.ObservedAt.Format("2006-01-02T15:04:05Z07:00"), strings.Join(r.DependsOn, ", "), cleanup)
			if len(r.Properties) > 0 {
				if err := a.output(r.Properties); err != nil {
					return err
				}
			}
		}
		for _, f := range s.GeneratedFiles {
			fmt.Fprintf(a.Out, "Generated file: %s\n%s\n", f.Path, f.Content)
		}
		if len(s.Findings) > 0 {
			fmt.Fprintln(a.Out, "Findings:")
			if err := a.output(s.Findings); err != nil {
				return err
			}
		}
	}
	if len(states) == 0 {
		fmt.Fprintln(a.Out, "No saved connections.")
	}
	return nil
}
func (a *application) ConfirmPlan(plan engine.DestroyPlan) (bool, error) {
	fmt.Fprintln(a.Out, "Cleanup plan:")
	if err := a.output(plan); err != nil {
		return false, err
	}
	value, err := a.ask("Delete these managed resources? [y/N]", "")
	return strings.EqualFold(value, "y"), err
}
func (a *application) ConfirmID(id string) (string, error) {
	return a.ask("Enter the complete connection ID "+id+" to confirm", "")
}
func (a *application) ask(label, defaultValue string) (string, error) {
	if defaultValue != "" {
		fmt.Fprintf(a.Err, "%s [%s]: ", label, defaultValue)
	} else {
		fmt.Fprintf(a.Err, "%s: ", label)
	}
	line, err := a.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read confirmation or configuration: %w", err)
	}
	line = strings.TrimSpace(line)
	if line == "" {
		line = defaultValue
	}
	return line, nil
}
func (a *application) init(ctx context.Context, path string) error {
	if a.json || !a.IsTerminal() {
		return errors.New("init requires an interactive terminal; write YAML directly for non-interactive use")
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("configuration %s already exists", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	c := model.Config{Version: 1, Provider: "aws", Maintenance: true, Probe: true, TiDB: model.TiDBConfig{PublicKeyEnv: "TIDB_PUBLIC_KEY", PrivateKeyEnv: "TIDB_PRIVATE_KEY"}}
	var err error
	prompts := []struct {
		label, def string
		dest       *string
	}{{"Connection name", "", &c.Name}, {"Connection kind (app/dm/ticdc)", "app", &c.Kind}, {"AWS profile (empty for default)", "", &c.AWS.Profile}, {"AWS region", "", &c.AWS.Region}}
	for _, p := range prompts {
		*p.dest, err = a.ask(p.label, p.def)
		if err != nil {
			return err
		}
	}
	inventory := Inventory{}
	if a.Discover != nil {
		inventory, err = a.Discover(ctx, c.AWS)
		if err != nil {
			fmt.Fprintf(a.Err, "AWS discovery unavailable: %v. Enter resource IDs manually.\n", err)
		} else {
			fmt.Fprintf(a.Err, "Discovered VPCs: %s\nDiscovered subnets: %s\nDiscovered database hosts: %s\n", strings.Join(inventory.VPCIDs, ", "), strings.Join(inventory.SubnetIDs, ", "), strings.Join(inventory.DatabaseHosts, ", "))
		}
	}
	c.AWS.AccountID, err = a.ask("AWS account ID", inventory.AccountID)
	if err != nil {
		return err
	}
	c.AWS.VPCID, err = a.ask("VPC ID", "")
	if err != nil {
		return err
	}
	subnets, err := a.ask("Subnet IDs (comma separated)", "")
	if err != nil {
		return err
	}
	for _, v := range strings.Split(subnets, ",") {
		if v = strings.TrimSpace(v); v != "" {
			c.AWS.SubnetIDs = append(c.AWS.SubnetIDs, v)
		}
	}
	c.TiDB.ProjectID, err = a.ask("TiDB project ID", "")
	if err != nil {
		return err
	}
	c.TiDB.ClusterID, err = a.ask("TiDB cluster ID", "")
	if err != nil {
		return err
	}
	if c.Kind == "app" {
		groups, err := a.ask("Application security group IDs (comma separated)", "")
		if err != nil {
			return err
		}
		c.AWS.SecurityGroupIDs = splitValues(groups)
		cidrs, err := a.ask("Application CIDRs (comma separated, optional)", "")
		if err != nil {
			return err
		}
		c.AWS.ClientCIDRs = splitValues(cidrs)
	}
	if c.Kind != "app" {
		c.Target.Type, err = a.ask("Target type (mysql/rds/aurora/kafka/msk)", "rds")
		if err != nil {
			return err
		}
		if c.Target.Type == "msk" {
			c.Target.MSKClusterARN, err = a.ask("MSK cluster ARN", "")
			if err != nil {
				return err
			}
		} else {
			c.Target.Host, err = a.ask("Target host", "")
			if err != nil {
				return err
			}
			port := "3306"
			if c.Target.Type == "kafka" {
				port = "9092"
			}
			p, err := a.ask("Target port", port)
			if err != nil {
				return err
			}
			c.Target.Port, err = strconv.Atoi(p)
			if err != nil {
				return errors.New("target port must be a number")
			}
			c.Target.SecurityGroupID, err = a.ask("Target security group ID", "")
			if err != nil {
				return err
			}
		}
		c.TiDB.PrincipalARN, err = a.ask("TiDB AWS principal ARN (if available)", "")
		if err != nil {
			return err
		}
	}
	if c.Target.Type == "kafka" {
		countString, err := a.ask("Number of Kafka brokers", "1")
		if err != nil {
			return err
		}
		count, err := strconv.Atoi(countString)
		if err != nil || count < 1 || count > 1000 {
			return errors.New("Kafka broker count must be between 1 and 1000")
		}
		for i := 0; i < count; i++ {
			b := model.Broker{}
			b.ID, err = a.ask(fmt.Sprintf("Broker %d ID", i+1), strconv.Itoa(i+1))
			if err != nil {
				return err
			}
			b.Host, err = a.ask(fmt.Sprintf("Broker %d host", i+1), "")
			if err != nil {
				return err
			}
			p, err := a.ask(fmt.Sprintf("Broker %d port", i+1), "9092")
			if err != nil {
				return err
			}
			b.Port, err = strconv.Atoi(p)
			if err != nil {
				return errors.New("broker port must be a number")
			}
			p, err = a.ask(fmt.Sprintf("Broker %d PrivateLink listener port", i+1), strconv.Itoa(19092+i))
			if err != nil {
				return err
			}
			b.ListenerPort, err = strconv.Atoi(p)
			if err != nil {
				return errors.New("broker listener port must be a number")
			}
			c.Target.Brokers = append(c.Target.Brokers, b)
		}
	}
	if err = c.Validate(); err != nil {
		return err
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Fprintf(a.Out, "Configuration written to %s. Review it, then run tilink plan -f %s.\n", path, path)
	return nil
}

func splitValues(value string) []string {
	var result []string
	for _, v := range strings.Split(value, ",") {
		if v = strings.TrimSpace(v); v != "" {
			result = append(result, v)
		}
	}
	return result
}
