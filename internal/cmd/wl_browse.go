package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/doltserver"
	"github.com/steveyegge/gastown/internal/style"
	"github.com/steveyegge/gastown/internal/wasteland"
	"github.com/steveyegge/gastown/internal/workspace"
)

var (
	wlBrowseProject  string
	wlBrowseStatus   string
	wlBrowseType     string
	wlBrowsePriority int
	wlBrowseLimit    int
	wlBrowseJSON     bool
)

var wlBrowseCmd = &cobra.Command{
	Use:   "browse",
	Short: "Browse wanted items on the commons board",
	Args:  cobra.NoArgs,
	RunE:  runWLBrowse,
	Long: `Browse the Wasteland wanted board.

Reads from the local Dolt server using the database configured in
mayor/wasteland.json (fork_db). Falls back to clone-then-discard
from the configured upstream only if no local database exists.

EXAMPLES:
  gt wl browse                          # All open wanted items
  gt wl browse --project gastown        # Filter by project
  gt wl browse --type bug               # Only bugs
  gt wl browse --status claimed         # Claimed items
  gt wl browse --priority 0             # Critical priority only
  gt wl browse --limit 5               # Show 5 items
  gt wl browse --json                   # JSON output`,
}

func init() {
	wlBrowseCmd.Flags().StringVar(&wlBrowseProject, "project", "", "Filter by project (e.g., gastown, beads, hop)")
	wlBrowseCmd.Flags().StringVar(&wlBrowseStatus, "status", "open", "Filter by status (open, claimed, in_review, completed, withdrawn)")
	wlBrowseCmd.Flags().StringVar(&wlBrowseType, "type", "", "Filter by type (feature, bug, design, rfc, docs)")
	wlBrowseCmd.Flags().IntVar(&wlBrowsePriority, "priority", -1, "Filter by priority (0=critical, 2=medium, 4=backlog)")
	wlBrowseCmd.Flags().IntVar(&wlBrowseLimit, "limit", 50, "Maximum items to display")
	wlBrowseCmd.Flags().BoolVar(&wlBrowseJSON, "json", false, "Output as JSON")

	wlCmd.AddCommand(wlBrowseCmd)
}

func runWLBrowse(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("not in a Gas Town workspace: %w", err)
	}

	query := buildBrowseQuery(BrowseFilter{
		Status:   wlBrowseStatus,
		Project:  wlBrowseProject,
		Type:     wlBrowseType,
		Priority: wlBrowsePriority,
		Limit:    wlBrowseLimit,
	})

	// Resolution order:
	// 1. Local Dolt server (configured DB from wasteland.json)
	// 2. Config local_dir (filesystem clone)
	// 3. Clone from configured upstream (not hardcoded)
	dbName := wasteland.ResolveDBName(townRoot)

	if doltserver.DatabaseExists(townRoot, dbName) {
		store := doltserver.NewWLCommonsWithDB(townRoot, dbName)
		if wlBrowseJSON {
			output, err := store.QueryJSON(query)
			if err != nil {
				return fmt.Errorf("querying local Dolt server: %w", err)
			}
			fmt.Print(output)
			return nil
		}
		output, err := store.QueryCSV(query)
		if err != nil {
			return fmt.Errorf("querying local Dolt server: %w", err)
		}
		return renderWLBrowseFromCSV(output)
	}

	// Fallback: try config local_dir
	if cfg, cfgErr := wasteland.LoadConfig(townRoot); cfgErr == nil && cfg.LocalDir != "" {
		if _, statErr := os.Stat(filepath.Join(cfg.LocalDir, ".dolt")); statErr == nil {
			return runBrowseFromCloneDir(cfg.LocalDir, query)
		}
	}

	// Last resort: clone from configured upstream
	return runBrowseViaClone(townRoot, query)
}

// runBrowseFromCloneDir queries a local dolt clone directory.
func runBrowseFromCloneDir(cloneDir, query string) error {
	doltPath, err := exec.LookPath("dolt")
	if err != nil {
		return fmt.Errorf("dolt not found in PATH")
	}

	if wlBrowseJSON {
		sqlCmd := exec.Command(doltPath, "sql", "-q", query, "-r", "json")
		sqlCmd.Dir = cloneDir
		sqlCmd.Stdout = os.Stdout
		sqlCmd.Stderr = os.Stderr
		return sqlCmd.Run()
	}

	return renderWLBrowseTable(doltPath, cloneDir, query)
}

// runBrowseViaClone clones the configured upstream to a temp dir, queries, and discards.
func runBrowseViaClone(townRoot, query string) error {
	doltPath, err := exec.LookPath("dolt")
	if err != nil {
		return fmt.Errorf("dolt not found in PATH — install from https://docs.dolthub.com/introduction/installation")
	}

	// Read upstream from config; fall back to hop/wl-commons only if no config
	commonsOrg := "hop"
	commonsDB := "wl-commons"
	if cfg, cfgErr := wasteland.LoadConfig(townRoot); cfgErr == nil && cfg.Upstream != "" {
		org, db, parseErr := wasteland.ParseUpstream(cfg.Upstream)
		if parseErr == nil {
			commonsOrg = org
			commonsDB = db
		}
	}

	tmpDir, err := os.MkdirTemp("", "wl-browse-*")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	cloneDir := filepath.Join(tmpDir, commonsDB)
	remote := fmt.Sprintf("%s/%s", commonsOrg, commonsDB)

	if !wlBrowseJSON {
		fmt.Printf("Cloning %s...\n", style.Bold.Render(remote))
	}

	cloneCmd := exec.Command(doltPath, "clone", remote, cloneDir)
	if !wlBrowseJSON {
		cloneCmd.Stderr = os.Stderr
	}
	if err := cloneCmd.Run(); err != nil {
		return fmt.Errorf("cloning %s: %w\nEnsure the database exists on DoltHub: https://www.dolthub.com/%s", remote, err, remote)
	}
	if !wlBrowseJSON {
		fmt.Printf("%s Cloned successfully\n\n", style.Bold.Render("✓"))
	}

	if wlBrowseJSON {
		sqlCmd := exec.Command(doltPath, "sql", "-q", query, "-r", "json")
		sqlCmd.Dir = cloneDir
		sqlCmd.Stdout = os.Stdout
		sqlCmd.Stderr = os.Stderr
		return sqlCmd.Run()
	}

	return renderWLBrowseTable(doltPath, cloneDir, query)
}

// renderWLBrowseFromCSV renders browse output from CSV data returned by the Dolt server.
func renderWLBrowseFromCSV(csvData string) error {
	rows := wlParseCSV(csvData)
	if len(rows) <= 1 {
		fmt.Println("No wanted items found matching your filters.")
		return nil
	}

	tbl := style.NewTable(
		style.Column{Name: "ID", Width: 12},
		style.Column{Name: "TITLE", Width: 40},
		style.Column{Name: "PROJECT", Width: 12},
		style.Column{Name: "TYPE", Width: 10},
		style.Column{Name: "PRI", Width: 4, Align: style.AlignRight},
		style.Column{Name: "POSTED BY", Width: 16},
		style.Column{Name: "STATUS", Width: 10},
		style.Column{Name: "EFFORT", Width: 8},
	)

	for _, row := range rows[1:] {
		if len(row) < 8 {
			continue
		}
		pri := wlFormatPriority(row[4])
		tbl.AddRow(row[0], row[1], row[2], row[3], pri, row[5], row[6], row[7])
	}

	fmt.Printf("Wanted items (%d):\n\n", len(rows)-1)
	fmt.Print(tbl.Render())

	return nil
}

// BrowseFilter holds filter parameters for building a browse query.
type BrowseFilter struct {
	Status   string
	Project  string
	Type     string
	Priority int
	Limit    int
}

func buildBrowseQuery(f BrowseFilter) string {
	var conditions []string

	if f.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = '%s'", doltserver.EscapeSQL(f.Status)))
	}
	if f.Project != "" {
		conditions = append(conditions, fmt.Sprintf("project = '%s'", doltserver.EscapeSQL(f.Project)))
	}
	if f.Type != "" {
		conditions = append(conditions, fmt.Sprintf("type = '%s'", doltserver.EscapeSQL(f.Type)))
	}
	if f.Priority >= 0 {
		conditions = append(conditions, fmt.Sprintf("priority = %d", f.Priority))
	}

	query := "SELECT id, title, project, type, priority, posted_by, status, effort_level FROM wanted"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY priority ASC, created_at DESC"
	query += fmt.Sprintf(" LIMIT %d", f.Limit)

	return query
}

func renderWLBrowseTable(doltPath, cloneDir, query string) error {
	sqlCmd := exec.Command(doltPath, "sql", "-q", query, "-r", "csv")
	sqlCmd.Dir = cloneDir
	output, err := sqlCmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("query failed: %s", string(exitErr.Stderr))
		}
		return fmt.Errorf("running query: %w", err)
	}

	rows := wlParseCSV(string(output))
	if len(rows) <= 1 {
		fmt.Println("No wanted items found matching your filters.")
		return nil
	}

	tbl := style.NewTable(
		style.Column{Name: "ID", Width: 12},
		style.Column{Name: "TITLE", Width: 40},
		style.Column{Name: "PROJECT", Width: 12},
		style.Column{Name: "TYPE", Width: 10},
		style.Column{Name: "PRI", Width: 4, Align: style.AlignRight},
		style.Column{Name: "POSTED BY", Width: 16},
		style.Column{Name: "STATUS", Width: 10},
		style.Column{Name: "EFFORT", Width: 8},
	)

	for _, row := range rows[1:] {
		if len(row) < 8 {
			continue
		}
		pri := wlFormatPriority(row[4])
		tbl.AddRow(row[0], row[1], row[2], row[3], pri, row[5], row[6], row[7])
	}

	fmt.Printf("Wanted items (%d):\n\n", len(rows)-1)
	fmt.Print(tbl.Render())

	return nil
}

func wlParseCSV(data string) [][]string {
	var rows [][]string
	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		if line == "" {
			continue
		}
		rows = append(rows, wlParseCSVLine(line))
	}
	return rows
}

func wlParseCSVLine(line string) []string {
	var fields []string
	var field strings.Builder
	inQuote := false

	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch {
		case ch == '"' && !inQuote:
			inQuote = true
		case ch == '"' && inQuote:
			if i+1 < len(line) && line[i+1] == '"' {
				field.WriteByte('"')
				i++
			} else {
				inQuote = false
			}
		case ch == ',' && !inQuote:
			fields = append(fields, field.String())
			field.Reset()
		default:
			field.WriteByte(ch)
		}
	}
	fields = append(fields, field.String())
	return fields
}

func wlFormatPriority(pri string) string {
	switch pri {
	case "0":
		return "P0"
	case "1":
		return "P1"
	case "2":
		return "P2"
	case "3":
		return "P3"
	case "4":
		return "P4"
	default:
		return pri
	}
}

// suppress unused import warning
var _ = json.Marshal
