// Package doltserver - wl_commons.go provides wl-commons (Wasteland) database operations.
//
// The wl-commons database is the shared wanted board for the Wasteland federation.
// Phase 1 (wild-west mode): direct writes to main branch via the local Dolt server.
package doltserver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// WLCommonsDB is the database name for the wl-commons shared wanted board.
const WLCommonsDB = "wl_commons"

// WLCommonsStore abstracts wl-commons database operations.
type WLCommonsStore interface {
	EnsureDB() error
	DatabaseExists(dbName string) bool
	InsertWanted(item *WantedItem) error
	ClaimWanted(wantedID, rigHandle string) error
	SubmitCompletion(completionID, wantedID, rigHandle, evidence string) error
	QueryWanted(wantedID string) (*WantedItem, error)
	QueryWantedFull(wantedID string) (*WantedItem, error)
	InsertStamp(stamp *StampRecord) error
	QueryLastStampForSubject(subject string) (*StampRecord, error)
	QueryStampsForSubject(subject string) ([]StampRecord, error)
	QueryBadges(handle string) ([]BadgeRecord, error)
	QueryAllSubjects() ([]string, error)
	UpsertLeaderboard(entry *LeaderboardEntry) error
}

// WLCommons implements WLCommonsStore using the real Dolt server.
type WLCommons struct {
	townRoot string
	dbName   string // overrides WLCommonsDB when set
}

// NewWLCommons creates a WLCommonsStore backed by the real Dolt server.
// Uses the default database name "wl_commons".
func NewWLCommons(townRoot string) *WLCommons { return &WLCommons{townRoot: townRoot} }

// NewWLCommonsWithDB creates a WLCommonsStore that targets a specific database
// on the local Dolt server. Use this when wasteland.json has a fork_db that
// differs from the default "wl_commons".
func NewWLCommonsWithDB(townRoot, dbName string) *WLCommons {
	return &WLCommons{townRoot: townRoot, dbName: dbName}
}

// DBName returns the database name this store targets.
func (w *WLCommons) DBName() string {
	if w.dbName != "" {
		return w.dbName
	}
	return WLCommonsDB
}

// resolveDB returns the database name to use in SQL queries.
func (w *WLCommons) resolveDB() string {
	return w.DBName()
}

func (w *WLCommons) EnsureDB() error              { return EnsureWLCommonsDB(w.townRoot, w.resolveDB()) }
func (w *WLCommons) DatabaseExists(db string) bool { return DatabaseExists(w.townRoot, db) }
func (w *WLCommons) InsertWanted(item *WantedItem) error {
	return InsertWantedDB(w.townRoot, w.resolveDB(), item)
}
func (w *WLCommons) ClaimWanted(wantedID, rigHandle string) error {
	return ClaimWantedDB(w.townRoot, w.resolveDB(), wantedID, rigHandle)
}
func (w *WLCommons) SubmitCompletion(completionID, wantedID, rigHandle, evidence string) error {
	return SubmitCompletionDB(w.townRoot, w.resolveDB(), completionID, wantedID, rigHandle, evidence)
}
func (w *WLCommons) QueryWanted(wantedID string) (*WantedItem, error) {
	return QueryWantedDB(w.townRoot, w.resolveDB(), wantedID)
}
func (w *WLCommons) QueryWantedFull(wantedID string) (*WantedItem, error) {
	return QueryWantedFullDB(w.townRoot, w.resolveDB(), wantedID)
}
func (w *WLCommons) InsertStamp(stamp *StampRecord) error {
	return InsertStampDB(w.townRoot, w.resolveDB(), stamp)
}
func (w *WLCommons) QueryLastStampForSubject(subject string) (*StampRecord, error) {
	return QueryLastStampForSubjectDB(w.townRoot, w.resolveDB(), subject)
}
func (w *WLCommons) QueryStampsForSubject(subject string) ([]StampRecord, error) {
	return QueryStampsForSubjectDB(w.townRoot, w.resolveDB(), subject)
}
func (w *WLCommons) QueryBadges(handle string) ([]BadgeRecord, error) {
	return QueryBadgesDB(w.townRoot, w.resolveDB(), handle)
}
func (w *WLCommons) QueryAllSubjects() ([]string, error) {
	return QueryAllSubjectsDB(w.townRoot, w.resolveDB())
}
func (w *WLCommons) UpsertLeaderboard(entry *LeaderboardEntry) error {
	return UpsertLeaderboardDB(w.townRoot, w.resolveDB(), entry)
}

// QueryCSV executes a raw SQL query against the store's database and returns CSV output.
func (w *WLCommons) QueryCSV(query string) (string, error) {
	fullQuery := fmt.Sprintf("USE %s; %s", w.resolveDB(), query)
	return doltSQLQuery(w.townRoot, fullQuery)
}

// QueryJSON executes a raw SQL query against the store's database and returns JSON output.
func (w *WLCommons) QueryJSON(query string) (string, error) {
	fullQuery := fmt.Sprintf("USE %s; %s", w.resolveDB(), query)
	return doltSQLQueryJSON(w.townRoot, fullQuery)
}

// WantedItem represents a row in the wanted table.
type WantedItem struct {
	ID              string   `json:"id"`
	Title           string   `json:"title"`
	Description     string   `json:"description,omitempty"`
	Project         string   `json:"project,omitempty"`
	Type            string   `json:"type,omitempty"`
	Priority        int      `json:"priority"`
	Tags            []string `json:"tags,omitempty"`
	PostedBy        string   `json:"posted_by,omitempty"`
	ClaimedBy       string   `json:"claimed_by,omitempty"`
	Status          string   `json:"status"`
	EffortLevel     string   `json:"effort_level,omitempty"`
	EvidenceURL     string   `json:"evidence_url,omitempty"`
	SandboxRequired bool     `json:"sandbox_required,omitempty"`
	CreatedAt       string   `json:"created_at,omitempty"`
	UpdatedAt       string   `json:"updated_at,omitempty"`
}

// isNothingToCommit returns true if the error indicates DOLT_COMMIT found no
// changes to commit. This happens when a conditional UPDATE matched 0 rows,
// leaving the working set unchanged.
func isNothingToCommit(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "nothing to commit")
}

// EscapeSQL escapes backslashes and single quotes for SQL string literals.
// Dolt (MySQL-compatible) treats \ as an escape character, so a trailing
// backslash in user input would escape the closing quote and break the query.
func EscapeSQL(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "'", "''")
}

// GenerateWantedID generates a unique wanted item ID in the format w-<10-char-hash>.
func GenerateWantedID(title string) string {
	randomBytes := make([]byte, 8)
	_, _ = rand.Read(randomBytes)

	input := fmt.Sprintf("%s:%d:%x", title, time.Now().UnixNano(), randomBytes)
	hash := sha256.Sum256([]byte(input))
	hashStr := hex.EncodeToString(hash[:])[:10]

	return fmt.Sprintf("w-%s", hashStr)
}

// EnsureWLCommonsDB ensures a database exists and has the correct schema.
// This is the DB-parameterized version.
func EnsureWLCommonsDB(townRoot, dbName string) error {
	config := DefaultConfig(townRoot)
	dbDir := filepath.Join(config.DataDir, dbName)

	if _, err := os.Stat(filepath.Join(dbDir, ".dolt")); err == nil {
		return nil
	}

	_, created, err := InitRig(townRoot, dbName)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", dbName, err)
	}

	if !created {
		return nil
	}

	if err := initWLCommonsSchemaDB(townRoot, dbName); err != nil {
		return fmt.Errorf("initializing %s schema: %w", dbName, err)
	}

	return nil
}

// EnsureWLCommons ensures the wl-commons database exists and has the correct schema.
func EnsureWLCommons(townRoot string) error {
	config := DefaultConfig(townRoot)
	dbDir := filepath.Join(config.DataDir, WLCommonsDB)

	if _, err := os.Stat(filepath.Join(dbDir, ".dolt")); err == nil {
		return nil
	}

	_, created, err := InitRig(townRoot, WLCommonsDB)
	if err != nil {
		return fmt.Errorf("creating wl-commons database: %w", err)
	}

	if !created {
		return nil
	}

	if err := initWLCommonsSchema(townRoot); err != nil {
		return fmt.Errorf("initializing wl-commons schema: %w", err)
	}

	return nil
}

func initWLCommonsSchema(townRoot string) error {
	schema := fmt.Sprintf(`USE %s;

CREATE TABLE IF NOT EXISTS _meta (
    %s VARCHAR(64) PRIMARY KEY,
    value TEXT
);

INSERT IGNORE INTO _meta (%s, value) VALUES ('schema_version', '1.0');
INSERT IGNORE INTO _meta (%s, value) VALUES ('wasteland_name', 'Gas Town Wasteland');

CREATE TABLE IF NOT EXISTS rigs (
    handle VARCHAR(255) PRIMARY KEY,
    display_name VARCHAR(255),
    dolthub_org VARCHAR(255),
    hop_uri VARCHAR(512),
    owner_email VARCHAR(255),
    gt_version VARCHAR(32),
    trust_level INT DEFAULT 0,
    registered_at TIMESTAMP,
    last_seen TIMESTAMP,
    rig_type VARCHAR(16) DEFAULT 'human',
    parent_rig VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS wanted (
    id VARCHAR(64) PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    project VARCHAR(64),
    type VARCHAR(32),
    priority INT DEFAULT 2,
    tags JSON,
    posted_by VARCHAR(255),
    claimed_by VARCHAR(255),
    status VARCHAR(32) DEFAULT 'open',
    effort_level VARCHAR(16) DEFAULT 'medium',
    evidence_url TEXT,
    sandbox_required TINYINT(1) DEFAULT 0,
    sandbox_scope JSON,
    sandbox_min_tier VARCHAR(32),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS completions (
    id VARCHAR(64) PRIMARY KEY,
    wanted_id VARCHAR(64),
    completed_by VARCHAR(255),
    evidence TEXT,
    validated_by VARCHAR(255),
    stamp_id VARCHAR(64),
    parent_completion_id VARCHAR(64),
    block_hash VARCHAR(64),
    hop_uri VARCHAR(512),
    completed_at TIMESTAMP,
    validated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stamps (
    id VARCHAR(64) PRIMARY KEY,
    author VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    valence JSON NOT NULL,
    confidence FLOAT DEFAULT 1,
    severity VARCHAR(16) DEFAULT 'leaf',
    context_id VARCHAR(64),
    context_type VARCHAR(32),
    stamp_type VARCHAR(32),
    pilot_cohort VARCHAR(64),
    skill_tags JSON,
    message TEXT,
    prev_stamp_hash VARCHAR(64),
    stamp_index INT,
    block_hash VARCHAR(64),
    hop_uri VARCHAR(512),
    created_at TIMESTAMP,
    CHECK (NOT(author = subject)),
    CHECK (stamp_type IS NULL OR stamp_type IN ('work', 'mentoring', 'peer_review', 'endorsement', 'boot_block'))
);

CREATE INDEX IF NOT EXISTS idx_stamps_stamp_type ON stamps (stamp_type);
CREATE INDEX IF NOT EXISTS idx_stamps_pilot_cohort ON stamps (pilot_cohort);

CREATE TABLE IF NOT EXISTS badges (
    id VARCHAR(64) PRIMARY KEY,
    rig_handle VARCHAR(255),
    badge_type VARCHAR(64),
    awarded_at TIMESTAMP,
    evidence TEXT
);

CREATE TABLE IF NOT EXISTS leaderboard (
    handle VARCHAR(255) PRIMARY KEY,
    display_name VARCHAR(255),
    tier VARCHAR(32),
    stamp_count INT DEFAULT 0,
    avg_quality FLOAT DEFAULT 0,
    cluster_breadth INT DEFAULT 0,
    top_skills JSON,
    badges JSON,
    computed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chain_meta (
    chain_id VARCHAR(64) PRIMARY KEY,
    chain_type VARCHAR(32),
    parent_chain_id VARCHAR(64),
    hop_uri VARCHAR(512),
    dolt_database VARCHAR(255),
    created_at TIMESTAMP
);

CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('--allow-empty', '-m', 'Initialize wl-commons schema v1.0');
`, WLCommonsDB,
		backtickKey(), backtickKey(), backtickKey())

	return doltSQLScriptWithRetry(townRoot, schema)
}

func backtickKey() string {
	return "`key`"
}

func initWLCommonsSchemaDB(townRoot, dbName string) error {
	schema := fmt.Sprintf(`USE %s;

CREATE TABLE IF NOT EXISTS _meta (
    %s VARCHAR(64) PRIMARY KEY,
    value TEXT
);

INSERT IGNORE INTO _meta (%s, value) VALUES ('schema_version', '1.0');
INSERT IGNORE INTO _meta (%s, value) VALUES ('wasteland_name', 'Gas Town Wasteland');

CREATE TABLE IF NOT EXISTS rigs (
    handle VARCHAR(255) PRIMARY KEY,
    display_name VARCHAR(255),
    dolthub_org VARCHAR(255),
    hop_uri VARCHAR(512),
    owner_email VARCHAR(255),
    gt_version VARCHAR(32),
    trust_level INT DEFAULT 0,
    registered_at TIMESTAMP,
    last_seen TIMESTAMP,
    rig_type VARCHAR(16) DEFAULT 'human',
    parent_rig VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS wanted (
    id VARCHAR(64) PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    project VARCHAR(64),
    type VARCHAR(32),
    priority INT DEFAULT 2,
    tags JSON,
    posted_by VARCHAR(255),
    claimed_by VARCHAR(255),
    status VARCHAR(32) DEFAULT 'open',
    effort_level VARCHAR(16) DEFAULT 'medium',
    evidence_url TEXT,
    sandbox_required TINYINT(1) DEFAULT 0,
    sandbox_scope JSON,
    sandbox_min_tier VARCHAR(32),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS completions (
    id VARCHAR(64) PRIMARY KEY,
    wanted_id VARCHAR(64),
    completed_by VARCHAR(255),
    evidence TEXT,
    validated_by VARCHAR(255),
    stamp_id VARCHAR(64),
    parent_completion_id VARCHAR(64),
    block_hash VARCHAR(64),
    hop_uri VARCHAR(512),
    completed_at TIMESTAMP,
    validated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stamps (
    id VARCHAR(64) PRIMARY KEY,
    author VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    valence JSON NOT NULL,
    confidence FLOAT DEFAULT 1,
    severity VARCHAR(16) DEFAULT 'leaf',
    context_id VARCHAR(64),
    context_type VARCHAR(32),
    stamp_type VARCHAR(32),
    pilot_cohort VARCHAR(64),
    skill_tags JSON,
    message TEXT,
    prev_stamp_hash VARCHAR(64),
    stamp_index INT,
    block_hash VARCHAR(64),
    hop_uri VARCHAR(512),
    created_at TIMESTAMP,
    CHECK (NOT(author = subject)),
    CHECK (stamp_type IS NULL OR stamp_type IN ('work', 'mentoring', 'peer_review', 'endorsement', 'boot_block'))
);

CREATE INDEX IF NOT EXISTS idx_stamps_stamp_type ON stamps (stamp_type);
CREATE INDEX IF NOT EXISTS idx_stamps_pilot_cohort ON stamps (pilot_cohort);

CREATE TABLE IF NOT EXISTS badges (
    id VARCHAR(64) PRIMARY KEY,
    rig_handle VARCHAR(255),
    badge_type VARCHAR(64),
    awarded_at TIMESTAMP,
    evidence TEXT
);

CREATE TABLE IF NOT EXISTS leaderboard (
    handle VARCHAR(255) PRIMARY KEY,
    display_name VARCHAR(255),
    tier VARCHAR(32),
    stamp_count INT DEFAULT 0,
    avg_quality FLOAT DEFAULT 0,
    cluster_breadth INT DEFAULT 0,
    top_skills JSON,
    badges JSON,
    computed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chain_meta (
    chain_id VARCHAR(64) PRIMARY KEY,
    chain_type VARCHAR(32),
    parent_chain_id VARCHAR(64),
    hop_uri VARCHAR(512),
    dolt_database VARCHAR(255),
    created_at TIMESTAMP
);

CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('--allow-empty', '-m', 'Initialize %s schema v1.0');
`, dbName,
		backtickKey(), backtickKey(), backtickKey(), dbName)

	return doltSQLScriptWithRetry(townRoot, schema)
}

// doltSQLQueryJSON executes a SQL query and returns JSON output.
func doltSQLQueryJSON(townRoot, query string) (string, error) {
	config := DefaultConfig(townRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := buildDoltSQLCmd(ctx, config, "-r", "json", "-q", query)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("dolt sql query failed: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

// --- DB-parameterized versions of query functions ---
// These accept a dbName parameter instead of using the hardcoded WLCommonsDB constant.

// InsertWantedDB inserts a new wanted item into the specified database.
func InsertWantedDB(townRoot, dbName string, item *WantedItem) error {
	if item.ID == "" {
		return fmt.Errorf("wanted item ID cannot be empty")
	}
	if item.Title == "" {
		return fmt.Errorf("wanted item title cannot be empty")
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	tagsJSON := "NULL"
	if len(item.Tags) > 0 {
		escaped := make([]string, len(item.Tags))
		for i, t := range item.Tags {
			t = strings.ReplaceAll(t, `\`, `\\`)
			t = strings.ReplaceAll(t, `"`, `\"`)
			t = strings.ReplaceAll(t, "'", "''")
			escaped[i] = t
		}
		tagsJSON = fmt.Sprintf("'[\"%s\"]'", strings.Join(escaped, `","`))
	}

	descField := "NULL"
	if item.Description != "" {
		descField = fmt.Sprintf("'%s'", EscapeSQL(item.Description))
	}
	projectField := "NULL"
	if item.Project != "" {
		projectField = fmt.Sprintf("'%s'", EscapeSQL(item.Project))
	}
	typeField := "NULL"
	if item.Type != "" {
		typeField = fmt.Sprintf("'%s'", EscapeSQL(item.Type))
	}
	postedByField := "NULL"
	if item.PostedBy != "" {
		postedByField = fmt.Sprintf("'%s'", EscapeSQL(item.PostedBy))
	}
	effortField := "'medium'"
	if item.EffortLevel != "" {
		effortField = fmt.Sprintf("'%s'", EscapeSQL(item.EffortLevel))
	}
	status := "'open'"
	if item.Status != "" {
		status = fmt.Sprintf("'%s'", EscapeSQL(item.Status))
	}

	script := fmt.Sprintf(`USE %s;

INSERT INTO wanted (id, title, description, project, type, priority, tags, posted_by, status, effort_level, created_at, updated_at)
VALUES ('%s', '%s', %s, %s, %s, %d, %s, %s, %s, %s, '%s', '%s');

CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl post: %s');
`,
		dbName,
		EscapeSQL(item.ID), EscapeSQL(item.Title), descField, projectField, typeField,
		item.Priority, tagsJSON, postedByField, status, effortField,
		now, now,
		EscapeSQL(item.Title))

	return doltSQLScriptWithRetry(townRoot, script)
}

// ClaimWantedDB updates a wanted item's status to claimed in the specified database.
func ClaimWantedDB(townRoot, dbName, wantedID, rigHandle string) error {
	script := fmt.Sprintf(`USE %s;
UPDATE wanted SET claimed_by='%s', status='claimed', updated_at=NOW()
  WHERE id='%s' AND status='open';
CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl claim: %s');
`, dbName, EscapeSQL(rigHandle), EscapeSQL(wantedID), EscapeSQL(wantedID))

	err := doltSQLScriptWithRetry(townRoot, script)
	if err == nil {
		return nil
	}
	if isNothingToCommit(err) {
		return fmt.Errorf("wanted item %q is not open or does not exist", wantedID)
	}
	return fmt.Errorf("claim failed: %w", err)
}

// SubmitCompletionDB inserts a completion record in the specified database.
func SubmitCompletionDB(townRoot, dbName, completionID, wantedID, rigHandle, evidence string) error {
	script := fmt.Sprintf(`USE %s;
UPDATE wanted SET status='in_review', evidence_url='%s', updated_at=NOW()
  WHERE id='%s' AND status='claimed' AND claimed_by='%s';
INSERT IGNORE INTO completions (id, wanted_id, completed_by, evidence, completed_at)
  SELECT '%s', '%s', '%s', '%s', NOW()
  FROM wanted WHERE id='%s' AND status='in_review' AND claimed_by='%s'
  AND NOT EXISTS (SELECT 1 FROM completions WHERE wanted_id='%s');
CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl done: %s');
`,
		dbName,
		EscapeSQL(evidence), EscapeSQL(wantedID), EscapeSQL(rigHandle),
		EscapeSQL(completionID), EscapeSQL(wantedID), EscapeSQL(rigHandle), EscapeSQL(evidence),
		EscapeSQL(wantedID), EscapeSQL(rigHandle), EscapeSQL(wantedID),
		EscapeSQL(wantedID))

	err := doltSQLScriptWithRetry(townRoot, script)
	if err == nil {
		return nil
	}
	if isNothingToCommit(err) {
		return fmt.Errorf("wanted item %q is not claimed by %q or does not exist", wantedID, rigHandle)
	}
	return fmt.Errorf("completion failed: %w", err)
}

// QueryWantedDB fetches a wanted item by ID from the specified database.
func QueryWantedDB(townRoot, dbName, wantedID string) (*WantedItem, error) {
	query := fmt.Sprintf(`USE %s; SELECT id, title, status, COALESCE(claimed_by, '') as claimed_by FROM wanted WHERE id='%s';`,
		dbName, EscapeSQL(wantedID))

	output, err := doltSQLQuery(townRoot, query)
	if err != nil {
		return nil, err
	}

	rows := parseSimpleCSV(output)
	if len(rows) == 0 {
		return nil, fmt.Errorf("wanted item %q not found", wantedID)
	}

	row := rows[0]
	item := &WantedItem{
		ID:        row["id"],
		Title:     row["title"],
		Status:    row["status"],
		ClaimedBy: row["claimed_by"],
	}
	return item, nil
}

// QueryWantedFullDB fetches all fields of a wanted item from the specified database.
func QueryWantedFullDB(townRoot, dbName, wantedID string) (*WantedItem, error) {
	query := fmt.Sprintf(`USE %s; SELECT id, title, COALESCE(description, '') as description, COALESCE(project, '') as project, COALESCE(type, '') as type, priority, COALESCE(tags, JSON_ARRAY()) as tags, COALESCE(posted_by, '') as posted_by, COALESCE(claimed_by, '') as claimed_by, status, COALESCE(effort_level, '') as effort_level, COALESCE(evidence_url, '') as evidence_url, COALESCE(sandbox_required, 0) as sandbox_required, COALESCE(CAST(created_at AS CHAR), '') as created_at, COALESCE(CAST(updated_at AS CHAR), '') as updated_at FROM wanted WHERE id='%s';`,
		dbName, EscapeSQL(wantedID))

	output, err := doltSQLQuery(townRoot, query)
	if err != nil {
		return nil, err
	}

	rows := parseSimpleCSV(output)
	if len(rows) == 0 {
		return nil, fmt.Errorf("wanted item %q not found", wantedID)
	}

	row := rows[0]
	item := &WantedItem{
		ID:          row["id"],
		Title:       row["title"],
		Description: row["description"],
		Project:     row["project"],
		Type:        row["type"],
		PostedBy:    row["posted_by"],
		ClaimedBy:   row["claimed_by"],
		Status:      row["status"],
		EffortLevel: row["effort_level"],
		EvidenceURL: row["evidence_url"],
		CreatedAt:   row["created_at"],
		UpdatedAt:   row["updated_at"],
	}
	if p := row["priority"]; p != "" {
		_, _ = fmt.Sscanf(p, "%d", &item.Priority)
	}
	if row["sandbox_required"] == "1" {
		item.SandboxRequired = true
	}
	if tagsJSON := row["tags"]; tagsJSON != "" && tagsJSON != "[]" {
		_ = json.Unmarshal([]byte(tagsJSON), &item.Tags)
	}
	return item, nil
}

// InsertStampDB inserts a new stamp record into the specified database.
func InsertStampDB(townRoot, dbName string, s *StampRecord) error {
	if s.ID == "" {
		return fmt.Errorf("stamp ID cannot be empty")
	}
	if s.Author == "" || s.Subject == "" {
		return fmt.Errorf("stamp author and subject are required")
	}
	if s.Author == s.Subject {
		return fmt.Errorf("stamp author cannot equal subject")
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	if s.CreatedAt != "" {
		now = s.CreatedAt
	}

	contextID := "NULL"
	if s.ContextID != "" {
		contextID = fmt.Sprintf("'%s'", EscapeSQL(s.ContextID))
	}
	contextType := "NULL"
	if s.ContextType != "" {
		contextType = fmt.Sprintf("'%s'", EscapeSQL(s.ContextType))
	}
	stampType := "NULL"
	if s.StampType != "" {
		stampType = fmt.Sprintf("'%s'", EscapeSQL(s.StampType))
	}
	pilotCohort := "NULL"
	if s.PilotCohort != "" {
		pilotCohort = fmt.Sprintf("'%s'", EscapeSQL(s.PilotCohort))
	}
	skillTags := "NULL"
	if s.SkillTags != "" {
		skillTags = fmt.Sprintf("'%s'", EscapeSQL(s.SkillTags))
	}
	message := "NULL"
	if s.Message != "" {
		message = fmt.Sprintf("'%s'", EscapeSQL(s.Message))
	}
	prevHash := "NULL"
	if s.PrevStampHash != "" {
		prevHash = fmt.Sprintf("'%s'", EscapeSQL(s.PrevStampHash))
	}
	stampIdx := "NULL"
	if s.StampIndex >= 0 {
		stampIdx = fmt.Sprintf("%d", s.StampIndex)
	}

	script := fmt.Sprintf(`USE %s;

INSERT INTO stamps (id, author, subject, valence, confidence, severity, context_id, context_type, stamp_type, pilot_cohort, skill_tags, message, prev_stamp_hash, stamp_index, created_at)
VALUES ('%s', '%s', '%s', '%s', %f, '%s', %s, %s, %s, %s, %s, %s, %s, %s, '%s');

CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl stamp: %s stamps %s');
`,
		dbName,
		EscapeSQL(s.ID), EscapeSQL(s.Author), EscapeSQL(s.Subject),
		EscapeSQL(s.Valence), s.Confidence, EscapeSQL(s.Severity),
		contextID, contextType, stampType, pilotCohort, skillTags, message,
		prevHash, stampIdx, now,
		EscapeSQL(s.Author), EscapeSQL(s.Subject))

	return doltSQLScriptWithRetry(townRoot, script)
}

// QueryLastStampForSubjectDB fetches the most recent stamp for a subject from the specified database.
func QueryLastStampForSubjectDB(townRoot, dbName, subject string) (*StampRecord, error) {
	query := fmt.Sprintf(`USE %s; SELECT id, COALESCE(stamp_index, -1) as stamp_index FROM stamps WHERE subject='%s' ORDER BY stamp_index DESC, created_at DESC LIMIT 1;`,
		dbName, EscapeSQL(subject))

	output, err := doltSQLQuery(townRoot, query)
	if err != nil {
		return nil, err
	}

	rows := parseSimpleCSV(output)
	if len(rows) == 0 {
		return nil, nil
	}

	row := rows[0]
	idx := 0
	if v, ok := row["stamp_index"]; ok && v != "-1" && v != "" {
		fmt.Sscanf(v, "%d", &idx)
	} else {
		idx = -1
	}

	return &StampRecord{
		ID:         row["id"],
		StampIndex: idx,
	}, nil
}

// InsertWanted inserts a new wanted item into the wl-commons database.
func InsertWanted(townRoot string, item *WantedItem) error {
	if item.ID == "" {
		return fmt.Errorf("wanted item ID cannot be empty")
	}
	if item.Title == "" {
		return fmt.Errorf("wanted item title cannot be empty")
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	tagsJSON := "NULL"
	if len(item.Tags) > 0 {
		escaped := make([]string, len(item.Tags))
		for i, t := range item.Tags {
			t = strings.ReplaceAll(t, `\`, `\\`)
			t = strings.ReplaceAll(t, `"`, `\"`)
			t = strings.ReplaceAll(t, "'", "''")
			escaped[i] = t
		}
		tagsJSON = fmt.Sprintf("'[\"%s\"]'", strings.Join(escaped, `","`))
	}

	descField := "NULL"
	if item.Description != "" {
		descField = fmt.Sprintf("'%s'", EscapeSQL(item.Description))
	}
	projectField := "NULL"
	if item.Project != "" {
		projectField = fmt.Sprintf("'%s'", EscapeSQL(item.Project))
	}
	typeField := "NULL"
	if item.Type != "" {
		typeField = fmt.Sprintf("'%s'", EscapeSQL(item.Type))
	}
	postedByField := "NULL"
	if item.PostedBy != "" {
		postedByField = fmt.Sprintf("'%s'", EscapeSQL(item.PostedBy))
	}
	effortField := "'medium'"
	if item.EffortLevel != "" {
		effortField = fmt.Sprintf("'%s'", EscapeSQL(item.EffortLevel))
	}
	status := "'open'"
	if item.Status != "" {
		status = fmt.Sprintf("'%s'", EscapeSQL(item.Status))
	}

	script := fmt.Sprintf(`USE %s;

INSERT INTO wanted (id, title, description, project, type, priority, tags, posted_by, status, effort_level, created_at, updated_at)
VALUES ('%s', '%s', %s, %s, %s, %d, %s, %s, %s, %s, '%s', '%s');

CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl post: %s');
`,
		WLCommonsDB,
		EscapeSQL(item.ID), EscapeSQL(item.Title), descField, projectField, typeField,
		item.Priority, tagsJSON, postedByField, status, effortField,
		now, now,
		EscapeSQL(item.Title))

	return doltSQLScriptWithRetry(townRoot, script)
}

// ClaimWanted updates a wanted item's status to claimed.
// Returns an error if the item does not exist or is not open.
//
// Uses a single-script approach: UPDATE + DOLT_ADD + DOLT_COMMIT in one
// invocation. If the UPDATE matches 0 rows (item not open), the working set
// is unchanged and DOLT_COMMIT fails with "nothing to commit" — which we
// map to a precondition error. This avoids splitting into separate sessions
// and eliminates the need for DOLT_RESET on failure.
func ClaimWanted(townRoot, wantedID, rigHandle string) error {
	script := fmt.Sprintf(`USE %s;
UPDATE wanted SET claimed_by='%s', status='claimed', updated_at=NOW()
  WHERE id='%s' AND status='open';
CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl claim: %s');
`, WLCommonsDB, EscapeSQL(rigHandle), EscapeSQL(wantedID), EscapeSQL(wantedID))

	err := doltSQLScriptWithRetry(townRoot, script)
	if err == nil {
		return nil
	}
	if isNothingToCommit(err) {
		return fmt.Errorf("wanted item %q is not open or does not exist", wantedID)
	}
	return fmt.Errorf("claim failed: %w", err)
}

// SubmitCompletion inserts a completion record and updates the wanted status.
// The item must have status='claimed' AND claimed_by=rigHandle to prevent
// completing an item claimed by another rig.
//
// Uses a single-script approach like ClaimWanted. The INSERT uses INSERT IGNORE
// with a SELECT conditional on status='in_review' AND claimed_by AND NOT EXISTS
// (prior completion). INSERT IGNORE makes the script idempotent on retry since
// completions.id is a PRIMARY KEY. NOT EXISTS prevents multiple completions per
// wanted item, ensuring the lifecycle is strictly post→claim→done.
func SubmitCompletion(townRoot, completionID, wantedID, rigHandle, evidence string) error {
	script := fmt.Sprintf(`USE %s;
UPDATE wanted SET status='in_review', evidence_url='%s', updated_at=NOW()
  WHERE id='%s' AND status='claimed' AND claimed_by='%s';
INSERT IGNORE INTO completions (id, wanted_id, completed_by, evidence, completed_at)
  SELECT '%s', '%s', '%s', '%s', NOW()
  FROM wanted WHERE id='%s' AND status='in_review' AND claimed_by='%s'
  AND NOT EXISTS (SELECT 1 FROM completions WHERE wanted_id='%s');
CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl done: %s');
`,
		WLCommonsDB,
		EscapeSQL(evidence), EscapeSQL(wantedID), EscapeSQL(rigHandle),
		EscapeSQL(completionID), EscapeSQL(wantedID), EscapeSQL(rigHandle), EscapeSQL(evidence),
		EscapeSQL(wantedID), EscapeSQL(rigHandle), EscapeSQL(wantedID),
		EscapeSQL(wantedID))

	err := doltSQLScriptWithRetry(townRoot, script)
	if err == nil {
		return nil
	}
	if isNothingToCommit(err) {
		return fmt.Errorf("wanted item %q is not claimed by %q or does not exist", wantedID, rigHandle)
	}
	return fmt.Errorf("completion failed: %w", err)
}

// QueryWanted fetches a wanted item by ID. Returns nil if not found.
func QueryWanted(townRoot, wantedID string) (*WantedItem, error) {
	query := fmt.Sprintf(`USE %s; SELECT id, title, status, COALESCE(claimed_by, '') as claimed_by FROM wanted WHERE id='%s';`,
		WLCommonsDB, EscapeSQL(wantedID))

	output, err := doltSQLQuery(townRoot, query)
	if err != nil {
		return nil, err
	}

	rows := parseSimpleCSV(output)
	if len(rows) == 0 {
		return nil, fmt.Errorf("wanted item %q not found", wantedID)
	}

	row := rows[0]
	item := &WantedItem{
		ID:        row["id"],
		Title:     row["title"],
		Status:    row["status"],
		ClaimedBy: row["claimed_by"],
	}
	return item, nil
}

// QueryWantedFull fetches all fields of a wanted item by ID. Returns nil if not found.
func QueryWantedFull(townRoot, wantedID string) (*WantedItem, error) {
	query := fmt.Sprintf(`USE %s; SELECT id, title, COALESCE(description, '') as description, COALESCE(project, '') as project, COALESCE(type, '') as type, priority, COALESCE(tags, JSON_ARRAY()) as tags, COALESCE(posted_by, '') as posted_by, COALESCE(claimed_by, '') as claimed_by, status, COALESCE(effort_level, '') as effort_level, COALESCE(evidence_url, '') as evidence_url, COALESCE(sandbox_required, 0) as sandbox_required, COALESCE(CAST(created_at AS CHAR), '') as created_at, COALESCE(CAST(updated_at AS CHAR), '') as updated_at FROM wanted WHERE id='%s';`,
		WLCommonsDB, EscapeSQL(wantedID))

	output, err := doltSQLQuery(townRoot, query)
	if err != nil {
		return nil, err
	}

	rows := parseSimpleCSV(output)
	if len(rows) == 0 {
		return nil, fmt.Errorf("wanted item %q not found", wantedID)
	}

	row := rows[0]
	item := &WantedItem{
		ID:          row["id"],
		Title:       row["title"],
		Description: row["description"],
		Project:     row["project"],
		Type:        row["type"],
		PostedBy:    row["posted_by"],
		ClaimedBy:   row["claimed_by"],
		Status:      row["status"],
		EffortLevel: row["effort_level"],
		EvidenceURL: row["evidence_url"],
		CreatedAt:   row["created_at"],
		UpdatedAt:   row["updated_at"],
	}
	if p := row["priority"]; p != "" {
		_, _ = fmt.Sscanf(p, "%d", &item.Priority)
	}
	if row["sandbox_required"] == "1" {
		item.SandboxRequired = true
	}
	if tagsJSON := row["tags"]; tagsJSON != "" && tagsJSON != "[]" {
		_ = json.Unmarshal([]byte(tagsJSON), &item.Tags)
	}
	return item, nil
}

// doltSQLQuery executes a SQL query and returns the raw CSV output.
func doltSQLQuery(townRoot, query string) (string, error) {
	config := DefaultConfig(townRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := buildDoltSQLCmd(ctx, config, "-r", "csv", "-q", query)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("dolt sql query failed: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

// parseSimpleCSV parses CSV output from dolt sql into a slice of maps.
// Handles quoted fields containing commas and escaped quotes.
func parseSimpleCSV(data string) []map[string]string {
	lines := strings.Split(strings.TrimSpace(data), "\n")
	if len(lines) < 2 {
		return nil
	}

	headers := parseCSVLine(lines[0])
	var result []map[string]string

	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		fields := parseCSVLine(line)
		row := make(map[string]string)
		for i, h := range headers {
			if i < len(fields) {
				row[strings.TrimSpace(h)] = strings.TrimSpace(fields[i])
			}
		}
		result = append(result, row)
	}
	return result
}

// parseCSVLine parses a single CSV line, handling quoted fields.
func parseCSVLine(line string) []string {
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

// StampRecord represents a row in the stamps table.
type StampRecord struct {
	ID            string
	Author        string
	Subject       string
	Valence       string // JSON string
	Confidence    float64
	Severity      string
	ContextID     string
	ContextType   string
	StampType     string
	PilotCohort   string
	SkillTags     string // JSON array string
	Message       string
	PrevStampHash string
	StampIndex    int
	CreatedAt     string
}

// InsertStamp inserts a new stamp record into the wl-commons stamps table.
func InsertStamp(townRoot string, s *StampRecord) error {
	if s.ID == "" {
		return fmt.Errorf("stamp ID cannot be empty")
	}
	if s.Author == "" || s.Subject == "" {
		return fmt.Errorf("stamp author and subject are required")
	}
	if s.Author == s.Subject {
		return fmt.Errorf("stamp author cannot equal subject")
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	if s.CreatedAt != "" {
		now = s.CreatedAt
	}

	contextID := "NULL"
	if s.ContextID != "" {
		contextID = fmt.Sprintf("'%s'", EscapeSQL(s.ContextID))
	}
	contextType := "NULL"
	if s.ContextType != "" {
		contextType = fmt.Sprintf("'%s'", EscapeSQL(s.ContextType))
	}
	stampType := "NULL"
	if s.StampType != "" {
		stampType = fmt.Sprintf("'%s'", EscapeSQL(s.StampType))
	}
	pilotCohort := "NULL"
	if s.PilotCohort != "" {
		pilotCohort = fmt.Sprintf("'%s'", EscapeSQL(s.PilotCohort))
	}
	skillTags := "NULL"
	if s.SkillTags != "" {
		skillTags = fmt.Sprintf("'%s'", EscapeSQL(s.SkillTags))
	}
	message := "NULL"
	if s.Message != "" {
		message = fmt.Sprintf("'%s'", EscapeSQL(s.Message))
	}
	prevHash := "NULL"
	if s.PrevStampHash != "" {
		prevHash = fmt.Sprintf("'%s'", EscapeSQL(s.PrevStampHash))
	}
	stampIdx := "NULL"
	if s.StampIndex >= 0 {
		stampIdx = fmt.Sprintf("%d", s.StampIndex)
	}

	script := fmt.Sprintf(`USE %s;

INSERT INTO stamps (id, author, subject, valence, confidence, severity, context_id, context_type, stamp_type, pilot_cohort, skill_tags, message, prev_stamp_hash, stamp_index, created_at)
VALUES ('%s', '%s', '%s', '%s', %f, '%s', %s, %s, %s, %s, %s, %s, %s, %s, '%s');

CALL DOLT_ADD('-A');
CALL DOLT_COMMIT('-m', 'wl stamp: %s stamps %s');
`,
		WLCommonsDB,
		EscapeSQL(s.ID), EscapeSQL(s.Author), EscapeSQL(s.Subject),
		EscapeSQL(s.Valence), s.Confidence, EscapeSQL(s.Severity),
		contextID, contextType, stampType, pilotCohort, skillTags, message,
		prevHash, stampIdx, now,
		EscapeSQL(s.Author), EscapeSQL(s.Subject))

	return doltSQLScriptWithRetry(townRoot, script)
}

// QueryLastStampForSubject fetches the most recent stamp for a subject rig,
// used to compute passbook chain linkage (prev_stamp_hash and stamp_index).
// Returns nil (not an error) if no stamps exist for the subject.
func QueryLastStampForSubject(townRoot, subject string) (*StampRecord, error) {
	query := fmt.Sprintf(`USE %s; SELECT id, COALESCE(stamp_index, -1) as stamp_index FROM stamps WHERE subject='%s' ORDER BY stamp_index DESC, created_at DESC LIMIT 1;`,
		WLCommonsDB, EscapeSQL(subject))

	output, err := doltSQLQuery(townRoot, query)
	if err != nil {
		return nil, err
	}

	rows := parseSimpleCSV(output)
	if len(rows) == 0 {
		return nil, nil
	}

	row := rows[0]
	idx := 0
	if v, ok := row["stamp_index"]; ok && v != "-1" && v != "" {
		fmt.Sscanf(v, "%d", &idx)
	} else {
		idx = -1
	}

	return &StampRecord{
		ID:         row["id"],
		StampIndex: idx,
	}, nil
}
