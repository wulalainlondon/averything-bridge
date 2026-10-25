package workitems

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

type CreateWorkflowInput struct {
	ID          string
	ProjectID   string
	Name        string
	Description string
	Definition  WorkflowDefinition
}

func ValidateWorkflow(definition WorkflowDefinition) error {
	if len(definition.Nodes) == 0 {
		return errors.New("workflow requires at least one node")
	}
	nodes := map[string]bool{}
	for _, node := range definition.Nodes {
		if strings.TrimSpace(node.ID) == "" || strings.TrimSpace(node.Name) == "" || nodes[node.ID] {
			return errors.New("workflow node ids and names must be unique and non-empty")
		}
		nodes[node.ID] = true
	}
	graph := map[string][]string{}
	for _, edge := range definition.Edges {
		if !nodes[edge.From] || !nodes[edge.To] || edge.From == edge.To {
			return errors.New("workflow edge references an invalid node")
		}
		graph[edge.From] = append(graph[edge.From], edge.To)
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) bool
	visit = func(id string) bool {
		if visiting[id] {
			return false
		}
		if visited[id] {
			return true
		}
		visiting[id] = true
		for _, next := range graph[id] {
			if !visit(next) {
				return false
			}
		}
		delete(visiting, id)
		visited[id] = true
		return true
	}
	for id := range nodes {
		if !visit(id) {
			return errors.New("workflow must be acyclic")
		}
	}
	return nil
}

func (s *Store) CreateWorkflow(ctx context.Context, in CreateWorkflowInput) (Workflow, error) {
	if strings.TrimSpace(in.Name) == "" {
		return Workflow{}, errors.New("workflow name is required")
	}
	if err := ValidateWorkflow(in.Definition); err != nil {
		return Workflow{}, err
	}
	if in.ID == "" {
		in.ID = newID("wf")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Workflow{}, err
	}
	defer tx.Rollback()
	if err := s.requireProject(ctx, tx, in.ProjectID); err != nil {
		return Workflow{}, err
	}
	now := s.now().UnixMilli()
	workflow := Workflow{ID: in.ID, ProjectID: in.ProjectID, Name: strings.TrimSpace(in.Name),
		Description: strings.TrimSpace(in.Description), Version: 1, Definition: in.Definition,
		CreatedAt: now, UpdatedAt: now}
	body, _ := json.Marshal(workflow.Definition)
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_workflows
		(id,project_id,name,description,version,definition,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,
		workflow.ID, workflow.ProjectID, workflow.Name, workflow.Description, workflow.Version, string(body), now, now); err != nil {
		return Workflow{}, err
	}
	if _, err := recordChange(ctx, tx, "workflow", workflow.ID, "workflow_created", ChangePayload{Workflow: &workflow}, now); err != nil {
		return Workflow{}, err
	}
	return workflow, tx.Commit()
}

func (s *Store) listWorkflows(ctx context.Context) ([]Workflow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,project_id,name,description,version,definition,created_at,updated_at,archived_at
		FROM work_workflows WHERE archived_at IS NULL ORDER BY project_id,created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var workflows []Workflow
	for rows.Next() {
		var workflow Workflow
		var body string
		if err := rows.Scan(&workflow.ID, &workflow.ProjectID, &workflow.Name, &workflow.Description,
			&workflow.Version, &body, &workflow.CreatedAt, &workflow.UpdatedAt, &workflow.ArchivedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(body), &workflow.Definition); err != nil {
			return nil, err
		}
		workflows = append(workflows, workflow)
	}
	return workflows, rows.Err()
}

func (s *Store) requireWorkflow(ctx context.Context, tx *sql.Tx, workflowID, projectID, nodeID string) error {
	if workflowID == "" {
		return nil
	}
	var storedProject, body string
	if err := tx.QueryRowContext(ctx, `SELECT project_id,definition FROM work_workflows WHERE id=? AND archived_at IS NULL`, workflowID).Scan(&storedProject, &body); err != nil {
		return err
	}
	if storedProject != projectID {
		return ErrCrossProject
	}
	if nodeID == "" {
		return nil
	}
	var definition WorkflowDefinition
	if json.Unmarshal([]byte(body), &definition) != nil {
		return errors.New("invalid stored workflow")
	}
	for _, node := range definition.Nodes {
		if node.ID == nodeID {
			return nil
		}
	}
	return ErrNotFound
}

// BuiltInWorkflows are intentionally simple, inspectable DAGs. The UI copies
// one into the authoritative store before assigning it to work.
func BuiltInWorkflows() []CreateWorkflowInput {
	return []CreateWorkflowInput{
		{Name: "Delivery loop", Description: "Plan, implement, verify, then ask for human acceptance.", Definition: WorkflowDefinition{
			Nodes: []WorkflowNode{{ID: "plan", Name: "Plan", Kind: "human"}, {ID: "implement", Name: "Implement", Kind: "agent"}, {ID: "verify", Name: "Verify", Kind: "agent"}, {ID: "accept", Name: "Accept", Kind: "human"}},
			Edges: []WorkflowEdge{{From: "plan", To: "implement"}, {From: "implement", To: "verify"}, {From: "verify", To: "accept"}},
		}},
		{Name: "Research loop", Description: "Collect evidence, synthesize, review, then decide.", Definition: WorkflowDefinition{
			Nodes: []WorkflowNode{{ID: "collect", Name: "Collect", Kind: "agent"}, {ID: "synthesize", Name: "Synthesize", Kind: "agent"}, {ID: "review", Name: "Review", Kind: "human"}, {ID: "decide", Name: "Decide", Kind: "human"}},
			Edges: []WorkflowEdge{{From: "collect", To: "synthesize"}, {From: "synthesize", To: "review"}, {From: "review", To: "decide"}},
		}},
	}
}
