package control

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const approvedArtifactLimit = 16 << 10

type approvedReview struct {
	Action       string   `json:"action"`
	Reason       string   `json:"reason"`
	DecisionTest string   `json:"decision_test"`
	EvidenceIDs  []string `json:"evidence_ids"`
	SourceSHA256 string   `json:"source_sha256"`
	ContractID   string   `json:"contract_id"`
}

func (s *Server) approveRevision(req request) (any, string) {
	var p struct {
		ReviewTaskID   string `json:"review_task_id"`
		ArtifactSHA256 string `json:"artifact_sha256"`
	}
	if strict(req.Params, &p) != nil || p.ReviewTaskID != "consumer" || len(p.ArtifactSHA256) != sha256.Size*2 {
		return nil, "invalid_request"
	}
	if _, err := hex.DecodeString(p.ArtifactSHA256); err != nil {
		return nil, "invalid_request"
	}

	r, err := s.Store.CooperativeReceipt(p.ReviewTaskID)
	if err != nil {
		return nil, errCode(err)
	}
	if r.Task.ID != p.ReviewTaskID || r.Task.Role != "consumer" || r.Task.Project != s.Config.Project {
		return nil, "scope_mismatch"
	}
	if r.Status != "completed" {
		return nil, "conflict"
	}
	var reviewRef Reference
	if strict([]byte(r.Text), &reviewRef) != nil || reviewRef.Producer != r.Task.ID {
		return nil, "invalid_request"
	}
	reviewData, code := s.readApprovedArtifact(reviewRef.Artifact, "review.json")
	if code != "" {
		return nil, code
	}
	if reviewRef.Artifact.SHA256 != p.ArtifactSHA256 {
		return nil, "conflict"
	}
	var review approvedReview
	if json.Unmarshal(reviewData, &review) != nil {
		return nil, "invalid_request"
	}
	switch review.Action {
	case "revise":
	case "complete", "investigate", "defer":
		return nil, "unsupported"
	default:
		return nil, "invalid_request"
	}
	if strings.TrimSpace(review.Reason) == "" || strings.TrimSpace(review.DecisionTest) == "" || len(review.EvidenceIDs) == 0 {
		return nil, "invalid_request"
	}
	seen := make(map[string]struct{}, len(review.EvidenceIDs))
	for _, id := range review.EvidenceIDs {
		if _, duplicate := seen[id]; duplicate || !ident.MatchString(id) {
			return nil, "invalid_request"
		}
		seen[id] = struct{}{}
	}

	writer, err := s.Store.CooperativeReceipt("producer")
	if err != nil {
		return nil, errCode(err)
	}
	if writer.Task.ID != "producer" || writer.Task.Role != "producer" || writer.Task.Project != s.Config.Project {
		return nil, "scope_mismatch"
	}
	if writer.Status != "completed" {
		return nil, "conflict"
	}
	var documentRef Reference
	if strict([]byte(writer.Text), &documentRef) != nil || documentRef.Producer != writer.Task.ID {
		return nil, "invalid_request"
	}
	documentData, code := s.readApprovedArtifact(documentRef.Artifact, "document.json")
	if code != "" {
		return nil, code
	}
	if review.SourceSHA256 != documentRef.Artifact.SHA256 {
		return nil, "conflict"
	}
	var document struct {
		TaskContract json.RawMessage `json:"task_contract"`
	}
	var contract struct {
		ID       string `json:"id"`
		Evidence []struct {
			ID   string `json:"id"`
			Text string `json:"text"`
		} `json:"evidence"`
	}
	if json.Unmarshal(documentData, &document) != nil || json.Unmarshal(document.TaskContract, &contract) != nil || strings.TrimSpace(contract.ID) == "" {
		return nil, "invalid_request"
	}
	if review.ContractID != contract.ID {
		return nil, "conflict"
	}
	contractEvidence := make(map[string]struct{}, len(contract.Evidence))
	for _, observation := range contract.Evidence {
		if _, duplicate := contractEvidence[observation.ID]; duplicate || !ident.MatchString(observation.ID) || strings.TrimSpace(observation.Text) == "" {
			return nil, "invalid_request"
		}
		contractEvidence[observation.ID] = struct{}{}
	}
	for _, id := range review.EvidenceIDs {
		if _, exists := contractEvidence[id]; !exists {
			return nil, "invalid_request"
		}
	}
	input, err := json.Marshal(struct {
		OriginalDocument json.RawMessage `json:"original_document"`
		TaskContract     json.RawMessage `json:"task_contract"`
		Review           approvedReview  `json:"review"`
		ReviewSHA256     string          `json:"review_sha256"`
	}{documentData, document.TaskContract, review, reviewRef.Artifact.SHA256})
	if err != nil {
		return nil, "invalid_request"
	}
	return s.submitApprovedRevision(writer.Task, string(input))
}

// name is a workflow constant or is derived from the server's revision ID.
func (s *Server) readApprovedArtifact(ref Artifact, name string) ([]byte, string) {
	if ref.ID != name || ref.Version != "v1" {
		return nil, "invalid_request"
	}
	root, err := os.OpenRoot(s.Store.Root)
	if err != nil {
		return nil, "conflict"
	}
	defer root.Close()
	path := filepath.Join("artifacts", name)
	entry, err := root.Lstat(path)
	if err != nil || !entry.Mode().IsRegular() {
		return nil, "conflict"
	}
	f, err := root.Open(path)
	if err != nil {
		return nil, "conflict"
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > approvedArtifactLimit {
		return nil, "conflict"
	}
	data, err := io.ReadAll(io.LimitReader(f, approvedArtifactLimit))
	if err != nil || int64(len(data)) != info.Size() {
		return nil, "conflict"
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, "conflict"
	}
	return data, ""
}

