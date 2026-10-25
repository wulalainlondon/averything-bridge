package linebridge

type Subject struct {
	Ref  string `json:"subject_ref"`
	Name string `json:"name"`
	Kind string `json:"kind"`
}
