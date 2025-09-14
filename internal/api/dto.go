package api

type EnqueueRequest struct {
	ID         string `json:"id"`
	Payload    string `json:"payload"`
	MaxRetries int    `json:"max_retries"`
}

type EnqueueResponse struct {
	ID string `json:"id"`
}
