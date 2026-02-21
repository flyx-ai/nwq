package taskinfo

import "context"

type workflowIDContextKey struct{}

func WithWorkflowID(ctx context.Context, workflowID string) context.Context {
	return context.WithValue(ctx, workflowIDContextKey{}, workflowID)
}

func GetWorkflowID(ctx context.Context) (string, bool) {
	workflowID, ok := ctx.Value(workflowIDContextKey{}).(string)
	return workflowID, ok
}
