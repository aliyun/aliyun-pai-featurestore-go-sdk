package featurestore

import (
	"context"
	"os"
	"testing"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/domain"
)

func TestGetLLMConfigEmbedding(t *testing.T) {
	name := os.Getenv("LLM_CONFIG_NAME")
	if name == "" {
		t.Skip("LLM_CONFIG_NAME not set")
	}

	// project is empty on purpose: LLMConfig is workspace-scoped and does not
	// require a project; the FeatureDB client is bootstrapped from the config's workspace.
	client, err := createFeatureStoreClient(region, "")
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	llm, err := client.GetLLMConfig(name)
	if err != nil {
		t.Fatalf("GetLLMConfig(%q): %v", name, err)
	}
	t.Logf("llm config: name=%s model=%s model_type=%s", llm.Name, llm.Model, llm.ModelType)

	ctx := context.Background()
	var vecs [][]float32
	switch llm.ModelType {
	case domain.LLMModelTypeMultiModalEmbedding:
		vecs, err = llm.CreateMultiModalEmbeddings(ctx, []domain.MultiModalContent{{Text: "今天天气不错"}})
	default:
		vecs, err = llm.CreateTextEmbeddings(ctx, []string{"今天天气不错"})
	}
	if err != nil {
		t.Fatalf("create embeddings: %v", err)
	}
	if len(vecs) == 0 || len(vecs[0]) == 0 {
		t.Fatalf("empty embeddings")
	}
	t.Logf("ok: %d vector(s), dim=%d", len(vecs), len(vecs[0]))
}
