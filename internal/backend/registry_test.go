package backend

import (
	"reflect"
	"testing"
)

func TestDefaultRegistryRemoteWSIsOptIn(t *testing.T) {
	without := DefaultRegistry(false)
	for _, d := range without {
		if d.ID == RemoteWS {
			t.Fatalf("remote-ws should be absent without configuration")
		}
	}

	with := DefaultRegistry(true)
	var remote *Definition
	for i := range with {
		if with[i].ID == RemoteWS {
			remote = &with[i]
		}
	}
	if remote == nil {
		t.Fatalf("remote-ws should be advertised when configured")
	}
	if !remote.Capabilities.Remote || !remote.Capabilities.History || !remote.Capabilities.Interactions {
		t.Fatalf("remote-ws capabilities = %+v", remote.Capabilities)
	}
}

func TestDefaultRegistryOllamaUsesInstalledSmokeModel(t *testing.T) {
	defs := DefaultRegistry(false)
	var ollama *Definition
	for i := range defs {
		if defs[i].ID == Ollama {
			ollama = &defs[i]
		}
	}
	if ollama == nil {
		t.Fatalf("ollama backend missing")
	}
	if ollama.DefaultModel != "qwen2.5:7b" {
		t.Fatalf("ollama default = %q", ollama.DefaultModel)
	}
	if len(ollama.Models) == 0 || ollama.Models[0].ID != "qwen2.5:7b" {
		t.Fatalf("ollama models = %+v", ollama.Models)
	}
}

func TestDefaultRegistryAdvertisesSteeringOnlyForCodex(t *testing.T) {
	for _, definition := range DefaultRegistry(false) {
		switch definition.ID {
		case Codex:
			if !definition.Capabilities.Steering {
				t.Fatal("Codex must advertise same-turn steering")
			}
		case Claude, Ollama:
			if definition.Capabilities.Steering {
				t.Fatalf("%s must not advertise unsupported steering", definition.ID)
			}
		}
	}
}

func TestDefaultRegistryIncludesAstraFallbackCapabilities(t *testing.T) {
	definitions := DefaultRegistry(false)
	var codex *Definition
	for i := range definitions {
		if definitions[i].ID == Codex {
			codex = &definitions[i]
			break
		}
	}
	if codex == nil {
		t.Fatal("Codex backend missing")
	}
	for _, model := range codex.Models {
		if model.ID != "gpt-6-astra" {
			continue
		}
		if model.DefaultReasoningEffort != "medium" || len(model.SupportedReasoningEfforts) != 5 {
			t.Fatalf("Astra fallback metadata = %+v", model)
		}
		if len(model.InputModalities) != 2 || model.InputModalities[1] != "image" {
			t.Fatalf("Astra input modalities = %v", model.InputModalities)
		}
		return
	}
	t.Fatal("Astra missing from Codex fallback registry")
}

func TestDefaultRegistryIncludesSol6WithoutChangingDefault(t *testing.T) {
	for _, definition := range DefaultRegistry(false) {
		if definition.ID != Codex {
			continue
		}
		if definition.DefaultModel != "gpt-5.6-sol" {
			t.Fatalf("adding an option changed the default: %s", definition.DefaultModel)
		}
		for _, model := range definition.Models {
			if model.ID != "gpt-6-sol" {
				continue
			}
			if model.Label != "GPT-6-Sol" || model.DefaultReasoningEffort != "medium" || model.IsDefault {
				t.Fatalf("Sol 6 metadata = %+v", model)
			}
			if !reflect.DeepEqual(model.SupportedReasoningEfforts, []string{"low", "medium", "high", "xhigh", "max", "ultra"}) || !reflect.DeepEqual(model.InputModalities, []string{"text", "image"}) {
				t.Fatalf("Sol 6 capabilities = %+v", model)
			}
			return
		}
	}
	t.Fatal("Sol 6 missing from Codex fallback registry")
}
