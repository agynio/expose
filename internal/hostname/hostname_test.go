package hostname

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

var exposureID = uuid.MustParse("7f3a2c91-1111-2222-3333-444455556666")

func TestDeriveSandbox(t *testing.T) {
	got := Derive(exposureID, "acme", Owner{Kind: OwnerKindSandbox, SandboxName: "super-sandbox"})
	if want := "super-sandbox.acme.agyn"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestDeriveAgentInstance(t *testing.T) {
	got := Derive(exposureID, "acme", Owner{
		Kind:           OwnerKindAgentInstance,
		Nickname:       "bob",
		InstanceSuffix: "research",
	})
	if want := "research.bob.acme.agyn"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A sandbox occupies one leaf under the organization label while an agent class
// occupies everything beneath its own, so the two namespaces cannot collide.
func TestSandboxAndAgentShareANameWithoutColliding(t *testing.T) {
	sandbox := Derive(exposureID, "acme", Owner{Kind: OwnerKindSandbox, SandboxName: "bob"})
	instance := Derive(exposureID, "acme", Owner{
		Kind:           OwnerKindAgentInstance,
		Nickname:       "bob",
		InstanceSuffix: "research",
	})
	if sandbox == instance {
		t.Fatalf("sandbox and instance addresses collided on %q", sandbox)
	}
	if want := "bob.acme.agyn"; sandbox != want {
		t.Fatalf("sandbox: got %q, want %q", sandbox, want)
	}
	if want := "research.bob.acme.agyn"; instance != want {
		t.Fatalf("instance: got %q, want %q", instance, want)
	}
}

// Every readable address is at least three labels, so none can collide with a
// two-label platform service name.
func TestReadableAddressesNeverCollideWithPlatformNames(t *testing.T) {
	platform := map[string]bool{"gateway.agyn": true, "llm-proxy.agyn": true, "tracing.agyn": true}
	for _, owner := range []Owner{
		{Kind: OwnerKindSandbox, SandboxName: "gateway"},
		{Kind: OwnerKindAgentInstance, Nickname: "llm-proxy", InstanceSuffix: "tracing"},
	} {
		got := Derive(exposureID, "acme", owner)
		if platform[got] {
			t.Fatalf("address %q collided with a platform service name", got)
		}
		if n := strings.Count(got, "."); n < 2 {
			t.Fatalf("address %q has fewer than three labels", got)
		}
	}
}

func TestFallback(t *testing.T) {
	want := "exposed-" + exposureID.String() + ".agyn"
	if got := Fallback(exposureID); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestDeriveFallsBack(t *testing.T) {
	fallback := Fallback(exposureID)
	cases := []struct {
		name  string
		slug  string
		owner Owner
	}{
		{"no nickname", "acme", Owner{Kind: OwnerKindAgentInstance, InstanceSuffix: "research"}},
		{"no suffix", "acme", Owner{Kind: OwnerKindAgentInstance, Nickname: "bob"}},
		{"underscore in nickname", "acme", Owner{Kind: OwnerKindAgentInstance, Nickname: "my_bot", InstanceSuffix: "research"}},
		{"underscore in suffix", "acme", Owner{Kind: OwnerKindAgentInstance, Nickname: "bob", InstanceSuffix: "code_review"}},
		{"no sandbox name", "acme", Owner{Kind: OwnerKindSandbox}},
		{"no org slug", "", Owner{Kind: OwnerKindSandbox, SandboxName: "super-sandbox"}},
		{"org slug not a label", "acme-", Owner{Kind: OwnerKindSandbox, SandboxName: "super-sandbox"}},
		{"unknown owner kind", "acme", Owner{Kind: OwnerKindUnspecified}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Derive(exposureID, tc.slug, tc.owner); got != fallback {
				t.Fatalf("got %q, want fallback %q", got, fallback)
			}
			if IsReadable(tc.slug, tc.owner) {
				t.Fatal("IsReadable reported true for a case that falls back")
			}
		})
	}
}

func TestIsDNSLabel(t *testing.T) {
	valid := []string{"a", "acme", "super-sandbox", "7a2f", "a-b-c", strings.Repeat("x", 63)}
	for _, s := range valid {
		if !IsDNSLabel(s) {
			t.Errorf("IsDNSLabel(%q) = false, want true", s)
		}
	}
	invalid := []string{
		"",
		"-acme",
		"acme-",
		"my_bot",
		"Acme",
		"a.b",
		"acme ",
		strings.Repeat("x", 64),
	}
	for _, s := range invalid {
		if IsDNSLabel(s) {
			t.Errorf("IsDNSLabel(%q) = true, want false", s)
		}
	}
}

func TestDeriveTrimsSurroundingSpace(t *testing.T) {
	got := Derive(exposureID, " acme ", Owner{Kind: OwnerKindSandbox, SandboxName: " super-sandbox "})
	if want := "super-sandbox.acme.agyn"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
