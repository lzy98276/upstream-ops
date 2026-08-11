package gateway

import "testing"

func TestJoinUpstreamURLGeminiVersionedAndUnversionedBase(t *testing.T) {
	const path = "/v1beta/models/gemini-2.5-flash:generateContent"
	cases := map[string]string{
		"unversioned": "https://generativelanguage.googleapis.com" + path,
		"v1beta":      "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			base := "https://generativelanguage.googleapis.com"
			if name == "v1beta" {
				base += "/v1beta"
			}
			if got := joinUpstreamURL(base, path); got != want {
				t.Fatalf("joinUpstreamURL(%q, %q) = %q, want %q", base, path, got, want)
			}
		})
	}
}
