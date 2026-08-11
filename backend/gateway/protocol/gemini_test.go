package protocol

import "testing"

func TestGeminiPath(t *testing.T) {
	tests := []struct {
		name   string
		model  string
		input  string
		stream bool
		want   string
	}{
		{
			name:  "generate content",
			model: "gemini-2.5-flash",
			input: "/v1/chat/completions",
			want:  "/v1beta/models/gemini-2.5-flash:generateContent",
		},
		{
			name:   "stream content",
			model:  "gemini-2.5-flash",
			input:  "/v1/chat/completions",
			stream: true,
			want:   "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
		},
		{
			name:  "native resource name",
			model: "models/gemini-2.5-flash",
			input: "/v1beta/models/gemini-2.5-flash:generateContent",
			want:  "/v1beta/models/gemini-2.5-flash:generateContent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GeminiPath(tt.model, tt.input, tt.stream); got != tt.want {
				t.Fatalf("GeminiPath(%q, %q, %t) = %q, want %q", tt.model, tt.input, tt.stream, got, tt.want)
			}
		})
	}
}
