package utils

import "testing"

func TestParseAppAndStreamFromPublishURL(t *testing.T) {
	app, stream, err := parseAppAndStreamFromPublishURL("srt://localhost:8890?streamid=publish:live/simulcast-fwv:<user>:<password>&pkt_size=1316")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if app != "live" {
		t.Fatalf("unexpected app: %s", app)
	}
	if stream != "simulcast-fwv" {
		t.Fatalf("unexpected stream: %s", stream)
	}
}

func TestResolvePublishURLWithAppSecret(t *testing.T) {
	input := "srt://localhost:8890?streamid=publish:live/simulcast-fwv:<user>:<password>&pkt_size=1316"
	got, err := ResolvePublishURL("", "test-secret", input)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if got == input {
		t.Fatalf("expected placeholders replaced, got same url: %s", got)
	}
}
