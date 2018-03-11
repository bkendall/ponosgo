package ponos

import (
	"testing"
)

var (
	testUri   = "amqp://fakehost:9999"
	testTasks = map[string]func(string){
		"test-queue": func(_ string) {},
	}
)

func TestCreateNewServer(t *testing.T) {
	_, err := NewServer(testUri, testTasks)
	if err != nil {
		t.Errorf("server creation should not fail: %s", err)
	}
}

func TestCreationWithZeroTasksFails(t *testing.T) {
	_, err := NewServer(testUri, map[string]func(string){})
	if err == nil {
		t.Error("server creation must fail without tasks")
	}
}
