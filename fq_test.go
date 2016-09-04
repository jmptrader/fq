package fq

import "testing"

func TestNewWriter(t *testing.T) {
	w, err := NewWriter("test.log")
	if err != nil {
		t.Fatal(err)
	}

	tests := []string{
		"hello world!",
		"the second line",
		"the last thing I'm going to write",
	}
	for _, test := range tests {
		_, err = w.Write([]byte(test))
		if err != nil {
			t.Fatal(err)
		}
	}

	r, err := NewReader("test.log")
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		b, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		if test != string(b) {
			t.Errorf("expected:%s, got %s", test, string(b))
		}
	}

	b, err := r.ReadAt(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != tests[1] {
		t.Errorf("ReadAt expected: %s, got %s", tests[1], string(b))
	}
}
