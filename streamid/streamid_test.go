package streamid

import (
	"reflect"
	"testing"
)

// TestLess checks both that a < b and !(b < a)
func TestLess(t *testing.T) {
	type args struct {
		a StreamID
		b StreamID
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test 1",
			args: args{
				StreamID{0, 0},
				StreamID{0, 1},
			},
		},
		{
			name: "test 2",
			args: args{
				StreamID{0, 0},
				StreamID{1, 0},
			},
		},
		{
			name: "test 3",
			args: args{
				StreamID{0, 0},
				StreamID{1, 1},
			},
		},
		{
			name: "test 4",
			args: args{
				StreamID{23, 35},
				StreamID{23, 37},
			},
		},
		{
			name: "test 5",
			args: args{
				StreamID{23, 35},
				StreamID{30, 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isLess := Less(tt.args.a, tt.args.b); !isLess {
				t.Errorf("Less() = %v, want %v", isLess, true)
			}
			if isLess := Less(tt.args.b, tt.args.a); isLess {
				t.Errorf("Less() = %v, want %v", isLess, false)
			}
		})
	}
}

func TestStreamID_Less(t *testing.T) {
	type fields struct {
		time int64
		seq  int64
	}
	type args struct {
		b StreamID
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "simple 1",
			fields: fields{0, 0},
			args:   args{StreamID{0, 1}},
			want:   true,
		},
		{
			name:   "simple 2",
			fields: fields{1, 0},
			args:   args{StreamID{0, 0}},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := StreamID{
				time: tt.fields.time,
				seq:  tt.fields.seq,
			}
			if got := a.Less(tt.args.b); got != tt.want {
				t.Errorf("StreamID.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	type args struct {
		raw string
	}
	tests := []struct {
		name    string
		args    args
		want    StreamID
		wantErr bool
	}{
		{
			name:    "simple",
			args:    args{"0-0"},
			want:    StreamID{0, 0},
			wantErr: false,
		},
		{
			name:    "bad ID 1",
			args:    args{"0-"},
			wantErr: true,
		},
		{
			name:    "bad ID 2",
			args:    args{""},
			wantErr: true,
		},
		{
			name:    "bad ID 3",
			args:    args{"-0"},
			wantErr: true,
		},
		{
			name:    "bad ID 4",
			args:    args{"0"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMustParse(t *testing.T) {
	type args struct {
		raw string
	}
	tests := []struct {
		name      string
		args      args
		want      StreamID
		wantPanic bool
	}{
		{
			name:      "simple",
			args:      args{"0-0"},
			want:      StreamID{0, 0},
			wantPanic: false,
		},
		{
			name:      "bad ID 1",
			args:      args{"0-"},
			wantPanic: true,
		},
		{
			name:      "bad ID 2",
			args:      args{""},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wantPanic {
						panic(r)
					}
				}
			}()
			if got := MustParse(tt.args.raw); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MustParse() = %v, want %v", got, tt.want)
			}
			if tt.wantPanic {
				t.Errorf("MustParse() did not panic when a panic was expected")
			}
		})
	}
}

func TestStreamID_String(t *testing.T) {
	type fields struct {
		time int64
		seq  int64
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "zero",
			fields: fields{time: 0, seq: 0},
			want:   "0-0",
		},
		{
			name:   "one zero",
			fields: fields{time: 1, seq: 0},
			want:   "1-0",
		},
		{
			name:   "multi-digit",
			fields: fields{time: 25, seq: 31},
			want:   "25-31",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := StreamID{
				time: tt.fields.time,
				seq:  tt.fields.seq,
			}
			if got := s.String(); got != tt.want {
				t.Errorf("StreamID.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
