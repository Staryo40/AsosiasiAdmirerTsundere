package fencehost

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeFenceVerifiesShutOff(t *testing.T) {
	const fakeVirsh = `#!/bin/sh
printf '%s\n' "$*" >> "$VIRSH_LOG" || exit 98
[ "$LC_ALL" = C ] || exit 99
step=$1
if [ "$1" = domstate ]; then
	if [ -e "$VIRSH_LOG.initial" ]; then
		step=final
		printf '%s\n' "$VIRSH_FINAL_STATE"
	else
		: > "$VIRSH_LOG.initial"
		step=initial
		printf '%s\n' "$VIRSH_INITIAL_STATE"
	fi
fi
case " $VIRSH_FAIL " in
	*" $step "*) exit 1 ;;
esac
`
	for _, test := range []struct {
		name        string
		initial     string
		final       string
		fail        string
		wantDestroy bool
		wantErr     bool
	}{
		{name: "already shut off", initial: "shut off", fail: "destroy"},
		{name: "running", initial: "running", final: "shut off", wantDestroy: true},
		{name: "concurrent shutdown", initial: "running", final: "shut off", fail: "destroy", wantDestroy: true},
		{name: "failed initial check is not proof", initial: "shut off", final: "shut off", fail: "initial", wantDestroy: true},
		{name: "unknown initial state is not proof", initial: "no state", final: "shut off", wantDestroy: true},
		{name: "still running", initial: "running", final: "running", fail: "destroy", wantDestroy: true, wantErr: true},
		{name: "unrecognized final state", initial: "running", final: "shut off (unverified)", wantDestroy: true, wantErr: true},
		{name: "failed final check is not proof", initial: "running", final: "shut off", fail: "final", wantDestroy: true, wantErr: true},
		{name: "autostart could not be disabled", initial: "shut off", fail: "autostart", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "virsh"), []byte(fakeVirsh), 0700); err != nil {
				t.Fatal(err)
			}
			log := filepath.Join(dir, "virsh.log")
			t.Setenv("PATH", dir) // Never fall back to a real virsh.
			t.Setenv("LC_ALL", "test-locale")
			t.Setenv("VIRSH_LOG", log)
			t.Setenv("VIRSH_INITIAL_STATE", test.initial)
			t.Setenv("VIRSH_FINAL_STATE", test.final)
			t.Setenv("VIRSH_FAIL", test.fail)

			if err := (nativeOps{}).fence(t.Context(), testDomain); (err != nil) != test.wantErr {
				t.Errorf("fence returned %v, want error=%t", err, test.wantErr)
			}
			commands := []string{"autostart --disable " + testDomain}
			if test.fail != "autostart" {
				commands = append(commands, "domstate "+testDomain)
			}
			if test.wantDestroy {
				commands = append(commands, "destroy "+testDomain, "domstate "+testDomain)
			}
			contents, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := string(contents), strings.Join(commands, "\n")+"\n"; got != want {
				t.Errorf("virsh commands = %q, want %q", got, want)
			}
		})
	}
}
