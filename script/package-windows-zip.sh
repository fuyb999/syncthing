#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

readonly GOVERSIONINFO_PKG_DEFAULT="github.com/josephspurrier/goversioninfo/cmd/goversioninfo@v1.4.0"
readonly WINDOWS_ARCHES_DEFAULT="amd64 386 arm64"
readonly WINDOWS_TAGS_DEFAULT="sqlite_omit_load_extension sqlite_dbstat"

usage() {
	cat <<'EOF'
Usage: bash script/package-windows-zip.sh [target...]

Build Windows zip archives from Ubuntu for the given targets.
Defaults to: syncthing stdiscosrv strelaysrv

Required tools:
  - Go 1.25+
  - zig
  - goversioninfo (installed automatically if missing)

Optional environment variables:
  WINDOWS_ARCHES        Space separated list, default: amd64 386 arm64
  TAGS                  Go build tags, default: sqlite_omit_load_extension sqlite_dbstat
  GOVERSIONINFO_PKG     go install target for goversioninfo

Examples:
  bash build.sh package-windows
  WINDOWS_ARCHES="amd64 arm64" bash build.sh package-windows syncthing
EOF
}

die() {
	echo "error: $*" >&2
	exit 1
}

require_cmd() {
	command -v "$1" >/dev/null 2>&1 || die "missing required command '$1'"
}

prepend_go_bin_to_path() {
	local gobin

	gobin="$(go env GOBIN)"
	if [[ -z "$gobin" ]]; then
		gobin="$(go env GOPATH)/bin"
	fi

	case ":$PATH:" in
	*:"$gobin":*)
		;;
	*)
		export PATH="$gobin:$PATH"
		;;
	esac
}

ensure_goversioninfo() {
	local pkg

	pkg="${GOVERSIONINFO_PKG:-$GOVERSIONINFO_PKG_DEFAULT}"
	if command -v goversioninfo >/dev/null 2>&1; then
		return
	fi

	echo "Installing goversioninfo via go install..."
	go install "$pkg"
	prepend_go_bin_to_path
	require_cmd goversioninfo
}

cc_for_arch() {
	case "$1" in
	amd64)
		printf '%s\n' "zig cc -target x86_64-windows"
		;;
	386)
		printf '%s\n' "zig cc -target x86-windows"
		;;
	arm64)
		printf '%s\n' "zig cc -target aarch64-windows"
		;;
	arm)
		die "windows/arm is intentionally unsupported here because it currently fails with linker errors"
		;;
	*)
		die "unsupported Windows architecture '$1'"
		;;
	esac
}

main() {
	local script_dir repo_root tags arch_list
	local -a targets arches

	if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
		usage
		return
	fi

	script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
	repo_root="$(cd -- "$script_dir/.." && pwd)"
	cd "$repo_root"

	require_cmd go
	require_cmd zig
	prepend_go_bin_to_path
	ensure_goversioninfo

	tags="${TAGS:-$WINDOWS_TAGS_DEFAULT}"
	arch_list="${WINDOWS_ARCHES:-$WINDOWS_ARCHES_DEFAULT}"
	IFS=' ' read -r -a arches <<<"$arch_list"

	if [[ "$#" -gt 0 ]]; then
		targets=("$@")
	else
		targets=(syncthing stdiscosrv strelaysrv)
	fi

	export CGO_ENABLED=1

	echo "Packaging Windows zip archives"
	echo "Targets: ${targets[*]}"
	echo "Architectures: ${arches[*]}"
	echo "Tags: $tags"

	for target in "${targets[@]}"; do
		for arch in "${arches[@]}"; do
			echo
			echo "==> $target windows/$arch"
			go run build.go -tags "$tags" -goos windows -goarch "$arch" -cc "$(cc_for_arch "$arch")" zip "$target"
		done
	done
}

main "$@"
