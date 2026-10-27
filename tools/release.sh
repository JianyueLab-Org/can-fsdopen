#!/bin/sh
# Build the release and its Corresponding Source from an explicit public snapshot.
set -eu
umask 022

fail() {
    printf 'release.sh: %s\n' "$*" >&2
    exit 1
}

[ "$#" -le 1 ] || fail 'usage: tools/release.sh [new-output-directory]'
root=$(CDPATH= cd -P "$(dirname "$0")/.." && pwd)
export GOWORK=off GOTOOLCHAIN=local
# Do not emit macOS resource forks or extended-attribute sidecars in releases.
export COPYFILE_DISABLE=1
GOOS=${GOOS:-$(go env GOOS)}
GOARCH=${GOARCH:-$(go env GOARCH)}
CGO_ENABLED=${CGO_ENABLED:-0}
export GOOS GOARCH CGO_ENABLED
case "$GOOS" in ''|*[!a-z0-9_]*) fail 'GOOS must be a Go target operating-system name' ;; esac
case "$GOARCH" in ''|*[!a-z0-9_]*) fail 'GOARCH must be a Go target architecture name' ;; esac
case "$CGO_ENABLED" in 0|1) ;; *) fail 'CGO_ENABLED must be 0 or 1' ;; esac

output=${1-"$root/dist/can-fsdopen-$GOOS-$GOARCH"}
case "$output" in
    ''|*/|.|..) fail 'output must name a new directory, without a trailing slash' ;;
    /*) ;;
    *) output="$PWD/$output" ;;
esac
leaf=$(basename "$output")
case "$leaf" in .|..) fail 'output must name a new directory' ;; esac
parent=$(dirname "$output")
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output already exists: $output"
[ ! -e "$output.tar.gz" ] && [ ! -L "$output.tar.gz" ] || fail "archive already exists: $output.tar.gz"
mkdir -p "$parent" || fail "cannot create output parent: $parent"
parent=$(CDPATH= cd -P "$parent" && pwd)
output="$parent/$leaf"
archive="$output.tar.gz"

[ ! -L "$root/.temp" ] || fail 'project .temp must not be a symbolic link'
mkdir -p "$root/.temp"
work=$(mktemp -d "$root/.temp/release.XXXXXXXX")
trap 'rm -rf "$work"' 0
trap 'exit 1' HUP INT TERM
source="$work/can-fsdopen-source"
artifact="$work/artifact"
mkdir "$source" "$artifact"

# Never collect a working tree, credentials, runtime configuration, or local output.
for file in go.mod go.sum README.md config.example.json LICENSE THIRD_PARTY_NOTICES.md .gitignore .dockerignore Dockerfile tools/release.sh; do
    [ -f "$root/$file" ] && [ ! -L "$root/$file" ] || fail "missing or symlinked public input: $file"
    mkdir -p "$source/$(dirname "$file")"
    cp "$root/$file" "$source/$file"
done
for directory in auth config fsd cmd; do
    [ -d "$root/$directory" ] && [ ! -L "$root/$directory" ] || fail "missing or symlinked source directory: $directory"
    (
        cd "$root"
        find "$directory" -type d \( -name '.*' -o -name bin -o -name dist -o -name certs -o -name vendor \) -prune -o -type f -name '*.go' -exec sh -c '
            destination=$1
            shift
            for file do
                mkdir -p "$destination/$(dirname "$file")" || exit 1
                cp "$file" "$destination/$file" || exit 1
            done
        ' sh "$source" {} +
    )
done
chmod +x "$source/tools/release.sh"

# Only the isolated snapshot receives generated vendor files or module changes.
(
    cd "$source"
    go mod vendor
)
binary=fsdopen
if [ "$GOOS" = windows ]; then
    binary=fsdopen.exe
fi
compiler=$(go version)
cat > "$source/SOURCE.txt" <<EOF
can-fsdopen Corresponding Source
License: AGPL-3.0-only. See LICENSE and THIRD_PARTY_NOTICES.md.

The accompanying source.tar.gz is the primary source distribution for this
binary. The Docker image carries the same archive at /app/source.tar.gz and
these instructions at /app/SOURCE.txt. Copy both files out of the image to
access its source; no remote repository or source download URL is required.

Extract source.tar.gz and enter can-fsdopen-source/. It contains the project
Go sources, tests, build script, Dockerfile, example configuration, notices,
and pinned dependencies in vendor/ with their upstream license files.

Build toolchain used: $compiler
Target: GOOS=$GOOS GOARCH=$GOARCH CGO_ENABLED=$CGO_ENABLED
Install Go 1.27.3 or a later patched Go compiler separately (https://go.dev/dl/).
The Go compiler and standard library are system toolchain components, not
bundled compiler binaries. The linked runtime/standard-library license is
reproduced in THIRD_PARTY_NOTICES.md. CGO_ENABLED=1 additionally requires a
C compiler and any target system libraries used by the Go toolchain.

Rebuild offline from the extracted source with the installed toolchain:
  GOTOOLCHAIN=local GOWORK=off GOPROXY=off GOSUMDB=off GOOS=$GOOS GOARCH=$GOARCH CGO_ENABLED=$CGO_ENABLED go build -mod=vendor -trimpath -buildvcs=false -o $binary ./cmd/fsdopen

The release script builds from this snapshot with -mod=vendor -trimpath.
It regenerates vendor in a separate snapshot when creating a new release;
that packaging step needs the pinned modules in the Go cache or network access.
The offline rebuild command above does not need the module cache or network.
EOF
(
    cd "$source"
    GOPROXY=off GOSUMDB=off go build -mod=vendor -trimpath -buildvcs=false -o "$artifact/$binary" ./cmd/fsdopen
)
cp "$source/LICENSE" "$source/THIRD_PARTY_NOTICES.md" "$source/SOURCE.txt" "$artifact/"
tar -czf "$artifact/source.tar.gz" -C "$work" can-fsdopen-source

# Claim a fresh directory; never remove or replace a caller-owned path.
mkdir "$output" || fail "cannot create new output directory: $output"
cp -R "$artifact/." "$output/"
# Noclobber also rejects an archive created while the release was building.
(
    set -C
    tar -czf - -C "$parent" "./$leaf" > "$archive"
) || fail "cannot create new archive: $archive (artifact directory retained)"
printf 'Artifact directory: %s\nRelease archive: %s\n' "$output" "$archive"
