#!/bin/sh

fetch_repo() (
	dir="$1"
	url="$2"
	cd "$dir"
	dir="$(pwd)"

	tmpdir="$(mktemp -d)"
	cd "$tmpdir"
	wget "$url" -O "tmp"
	tar xf tmp
	tar -zcvf x86_64-stagedata index.plist index-meta.plist
	mv x86_64-stagedata "$dir"
	rm -r "$tmpdir"
)

set -x

fetch_repo repos/x86_64-repodata https://alpha.de.repo.voidlinux.org/current/x86_64-repodata
fetch_repo repos/debug_x86_64-repodata https://alpha.de.repo.voidlinux.org/current/debug/x86_64-repodata
fetch_repo repos/multilib_x86_64-repodata https://alpha.de.repo.voidlinux.org/current/multilib/x86_64-repodata
fetch_repo repos/nonfree_x86_64-repodata https://alpha.de.repo.voidlinux.org/current/nonfree/x86_64-repodata
fetch_repo repos/multilib_nonfree_x86_64-repodata https://alpha.de.repo.voidlinux.org/current/multilib/nonfree/x86_64-repodata
