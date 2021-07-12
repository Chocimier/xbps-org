#!/bin/sh

MIRROR=${MIRROR:-https://alpha.de.repo.voidlinux.org/current}

fetch_repo() (
	dir_rel="$1"
	url="$2"
	mkdir -p "$dir_rel"
	cd "$dir_rel"
	dir="$(pwd)"

	tmpdir="$(mktemp -d)"
	cd "$tmpdir"
	wget "$url" -O "tmp"
	tar xf tmp
	tar -zcf x86_64-stagedata index.plist index-meta.plist
	mv x86_64-stagedata "$dir"
	if [ "$REPO_CACHE_DIR" ]; then
		mkdir -p $REPO_CACHE_DIR/$(dirname $url | sed s,$MIRROR,,)
		cp -v tmp $REPO_CACHE_DIR/$(echo $url | sed s,$MIRROR,,)
	fi
	rm -r "$tmpdir"
)

fetch_repo repos/x86_64-repodata $MIRROR/x86_64-repodata
fetch_repo repos/debug_x86_64-repodata $MIRROR/debug/x86_64-repodata
fetch_repo repos/multilib_x86_64-repodata $MIRROR/multilib/x86_64-repodata
fetch_repo repos/nonfree_x86_64-repodata $MIRROR/nonfree/x86_64-repodata
fetch_repo repos/multilib_nonfree_x86_64-repodata $MIRROR/multilib/nonfree/x86_64-repodata
