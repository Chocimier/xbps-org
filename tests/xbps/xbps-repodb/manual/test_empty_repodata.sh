#!/usr/bin/sh

DESTDIR="${DESTDIR:-/}"

cd $(dirname ${0})
rm -r repos
./fetch_to_stage.sh
LD_LIBRARY_PATH="${DESTDIR}/usr/local/lib" "${DESTDIR}/usr/local/bin/xbps-repodb" -i -d repos/*

