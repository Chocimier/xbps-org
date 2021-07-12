#!/usr/bin/sh

DESTDIR="${DESTDIR:-/}"

cd $(dirname ${0})
rm -r repos
tar xf repos-fromempty.tar.xz
xbps-query -i $(printf '\055-repository=%s ' repos/* ) -p pkgver -Rs '' | cut -d: -f1 | sort --random-sort | head -n 500 | sed 's/$/.x86_64.xbps/' > removed
for i in repos/*x86_64-repodata; do
	(cd $i &&
		mv x86_64-stagedata stagedata
		cat ../../removed | xargs -n1 env LD_LIBRARY_PATH="${DESTDIR}/usr/local/lib" "${DESTDIR}/usr/local/bin/xbps-rindex" -R
		mv stagedata x86_64-stagedata
	)
done
date
LD_LIBRARY_PATH="${DESTDIR}/usr/local/lib" "${DESTDIR}/usr/local/bin/xbps-repodb" -i repos/*
date
