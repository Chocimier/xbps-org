#!/usr/bin/sh

DESTDIR="${DESTDIR:-/}"

cd "$(dirname ${0})"
rm -r repos
./fetch_to_stage.sh # writes stagedata
tar xf repos-2020-12-27.tar.xz # writes plist
for i in repos/*x86_64-repodata; do
	(cd $i &&
		pwd
		mv x86_64-stagedata x86_64-repodata
		LD_LIBRARY_PATH="${DESTDIR}/usr/local/lib" "${DESTDIR}/usr/local/bin/xbps-checkvers" -D $(xdistdir) -e --format='%n-%r.x86_64.xbps' -i --repository=$PWD |
			xargs env LD_LIBRARY_PATH="${DESTDIR}/usr/local/lib" "${DESTDIR}/usr/local/bin/xbps-rindex" -R
		cat ../../outdated |
			xargs -n1 env LD_LIBRARY_PATH="${DESTDIR}/usr/local/lib" "${DESTDIR}/usr/local/bin/xbps-rindex" -R
		mv x86_64-repodata x86_64-stagedata
		tar -zcf x86_64-repodata index.plist index-meta.plist
	)
done
LD_LIBRARY_PATH="${DESTDIR}/usr/local/lib" "${DESTDIR}/usr/local/bin/xbps-repodb" -i -d repos/*x86_64-repodata

