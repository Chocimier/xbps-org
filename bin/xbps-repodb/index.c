#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <xbps.h>
#include "defs.h"
#include "uthash.h"

`

struct hash_str_holder_t {
	char *str;
	UT_hash_handle hh;
};

struct repolock_t {
	int fd;
	char *name;
};

struct package_t {
	const char *pkgver;
	xbps_array_t revdeps;
	xbps_dictionary_t dict;
	int repo;
};

struct node_t {
	char *pkgname;
	struct package_t assured;
	struct package_t proposed;
	UT_hash_handle hh;
};

struct repos_state_t {
	struct node_t *nodes;
	xbps_dictionary_t shlib_providers;
	xbps_dictionary_t shlib_users;
	/**
	 * key is virtual pkgname, value is dictionary,
	 * where key is pkgname of real package, value is pkgver of virtual it provides
	 */
	xbps_dictionary_t virtual_providers;
	/**
	 * key is virtual pkgname, value is dictionary,
	 * where key is pkgname of real package, value is pkgpattern of virtual it depends on
	 */
	xbps_dictionary_t virtual_users;
	int repos_count;
	struct xbps_repo **repos;
	struct xbps_repo **stages;
	struct xbps_handle *xhp;
};

enum source {
	SOURCE_REPODATA = 0,
	SOURCE_STAGEDATA
};

static xbps_array_t
get_possibly_new_array(xbps_dictionary_t dict, const char *key) {
	xbps_array_t array = xbps_dictionary_get(dict, key);
	if (!array) {
		array = xbps_array_create();
		if (array) {
			xbps_dictionary_set_and_rel(dict, key, array);
		}
	}
	return array;
}

static xbps_dictionary_t
get_possibly_new_dictionary(xbps_dictionary_t dict, const char *key) {
	xbps_dictionary_t member = xbps_dictionary_get(dict, key);
	if (!member) {
		member = xbps_dictionary_create();
		if (member) {
			xbps_dictionary_set_and_rel(dict, key, member);
		}
	}
	return member;
}

static struct hash_str_holder_t *owned_strings_container = NULL;

static char *
owned_string(const char *content) {
	struct hash_str_holder_t *holder = NULL;
	size_t len = strlen(content);

	HASH_FIND(hh, owned_strings_container, content, len, holder);
	if (!holder) {
		holder = calloc(1, sizeof *holder);
		holder->str = strdup(content);
		HASH_ADD_KEYPTR(hh, owned_strings_container, holder->str, len, holder);
	}
	return holder->str;
}

static void
free_owned_strings(void) {
	struct hash_str_holder_t *holder = NULL;
	struct hash_str_holder_t *tmp = NULL;

	HASH_ITER(hh, owned_strings_container, holder, tmp) {
		HASH_DEL(owned_strings_container, holder);
		free(holder->str);
		free(holder);
	}
}

static struct package_t *
top_package(struct node_t *node) {
	if (node->proposed.pkgver) {
		return &node->proposed;
	}
	return &node->assured;
}

static void
package_init(struct package_t *package, xbps_dictionary_t pkg, int repo_serial) {
	xbps_dictionary_get_cstring_nocopy(pkg, "pkgver", &package->pkgver);
	package->revdeps = xbps_array_create();
	package->repo = repo_serial;
	xbps_object_retain(pkg);
	package->dict = pkg;
}

static struct package_t*
package_copy(struct package_t *source, struct package_t *dest) {
	package_init(dest, source->dict, source->repo);
	xbps_object_release(dest->revdeps);
	dest->revdeps = xbps_array_copy(source->revdeps);
	return dest;
}

static void
package_release(struct package_t *package) {
	if (!package) {
		return;
	}
	if (package->revdeps) {
		xbps_object_release(package->revdeps);
	}
	if (package->dict) {
		xbps_object_release(package->dict);
	}
}

static void
repo_state_purge_graph(struct repos_state_t *graph) {
	struct node_t *current_node = NULL;
	struct node_t *tmp_node = NULL;

	HASH_ITER(hh, graph->nodes,current_node, tmp_node) {
		HASH_DEL(graph->nodes, current_node);
		package_release(&current_node->assured);
		package_release(&current_node->proposed);
		free(current_node);
	}
	xbps_object_release(graph->shlib_providers);
	xbps_object_release(graph->shlib_users);
	xbps_object_release(graph->virtual_providers);
	xbps_object_release(graph->virtual_users);
	graph->shlib_providers = xbps_dictionary_create();
	graph->shlib_users = xbps_dictionary_create();
	graph->virtual_providers = xbps_dictionary_create();
	graph->virtual_users = xbps_dictionary_create();
}

static void
repo_state_init(struct repos_state_t *graph, struct xbps_handle *xhp, int repos_count) {
	graph->shlib_providers = xbps_dictionary_create();
	graph->shlib_users = xbps_dictionary_create();
	graph->virtual_providers = xbps_dictionary_create();
	graph->virtual_users = xbps_dictionary_create();
	graph->repos_count = repos_count;
	graph->repos = calloc(graph->repos_count, sizeof *graph->repos);
	graph->stages = calloc(graph->repos_count, sizeof *graph->stages);
	graph->xhp = xhp;
}

static void
repo_state_release(struct repos_state_t *graph) {
	repo_state_purge_graph(graph);
	xbps_object_release(graph->shlib_providers);
	xbps_object_release(graph->shlib_users);
	xbps_object_release(graph->virtual_providers);
	xbps_object_release(graph->virtual_users);
	for(int i = 0; i < graph->repos_count; ++i) {
		if (graph->repos[i]) {
			xbps_repo_release(graph->repos[i]);
		}
		if (graph->stages[i]) {
			xbps_repo_release(graph->stages[i]);
		}
	}
	free(graph->repos);
	free(graph->stages);
}

static int
repo_is_package_missing(struct repos_state_t *graph, const char *deppattern) {
	char depname[XBPS_NAME_SIZE];
	struct node_t *depnode = NULL;
	bool has_name = false;

	has_name = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
	if (!has_name) {
		has_name = xbps_pkg_name(depname, sizeof depname, deppattern);
	}
	if (!has_name) {
		return ENXIO;
	}
	HASH_FIND(hh, graph->nodes, depname, strlen(depname), depnode);
	if (depnode) {
		if (xbps_pkgpattern_match(top_package(depnode)->pkgver, deppattern)) {
			return ERANGE;
		}
	} else {
		xbps_dictionary_t virtual_versions = NULL;
		bool satisfied = false;
		xbps_object_iterator_t iter = NULL;
		xbps_object_t keysym = NULL;

		virtual_versions = xbps_dictionary_get(graph->virtual_providers, depname);
		if (!virtual_versions) {
			return ENOENT;
		}
		iter = xbps_dictionary_iterator(virtual_versions);
		while ((keysym = xbps_object_iterator_next(iter))) {
			const char *provider = xbps_dictionary_keysym_cstring_nocopy(keysym);
			const char *virtual_version = NULL;
			xbps_dictionary_get_cstring_nocopy(virtual_versions, provider, &virtual_version);
			if (xbps_pkgpattern_match(virtual_version, deppattern)) {
				satisfied = true;
				break;
			}
		}
		xbps_object_iterator_release(iter);
		if (!satisfied) {
			return ENOMSG;
		}
	}
	return 0;
}

static int
verify_graph(struct repos_state_t *graph) {
	int rv = 0;
	struct node_t *curr_node;

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		xbps_array_t shlib_requires = xbps_dictionary_get(curr_node->proposed.dict, "shlib-requires");
		for (unsigned int i = 0; i < xbps_array_count(shlib_requires); i++) {
			const char *shlib = NULL;
			xbps_array_get_cstring_nocopy(shlib_requires, i, &shlib);
			if (!xbps_dictionary_get(graph->shlib_providers, shlib)) {
				fprintf(stderr, "'%s' requires unavailable shlib '%s'\n", curr_node->proposed.pkgver, shlib);
				rv = ENOEXEC;
			}
		}
	}

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		xbps_array_t deps = xbps_dictionary_get(curr_node->proposed.dict, "run_depends");
		for (unsigned int i = 0; i < xbps_array_count(deps); i++) {
			int missing = 0;
			const char *deppattern = NULL;
			xbps_array_get_cstring_nocopy(deps, i, &deppattern);
			missing = repo_is_package_missing(graph, deppattern);
			if (missing) {
				rv = missing;
				switch (missing) {
					case ENXIO:
						fprintf(stderr, "'%s' requires '%s' that doesn't contain package name\n", curr_node->pkgname, deppattern);
						break;
					case ERANGE:
						fprintf(stderr, "'%s' requires package '%s', but other version is present\n", curr_node->proposed.pkgver, deppattern);
						break;
					case ENOMSG:
						fprintf(stderr, "'%s' requires virtual package in unavailable version '%s'\n", curr_node->pkgname, deppattern);
						break;
					default:
						fprintf(stderr, "'%s' requires unavailable package or virtual package '%s'\n", curr_node->pkgname, deppattern);
						break;
				}
			}
		}
	}

	return rv;
}

static int
load_repo(struct repos_state_t *graph, struct xbps_repo *current_repo, enum source source, int repo_serial) {
	xbps_object_iterator_t iter = NULL;
	xbps_object_t keysym = NULL;
	struct xbps_repo **repos_array = (source == SOURCE_STAGEDATA) ? graph->stages : graph->repos;

	xbps_dbg_printf(graph->xhp, "loading repo '%s'\n", current_repo->uri);
	iter = xbps_dictionary_iterator(current_repo->idx);
	while ((keysym = xbps_object_iterator_next(iter))) {
		xbps_dictionary_t pkg;
		struct node_t *new_node;
		struct node_t *existing_node = NULL;

		pkg = xbps_dictionary_get_keysym(current_repo->idx, keysym);
		new_node = calloc(1, sizeof *new_node);
		new_node->pkgname = owned_string(xbps_dictionary_keysym_cstring_nocopy(keysym));
		package_init(&new_node->proposed, pkg, repo_serial);

		HASH_FIND(hh, graph->nodes, new_node->pkgname, strlen(new_node->pkgname), existing_node);

		if (existing_node) {
			//TODO: reverts, look at rindex' index_add
			if (xbps_cmpver(existing_node->proposed.pkgver, new_node->proposed.pkgver) >= 0) {
				fprintf(stderr, "'%s' from '%s' is about to push out '%s' from '%s'\n",
				    existing_node->proposed.pkgver, repos_array[existing_node->proposed.repo]->uri,
				    new_node->proposed.pkgver, repos_array[new_node->proposed.repo]->uri);
				package_release(&new_node->proposed);
				free(new_node);
				continue;
			}
			fprintf(stderr, "'%s' from '%s' is about to push out '%s' from '%s'\n",
			    existing_node->proposed.pkgver, repos_array[existing_node->proposed.repo]->uri,
			    new_node->proposed.pkgver, repos_array[new_node->proposed.repo]->uri);
			HASH_DEL(graph->nodes, existing_node);
			package_release(&existing_node->proposed);
			free(existing_node);
		}

		HASH_ADD_KEYPTR(hh, graph->nodes, new_node->pkgname, strlen(new_node->pkgname), new_node);

	}
	xbps_object_iterator_release(iter);
	return 0;
}

static int
build_graph(struct repos_state_t *graph, enum source source, bool verify) {
	int rv = 0;
	struct node_t *curr_node;

	for (int i = 0; i < graph->repos_count; ++i) {
		struct xbps_repo *repo = (source == SOURCE_STAGEDATA) ? graph->stages[i] : graph->repos[i];
		fprintf(stderr, "loading repo %d %p '%s', source %x\n", i, repo, (repo ? repo->uri : NULL), source);
		if (!repo) {
			continue;
		}
		rv = load_repo(graph, repo, source, i);
		if (rv) {
			fprintf(stderr, "can't load '%s' repo into graph, exiting\n", repo->uri);
			goto exit;
		}
	}

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		xbps_array_t shlib_provides = xbps_dictionary_get(curr_node->proposed.dict, "shlib-provides");
		xbps_array_t shlib_requires = xbps_dictionary_get(curr_node->proposed.dict, "shlib-requires");
		xbps_array_t provides = xbps_dictionary_get(curr_node->proposed.dict, "provides");

		for (unsigned int i = 0; i < xbps_array_count(shlib_provides); i++) {
			const char *shlib = NULL;
			xbps_array_t providers;
			xbps_array_get_cstring_nocopy(shlib_provides, i, &shlib);
			providers = get_possibly_new_array(graph->shlib_providers, shlib);
			if (!providers) {
				return ENOMEM;
			}
			xbps_array_add_cstring_nocopy(providers, curr_node->pkgname);
		}

		for (unsigned int i = 0; i < xbps_array_count(shlib_requires); i++) {
			const char *shlib = NULL;
			xbps_array_t users;
			xbps_array_get_cstring_nocopy(shlib_requires, i, &shlib);
			users = get_possibly_new_array(graph->shlib_users, shlib);
			if (!users) {
				return ENOMEM;
			}
			xbps_array_add_cstring_nocopy(users, curr_node->pkgname);
		}

		for (unsigned int i = 0; i < xbps_array_count(provides); i++) {
			const char *virtual = NULL;
			xbps_dictionary_t providers;
			char virtual_pkgname[XBPS_NAME_SIZE] = {0};
			bool ok;
			xbps_array_get_cstring_nocopy(provides, i, &virtual);
			ok = xbps_pkg_name(virtual_pkgname, sizeof virtual_pkgname, virtual);
			if (ok) {
				xbps_dbg_printf(graph->xhp, "virtual '%s' (%s) provided by '%s'\n", virtual_pkgname, virtual, curr_node->pkgname);
			} else {
				xbps_dbg_printf(graph->xhp, "invalid virtual pkgver '%s' provided by package '%s', ignoring\n", virtual, curr_node->pkgname);
				continue;
			}
			providers = get_possibly_new_dictionary(graph->virtual_providers, owned_string(virtual_pkgname));
			if (!providers) {
				return ENOMEM;
			}
			xbps_dictionary_set_cstring_nocopy(providers, curr_node->pkgname, virtual);
		}
	}

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		xbps_array_t deps = xbps_dictionary_get(curr_node->proposed.dict, "run_depends");
		for (unsigned int i = 0; i < xbps_array_count(deps); i++) {
			const char *deppattern = NULL;
			struct node_t *depnode = NULL;
			xbps_dictionary_t virtual_providers = NULL;
			char depname[XBPS_NAME_SIZE] = {0};
			bool ok;
			xbps_array_get_cstring_nocopy(deps, i, &deppattern);
			ok = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
			if (!ok) {
				ok = xbps_pkg_name(depname, sizeof depname, deppattern);
			}
			if (!ok) {
				fprintf(stderr, "'%s' requires '%s' that has no package name\n", curr_node->proposed.pkgver, deppattern);
				//rv = ENXIO;
				continue;
			}
			HASH_FIND(hh, graph->nodes, depname, strlen(depname), depnode);
			if (depnode) {
				xbps_array_add_cstring_nocopy(depnode->proposed.revdeps, curr_node->pkgname);
				continue;
			}
			virtual_providers = xbps_dictionary_get(graph->virtual_providers, depname);
			if (virtual_providers) {
				xbps_dictionary_set_cstring_nocopy(get_possibly_new_dictionary(graph->virtual_users, depname), curr_node->pkgname, deppattern);
				continue;
			}
			xbps_dbg_printf(graph->xhp, "package '%s' depends on unreachable '%s' (%s)\n", curr_node->pkgname, depname, deppattern);
			// rv = ENOENT;
		}
	}

	if (rv) {
		goto exit;
	}

	if (verify) {
		rv = verify_graph(graph);
	}
exit:
	if (rv) {
		fprintf(stderr, "graph from source %x failed to build\n", source);
		repo_state_purge_graph(graph);
	} else if (verify) {
		for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
			curr_node->assured = curr_node->proposed;
			memset(&curr_node->proposed, 0, sizeof curr_node->proposed);
		}
	}
	return rv;
}

static xbps_array_t
changes_in_stage(struct repos_state_t *repodata, struct repos_state_t *stage) {
	xbps_array_t changes = xbps_array_create();
	// new and updated
	for (struct node_t* stage_node = stage->nodes; stage_node; stage_node = stage_node->hh.next) {
		struct node_t *graph_node = NULL;
		HASH_FIND(hh, repodata->nodes, stage_node->pkgname, strlen(stage_node->pkgname), graph_node);
		if (!graph_node) {
			xbps_array_add_cstring_nocopy(changes, stage_node->pkgname);
			xbps_dbg_printf(repodata->xhp, "new '%s'\n", stage_node->proposed.pkgver);
		} else if (strcmp(graph_node->assured.pkgver, stage_node->proposed.pkgver)) { // TODO: check if newer
			xbps_array_add_cstring_nocopy(changes, stage_node->pkgname);
			xbps_dbg_printf(repodata->xhp, "updated '%s' -> '%s'\n", graph_node->assured.pkgver, stage_node->proposed.pkgver);
		}
	}
	// removed
	for (struct node_t* graph_node = repodata->nodes; graph_node; graph_node = graph_node->hh.next) {
		struct node_t *stage_node = NULL;
		HASH_FIND(hh, stage->nodes, graph_node->pkgname, strlen(graph_node->pkgname), stage_node);
		if (!stage_node) {
			xbps_array_add_cstring_nocopy(changes, graph_node->pkgname);
			xbps_dbg_printf(repodata->xhp, "removed '%s'\n", graph_node->assured.pkgver);
		}
	}
	return changes;
}

static int
remove_from_graph(const char *pkgname, struct repos_state_t *graph) {
	struct node_t *curr_node;
	xbps_array_t shlib_provides = NULL;
	xbps_array_t shlib_requires = NULL;
	xbps_array_t provides = NULL;
	bool found = false;

	HASH_FIND(hh, graph->nodes, pkgname, strlen(pkgname), curr_node);

	if (!curr_node) {
		xbps_dbg_printf(graph->xhp, "Can't remove '%s', it's not here\n", pkgname);
		return EFAULT;
	}

	shlib_provides = xbps_dictionary_get(curr_node->assured.dict, "shlib-provides");
	shlib_requires = xbps_dictionary_get(curr_node->assured.dict, "shlib-requires");
	provides = xbps_dictionary_get(curr_node->assured.dict, "provides");

	for (unsigned int i = 0; i < xbps_array_count(shlib_provides); i++) {
		const char *shlib = NULL;
		xbps_array_t providers;
		xbps_array_get_cstring_nocopy(shlib_provides, i, &shlib);
		providers = get_possibly_new_array(graph->shlib_providers, shlib);
		if (!providers) {
			//TODO
			xbps_dbg_printf(graph->xhp, "Can't find '%s' of pkgname, it's not here\n", pkgname);
			return EFAULT;
		}
		do {
			found = xbps_remove_string_from_array(providers, pkgname);
		} while (found);
	}

	for (unsigned int i = 0; i < xbps_array_count(shlib_requires); i++) {
		const char *shlib = NULL;
		xbps_array_t users;
		xbps_array_get_cstring_nocopy(shlib_requires, i, &shlib);
		users = get_possibly_new_array(graph->shlib_users, shlib);
		if (!users) {
			//TODO
			xbps_dbg_printf(graph->xhp, "Can't find '%s' of pkgname, it's not here\n", pkgname);
			return EFAULT;
		}
		do {
			found = xbps_remove_string_from_array(users, pkgname);
		} while (found);
	}

	for (unsigned int i = 0; i < xbps_array_count(provides); i++) {
		const char *virtual = NULL;
		xbps_dictionary_t providers;
		char virtual_pkgname[XBPS_NAME_SIZE] = {0};
		bool ok;
		xbps_array_get_cstring_nocopy(provides, i, &virtual);
		ok = xbps_pkg_name(virtual_pkgname, sizeof virtual_pkgname, virtual);
		if(!ok) {
			xbps_dbg_printf(graph->xhp, "invalid virtual pkgver '%s' provided by package '%s', ignoring\n", virtual, curr_node->pkgname);
			continue;
		}
		providers = xbps_dictionary_get(graph->virtual_providers, virtual_pkgname);
		if (!providers) {
			return EFAULT;
		}
		xbps_dictionary_set_cstring_nocopy(providers, curr_node->pkgname, virtual);
	}

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		xbps_array_t deps = xbps_dictionary_get(curr_node->proposed.dict, "run_depends");
		for (unsigned int i = 0; i < xbps_array_count(deps); i++) {
			const char *deppattern = NULL;
			struct node_t *depnode = NULL;
			xbps_dictionary_t virtual_providers = NULL;
			char depname[XBPS_NAME_SIZE] = {0};
			bool ok;
			xbps_array_get_cstring_nocopy(deps, i, &deppattern);
			ok = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
			if (!ok) {
				ok = xbps_pkg_name(depname, sizeof depname, deppattern);
			}
			if (!ok) {
				fprintf(stderr, "'%s' requires '%s' that has no package name\n", curr_node->proposed.pkgver, deppattern);
				//rv = ENXIO;
				continue;
			}
			HASH_FIND(hh, graph->nodes, depname, strlen(depname), depnode);
			if (depnode) {
				xbps_array_add_cstring_nocopy(depnode->proposed.revdeps, curr_node->pkgname);
				continue;
			}
			virtual_providers = xbps_dictionary_get(graph->virtual_providers, depname);
			if (virtual_providers) {
				xbps_dictionary_set_cstring_nocopy(get_possibly_new_dictionary(graph->virtual_users, depname), curr_node->pkgname, deppattern);
				continue;
			}
			xbps_dbg_printf(graph->xhp, "package '%s' depends on unreachable '%s' (%s)\n", curr_node->pkgname, depname, deppattern);
			// rv = ENOENT;
		}
	}

	if (rv) {
		goto exit;
	}

	if (verify) {
		rv = verify_graph(graph);
	}
exit:
	if (rv) {
		fprintf(stderr, "graph from source %x failed to build\n", source);
		repo_state_purge_graph(graph);
	} else if (verify) {
		for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
			curr_node->assured = curr_node->proposed;
			memset(&curr_node->proposed, 0, sizeof curr_node->proposed);
		}
	}
	return rv;

	return -1;
}

static int
propose_package(const char *pkgname, struct repos_state_t *graph, struct repos_state_t *stage, xbps_dictionary_t queue) {
	struct node_t *repodata_node = NULL;
	struct node_t *stage_node = NULL;
	xbps_array_t repodata_shlib_provides = NULL;
	xbps_array_t repodata_provides = NULL;
	xbps_array_t repodata_revdeps = NULL;
	xbps_array_t stage_shlib_provides = NULL;
	xbps_array_t stage_shlib_requires = NULL;
	xbps_array_t stage_provides = NULL;
	xbps_array_t stage_depends = NULL;

	(void) queue;

	HASH_FIND(hh, graph->nodes, pkgname, strlen(pkgname), repodata_node);
	HASH_FIND(hh, stage->nodes, pkgname, strlen(pkgname), stage_node);

	if (repodata_node) {
		repodata_shlib_provides = xbps_dictionary_get(repodata_node->assured.dict, "shlib-provides");
		repodata_provides = xbps_dictionary_get(repodata_node->assured.dict, "provides");
		repodata_revdeps = repodata_node->assured.revdeps;
	}
	if (stage_node) {
		stage_shlib_provides = xbps_dictionary_get(stage_node->proposed.dict, "shlib-provides");
		stage_shlib_requires = xbps_dictionary_get(stage_node->proposed.dict, "shlib-requires");
		stage_provides = xbps_dictionary_get(stage_node->proposed.dict, "provides");
		stage_depends = xbps_dictionary_get(stage_node->proposed.dict, "run_depends");
	}

	if (stage_node && repodata_node) {
		// Does new version provide solibs that old provided?
		for (unsigned int i = 0; i < xbps_array_count(repodata_shlib_provides); i++) {
			const char *shlib = NULL;
			xbps_array_get_cstring_nocopy(repodata_shlib_provides, i, &shlib);
			if (!stage_shlib_provides || !xbps_match_string_in_array(stage_shlib_provides, shlib)) {
				return ENOTSUP;
			}
		}
		// Are solibs required by new versions available?
		for (unsigned int i = 0; i < xbps_array_count(stage_shlib_requires); i++) {
			const char *shlib = NULL;
			xbps_array_t providers = NULL;
			xbps_array_get_cstring_nocopy(stage_shlib_requires, i, &shlib);
			providers = xbps_dictionary_get(graph->shlib_providers, shlib);
			if (!xbps_array_count(providers)) {
				return ENOTSUP;
			}
		}
		// Does new version satisfy dependencies of revdeps of old version?
		for (unsigned int i = 0; i < xbps_array_count(repodata_revdeps); i++) {
			const char *revdep = NULL;
			struct node_t *revdep_node = NULL;
			xbps_array_t revdep_deps = NULL;
			xbps_array_get_cstring_nocopy(repodata_revdeps, i, &revdep);
			HASH_FIND(hh, graph->nodes, revdep, strlen(revdep), revdep_node);
			if (!revdep_node) {
				return EFAULT;
			}
			revdep_deps = xbps_dictionary_get(top_package(revdep_node)->dict, "run_depends");
			if (!revdep_deps) {
				return EFAULT;
			}
			if (!xbps_match_pkgdep_in_array(revdep_deps, stage_node->proposed.pkgver)) {
				return ENOTSUP;
			}
		}
		// Are dependencies of new versions available?
		for (unsigned int i = 0; i < xbps_array_count(stage_depends); i++) {
			const char *deppattern = NULL;
			xbps_array_get_cstring_nocopy(stage_depends, i, &deppattern);
			if (repo_is_package_missing(graph, deppattern)) {
				return ENOTSUP;
			}
		}
		// Does new version has all `provides` that old have?
		for (unsigned int i = 0; i < xbps_array_count(repodata_provides); i++) {
			const char *provides = NULL;
			xbps_array_get_cstring_nocopy(repodata_provides, i, &provides);
			if (!xbps_match_string_in_array(stage_provides, provides)) {
				return ENOTSUP;
			}
		}
		package_release(&repodata_node->proposed);
		package_copy(&stage_node->proposed, &repodata_node->assured);
	} else if (repodata_node) {
		// Are provided solibs used?
		for (unsigned int i = 0; i < xbps_array_count(repodata_shlib_provides); i++) {
			const char *shlib = NULL;
			xbps_array_t users = NULL;
			xbps_array_t providers = NULL;
			xbps_array_get_cstring_nocopy(repodata_shlib_provides, i, &shlib);
			users = xbps_dictionary_get(graph->shlib_users, shlib);
			providers = xbps_dictionary_get(graph->shlib_providers, shlib);
			if (xbps_array_count(users) > 0 && xbps_array_count(providers) < 2) {
				return ENOTSUP;
			}
		}
		// Is package a dependency?
		if (xbps_array_count(repodata_revdeps)) {
			return ENOTSUP;
		}
		// Are all used `provides` backed by other packages?
		for (unsigned int i = 0; i < xbps_array_count(repodata_provides); i++) {
			const char *virtual = NULL;
			char virtual_pkgname[XBPS_NAME_SIZE] = {0};
			xbps_dictionary_t users = NULL;
			xbps_object_iterator_t users_iter = NULL;
			xbps_dictionary_t providers = NULL;
			xbps_dictionary_keysym_t keysym = NULL;
			bool ok = false;
			bool backed = true;

			xbps_array_get_cstring_nocopy(repodata_provides, i, &virtual);
			ok = xbps_pkg_name(virtual_pkgname, sizeof virtual_pkgname, virtual);
			if (!ok) {
				return EFAULT;
			}
			providers = xbps_dictionary_get(graph->virtual_providers, virtual_pkgname);
			users = xbps_dictionary_get(graph->virtual_users, virtual_pkgname);
			users_iter = xbps_dictionary_iterator(users);
			while ((keysym = xbps_object_iterator_next(users_iter))) {
				xbps_object_iterator_t providers_iter = NULL;
				const char *user = xbps_dictionary_keysym_cstring_nocopy(keysym);
				const char *req_pattern = xbps_dictionary_get_keysym(users, keysym);

				providers_iter = xbps_dictionary_iterator(providers);
				backed = false;
				while ((keysym = xbps_object_iterator_next(providers_iter))) {
					const char *provider_pkgname = xbps_dictionary_keysym_cstring_nocopy(keysym);
					const char *provided_pkgver = xbps_dictionary_get_keysym(providers, keysym);

					if (strcmp(provider_pkgname, pkgname) == 0) {
						continue;
					}
					if (xbps_pkgpattern_match(provided_pkgver, req_pattern)) {
						backed = true;
						break;
					}
				}
				xbps_object_iterator_release(providers_iter);
				if (!backed) {
					xbps_dbg_printf(graph->xhp,
						"Package '%s' cannot be removed, as it is the only provider of '%s', used by '%s'.\n",
						pkgname, req_pattern, user);
					break;
				}
			}
			xbps_object_iterator_release(users_iter);
			if (!backed) {
				return ENOTSUP;
			}
		}
		remove_from_graph(pkgname, graph);
	} else {
		return ENOTSUP;
	}
	return 0;
}

static int
revert_to_assured(struct repos_state_t *graph, struct repos_state_t *stage, xbps_dictionary_t proposed_set) {
	(void) graph;
	(void) stage;
	(void) proposed_set;

	return -1;
}

static int
hide_from_stage(const char *pkgname, struct repos_state_t *graph, struct repos_state_t *stage) {
	(void) pkgname;
	(void) graph;
	(void) stage;

	return -1;
}

static int
update_repodata_from_stage(struct repos_state_t *graph) {
	struct repos_state_t *stage = calloc(1, sizeof *stage);
	int rv = 0;
	xbps_array_t changes = NULL;

	repo_state_init(stage, graph->xhp, graph->repos_count);
	memcpy(stage->stages, graph->stages, graph->repos_count * sizeof *stage->stages);
	rv = build_graph(stage, SOURCE_STAGEDATA, false);
	if (rv) {
		fprintf(stderr, "cannot load stage\n");
		goto exit;
	}
	rv = EALREADY;
	changes = changes_in_stage(graph, stage);
	for (unsigned int i = 0; i < xbps_array_count(changes); ++i) {
		const char *pkgname = xbps_string_cstring_nocopy(xbps_array_get(changes, i));
		struct node_t *stage_node = NULL;
		struct node_t *repodata_node = NULL;
		xbps_dictionary_t proposed_set = NULL;
		xbps_dictionary_t queue = NULL;

		HASH_FIND(hh, stage->nodes, pkgname, strlen(pkgname), stage_node);
		HASH_FIND(hh, graph->nodes, pkgname, strlen(pkgname), repodata_node);
		// package already processed
		if ((!stage_node && !repodata_node)
		    || (stage_node && repodata_node && strcmp(stage_node->proposed.pkgver, repodata_node->assured.pkgver) == 0)) {
			continue;
		}
		proposed_set = xbps_dictionary_create();
		queue = xbps_dictionary_create();
		xbps_dictionary_set_bool(queue, pkgname, true);
		while (xbps_dictionary_count(queue)) {
			xbps_object_iterator_t iter = xbps_dictionary_iterator(queue);
			xbps_object_t keysym = xbps_object_iterator_next(iter);
			const char *proposed_pkgname = owned_string(xbps_dictionary_keysym_cstring_nocopy(keysym));
			int declined = 0;

			xbps_dictionary_remove_keysym(queue, keysym);
			xbps_object_iterator_release(iter);
			declined = propose_package(proposed_pkgname, graph, stage, queue);
			if (!declined) {
				xbps_dictionary_set_bool(proposed_set, proposed_pkgname, true);
				xbps_dictionary_remove(queue, proposed_pkgname);
				fprintf(stderr, "staged '%s' accepted to repodata\n", proposed_pkgname);
				rv = 0;
			} else {
				fprintf(stderr, "not accepting '%s' to repodata\n", proposed_pkgname);
				revert_to_assured(graph, stage, proposed_set);
				hide_from_stage(pkgname, graph, stage);
			}
		}
		xbps_object_release(proposed_set);
		xbps_object_release(queue);
	}
exit:
	memset(stage->stages, 0, stage->repos_count * sizeof *stage->stages);
	repo_state_release(stage);
	xbps_object_release(changes);
	return rv;
}

static int
write_repos(struct repos_state_t *graph, const char *compression, char *repos[]) {
	xbps_dictionary_t* dictionaries = NULL;
	int rv = 0;

	dictionaries = calloc(graph->repos_count, sizeof *dictionaries);
	if (!dictionaries) {
		fprintf(stderr, "failed to allocate memory\n");
		return 1;
	}
	for (int i = 0; i < graph->repos_count; ++i) {
		dictionaries[i] = xbps_dictionary_create();
		if (!dictionaries[i]) {
			fprintf(stderr, "failed to allocate memory\n");
			rv = ENOMEM;
			goto exit;
		}
	}
	for (struct node_t *node = graph->nodes; node; node = node->hh.next) {
		if (node->assured.dict) {
			xbps_dictionary_set(dictionaries[node->assured.repo], node->pkgname, node->assured.dict);
			fprintf(stderr, "Putting %s (%s) into %s \n", node->pkgname, node->assured.pkgver, graph->repos[node->assured.repo]->uri);
		}
	}
	// make flushing atomic?
	for (int i = 0; i < graph->repos_count; ++i) {
		xbps_dictionary_t idxmeta = graph->repos[i] ? graph->repos[i]->idxmeta : NULL;
		xbps_repodata_flush(graph->xhp, repos[i], "repodata", dictionaries[i], idxmeta, compression);
	}
exit:
	for (int i = 0; i < graph->repos_count; ++i) {
		if (dictionaries[i]) {
			xbps_object_release(dictionaries[i]);
		}
	}
	free(dictionaries);
	return rv;
}

int
index_repos(struct xbps_handle *xhp, const char *compression, int argc, char *argv[])
{
	int rv = 0;
	struct repos_state_t graph = {0};
	struct repolock_t *locks = NULL;

	repo_state_init(&graph, xhp, argc);
	locks = calloc(graph.repos_count, sizeof *locks);
	for (int i = 0; i < graph.repos_count; ++i) {
		const char *path = argv[i];
		bool locked = xbps_repo_lock(xhp, path, &locks[i].fd, &locks[i].name);

		if (!locked) {
			rv = errno;
			fprintf(stderr, "repo '%s' failed to lock\n", path);
			goto exit;
		}
		graph.repos[i] = xbps_repo_public_open(xhp, path);
		if (!graph.repos[i]) {
			if (errno == ENOENT) {
				// TODO: initialize
				xbps_dbg_printf(graph.xhp, "repo index '%s' is not there\n", path);
			} else {
				fprintf(stderr, "repo index '%s' failed to open\n", path);
				rv = errno;
				goto exit;
			}
		}
		graph.stages[i] = xbps_repo_stage_open(xhp, path);
		if (!graph.stages[i]) {
			if (errno == ENOENT) {
				xbps_dbg_printf(graph.xhp, "repo stage '%s' is not there\n", path);
			} else {
				fprintf(stderr, "repo stage '%s' failed to open\n", path);
				rv = errno;
				goto exit;
			}
		}
	}
	rv = build_graph(&graph, SOURCE_REPODATA, true);
	if (!rv) {
		rv = update_repodata_from_stage(&graph);
	} else {
		rv = EALREADY;
//		rv = build_graph(&graph, SOURCE_STAGEDATA, true);
//		if (rv) {
//			fprintf(stderr, "can't initialize graph, exiting\n");
//		}
		// this happily overwrites inconsistent repodata with empty stagedata
		// some heuristic may be needed to prevent
	}
	if (rv == EALREADY) {
		// no updates to apply
		rv = 0;
	} else if (!rv) {
		rv = write_repos(&graph, compression, argv);
	}
exit:
	for (int i = graph.repos_count - 1; i >= 0; --i) {
		if (locks[i].fd) {
			xbps_repo_unlock(locks[i].fd, locks[i].name);
		}
	}
	free(locks);
	repo_state_release(&graph);
	free_owned_strings();
	return rv;
}
