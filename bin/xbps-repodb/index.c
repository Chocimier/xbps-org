#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <xbps.h>
#include "defs.h"
#include "uthash.h"

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
			xbps_dictionary_set(dict, key, array);
			xbps_object_release(array);
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
			xbps_dictionary_set(dict, key, member);
			xbps_object_release(member);
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

static void
package_init(struct package_t *package, xbps_dictionary_t pkg, int repo_serial) {
	xbps_dictionary_get_cstring_nocopy(pkg, "pkgver", &package->pkgver);
	package->revdeps = xbps_array_create();
	package->repo = repo_serial;
	package->dict = xbps_dictionary_copy(pkg);
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
			char depname[XBPS_NAME_SIZE];
			const char *deppattern = NULL;
			struct node_t *depnode = NULL;
			bool ok;
			xbps_array_get_cstring_nocopy(deps, i, &deppattern);
			ok = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
			if (!ok) {
				ok = xbps_pkg_name(depname, sizeof depname, deppattern);
			}
			if (!ok) {
				fprintf(stderr, "'%s' requires '%s' that doesn't contain package name\n", curr_node->pkgname, deppattern);
				rv = ENXIO;
			}
			HASH_FIND(hh, graph->nodes, depname, strlen(depname), depnode);
			if (depnode) {
				if (xbps_pkgpattern_match(depnode->proposed.pkgver, deppattern) != 1) {
					fprintf(stderr, "'%s' requires package '%s', but mismatching '%s' is present\n", curr_node->proposed.pkgver, deppattern, depnode->proposed.pkgver);
					rv = ENOENT;
				}
			} else { //TODO read it
				xbps_dictionary_t virtual_versions;
				bool satisfied = false;
				xbps_object_iterator_t iter;
				xbps_object_t keysym;

				virtual_versions = xbps_dictionary_get(graph->virtual_providers, depname);
				if (!virtual_versions) {
					rv = ENOENT;
					fprintf(stderr, "'%s' requires unavailable package '%s'\n", curr_node->pkgname, deppattern);
					continue;
				}
				iter = xbps_dictionary_iterator(virtual_versions);
				while ((keysym = xbps_object_iterator_next(iter))) {
					const char *provider = xbps_dictionary_keysym_cstring_nocopy(keysym);
					const char *virtual_version;
					xbps_dictionary_get_cstring_nocopy(virtual_versions, provider, &virtual_version);
					/*xbps_dbg_printf(graph->xhp, "%s\t%s\t%s\n", depname, provider, virtual_version);*/
					if (xbps_pkgpattern_match(virtual_version, deppattern) == 1) {
						satisfied = true;
						break;
					}
				}
				xbps_object_iterator_release(iter);
				if (!satisfied) {
					rv = ENOENT;
					fprintf(stderr, "'%s' requires unavailable package or virtual package '%s'\n", curr_node->pkgname, deppattern);
				}
			}
		}
	}

	return rv;
}

static int load_repo(struct repos_state_t *graph, struct xbps_repo *current_repo, enum source source, int repo_serial);

static int
build_graph(struct repos_state_t *graph, enum source source) {
	int rv = 0;
	struct node_t *curr_node;

	for (int i = 0; i < graph->repos_count; ++i) {
		struct xbps_repo *repo = (source == SOURCE_STAGEDATA) ? graph->stages[i] : graph->repos[i];
		if (!repo) {
			continue;
		}
		fprintf(stderr, "loading repo %s, source %x\n", repo->uri, source);
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
				rv = ENXIO;
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
			rv = ENOENT;
		}
	}

	if (rv) {
		goto exit;
	}

	rv = verify_graph(graph);
exit:
	if (!rv) {
		for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
			curr_node->assured = curr_node->proposed;
			memset(&curr_node->proposed, 0, sizeof curr_node->proposed);
		}
	} else {
		fprintf(stderr, "graph from source %x failed to build\n", source);
		repo_state_purge_graph(graph);
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
update_repodata_from_stage(struct repos_state_t *graph) {
	(void) graph;
	return EALREADY;
}

static int write_repos(struct repos_state_t *graph, const char *compression, char *repos[]) {
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
	rv = build_graph(&graph, SOURCE_REPODATA);
	if (!rv) {
		rv = update_repodata_from_stage(&graph);
	} else {
		rv = build_graph(&graph, SOURCE_STAGEDATA);
		if (rv) {
			fprintf(stderr, "can't initialize graph, exiting\n");
		}
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
