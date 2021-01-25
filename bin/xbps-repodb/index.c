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
	xbps_array_t deps;
	xbps_array_t revdeps;
	xbps_array_t shlib_requires;
	xbps_array_t shlib_provides;
	xbps_array_t provides;
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
	struct xbps_handle *xhp;
};

static xbps_array_t
get_possibly_new_array(xbps_dictionary_t dict, const char *key) {
	xbps_array_t array = xbps_dictionary_get(dict, key);
	if (!array) {
		array = xbps_array_create();
		if (array) {
			xbps_dictionary_set(dict, key, array);
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
	package->deps = xbps_dictionary_get(pkg, "run_depends");
	package->revdeps = xbps_array_create();
	package->shlib_requires = xbps_dictionary_get(pkg, "shlib-requires");
	package->shlib_provides = xbps_dictionary_get(pkg, "shlib-provides");
	package->provides = xbps_dictionary_get(pkg, "provides");
	package->repo = repo_serial;
	//TODO arrays above are seem redundant now
	package->dict = xbps_dictionary_copy(pkg);
}

static void
package_destroy(struct package_t *package) {
	xbps_object_release(package->revdeps);
}

/**
 * Checks if all packages in graph have dependencies available.
 * @return Zero when graph is consistent, negative if inconsistent, positive on error.
 */
static int
verify_graph(struct repos_state_t *graph) {
	int rv = 0;
	struct node_t *curr_node;

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		for (unsigned int i = 0; i < xbps_array_count(curr_node->proposed.shlib_requires); i++) {
			const char *shlib = NULL;
			xbps_array_get_cstring_nocopy(curr_node->proposed.shlib_requires, i, &shlib);
			if (!xbps_dictionary_get(graph->shlib_providers, shlib)) {
				fprintf(stderr, "'%s' requires unavailable shlib '%s'\n", curr_node->proposed.pkgver, shlib);
				rv = ENOPROTOOPT; //TODO xbps-install
			}
		}
	}

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		for (unsigned int i = 0; i < xbps_array_count(curr_node->proposed.deps); i++) {
			char depname[XBPS_NAME_SIZE];
			const char *deppattern = NULL;
			struct node_t *depnode = NULL;
			bool ok;
			xbps_array_get_cstring_nocopy(curr_node->proposed.deps, i, &deppattern);
			ok = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
			if (!ok) {
				ok = xbps_pkg_name(depname, sizeof depname, deppattern);
			}
			if (!ok) {
				fprintf(stderr, "'%s' requires '%s' that doesn't contain package name\n", curr_node->pkgname, deppattern);
				rv = EFAULT;
			}
			HASH_FIND(hh, graph->nodes, depname, strlen(depname), depnode);
			if (depnode) {
				if (xbps_pkgpattern_match(depnode->proposed.pkgver, deppattern) != 1) {
					fprintf(stderr, "'%s' requires package '%s', but mismatching '%s' is present\n", curr_node->proposed.pkgver, deppattern, depnode->proposed.pkgver);
					rv = EFAULT;
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

static int
build_graph(struct repos_state_t *graph) {
	int rv = 0;
	struct node_t *curr_node;

	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		for (unsigned int i = 0; i < xbps_array_count(curr_node->proposed.shlib_provides); i++) {
			const char *shlib = NULL;
			xbps_array_t providers;
			xbps_array_get_cstring_nocopy(curr_node->proposed.shlib_provides, i, &shlib);
			providers = get_possibly_new_array(graph->shlib_providers, shlib);
			if (!providers) {
				return ENOMEM;
			}
			xbps_array_add_cstring_nocopy(providers, curr_node->pkgname);
		}

		for (unsigned int i = 0; i < xbps_array_count(curr_node->proposed.shlib_requires); i++) {
			const char *shlib = NULL;
			xbps_array_t users;
			xbps_array_get_cstring_nocopy(curr_node->proposed.shlib_requires, i, &shlib);
			users = get_possibly_new_array(graph->shlib_users, shlib);
			if (!users) {
				return ENOMEM;
			}
			xbps_array_add_cstring_nocopy(users, curr_node->pkgname);
		}

		for (unsigned int i = 0; i < xbps_array_count(curr_node->proposed.provides); i++) {
			const char *virtual = NULL;
			xbps_dictionary_t providers;
			char virtual_pkgname[XBPS_NAME_SIZE] = {0};
			bool ok;
			xbps_array_get_cstring_nocopy(curr_node->proposed.provides, i, &virtual);
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
		for (unsigned int i = 0; i < xbps_array_count(curr_node->proposed.deps); i++) {
			const char *deppattern = NULL;
			struct node_t *depnode = NULL;
			xbps_dictionary_t virtual_providers = NULL;
			char depname[XBPS_NAME_SIZE] = {0};
			bool ok;
			xbps_array_get_cstring_nocopy(curr_node->proposed.deps, i, &deppattern);
			ok = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
			if (!ok) {
				ok = xbps_pkg_name(depname, sizeof depname, deppattern);
			}
			if (!ok) {
				fprintf(stderr, "'%s' requires '%s' that has no package name\n", curr_node->proposed.pkgver, deppattern);
				rv = EFAULT;
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
		return rv;
	}

	//if ((rv = verify_graph(graph))) {
		//return rv;
	//}
	for (curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		curr_node->assured = curr_node->proposed;
		memset(&curr_node->proposed, 0, sizeof curr_node->proposed);
	}
	return 0;
}

static int
print_state(struct repos_state_t *graph) {
	xbps_object_iterator_t iter;
	xbps_object_t keysym;

	iter = xbps_dictionary_iterator(graph->shlib_providers);
	while ((keysym = xbps_object_iterator_next(iter))) {
		const char *shlibname = xbps_dictionary_keysym_cstring_nocopy(keysym);
		xbps_array_t pkgs = xbps_dictionary_get(graph->shlib_providers, shlibname);

		fprintf(stdout, "%s:\n", shlibname);

		for (unsigned int i = 0; i < xbps_array_count(pkgs); i++) {
			const char *pkg = NULL;
			xbps_array_get_cstring_nocopy(pkgs, i, &pkg);
			fprintf(stdout, "  %s\n", pkg);
		}
	}
	xbps_object_iterator_release(iter);
	return 0;
}

static int
load_repo(struct repos_state_t *graph, struct xbps_repo *current_repo, int repo_serial) {
	xbps_object_iterator_t iter;
	xbps_object_t keysym;

	xbps_dbg_printf(graph->xhp, "loading repo '%s'\n", current_repo->uri);
	graph->repos[repo_serial] = current_repo;
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
				    existing_node->proposed.pkgver, graph->repos[existing_node->proposed.repo]->uri,
				    new_node->proposed.pkgver, graph->repos[new_node->proposed.repo]->uri);
				package_destroy(&new_node->proposed);
				free(new_node);
				continue;
			}
			fprintf(stderr, "'%s' from '%s' is about to push out '%s' from '%s'\n",
			    existing_node->proposed.pkgver, graph->repos[existing_node->proposed.repo]->uri,
			    new_node->proposed.pkgver, graph->repos[new_node->proposed.repo]->uri);
			HASH_DEL(graph->nodes, existing_node);
			package_destroy(&existing_node->proposed);
			free(existing_node);
		}

		HASH_ADD_KEYPTR(hh, graph->nodes, new_node->pkgname, strlen(new_node->pkgname), new_node);

	}
	xbps_object_iterator_release(iter);
	return 0;
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
			rv = 1;
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
		xbps_repodata_flush(graph->xhp, repos[i], "repodata", dictionaries[i], graph->repos[i]->idxmeta, compression);
	}
exit:
	free(dictionaries);
	return rv;
}

int
index_repos(struct xbps_handle *xhp, const char *compression, int argc, char *argv[])
{
	int rv = 0;
	struct xbps_repo *current_repo;
	struct repos_state_t graph = {0};
	struct repolock_t *locks = NULL;
	int inconsistent = -1;
	graph.shlib_providers = xbps_dictionary_create();
	graph.shlib_users = xbps_dictionary_create();
	graph.virtual_providers = xbps_dictionary_create();
	graph.virtual_users = xbps_dictionary_create();
	graph.repos_count = argc;
	graph.repos = calloc(graph.repos_count, sizeof *graph.repos);
	graph.xhp = xhp;
	locks = calloc(graph.repos_count, sizeof *locks);
	for (int i = 0; i < graph.repos_count; ++i) {
		const char *path = argv[i];
		bool locked = xbps_repo_lock(xhp, path, &locks[i].fd, &locks[i].name);
		if (!locked) {
			rv = 1;
			goto exit;
		}
		current_repo = xbps_repo_public_open(xhp, path);
		if (current_repo == NULL) {
			fprintf(stderr, "repo '%s' failed to open\n", path);
			rv = 1;
			goto exit;
		}
		rv = load_repo(&graph, current_repo, i);
		if (rv) {
			fprintf(stderr, "can't load '%s' repo into graph, exiting\n", path);
			goto exit;
		}
	}
	rv = build_graph(&graph);
	if (rv) {
		fprintf(stderr, "can't initialize graph, exiting\n");
		goto exit;
	}
	(void) print_state;
	(void)verify_graph;
	inconsistent = false; // verify_graph(&graph);
	if (inconsistent) {
		fprintf(stderr, "inconsistent graph, exiting\n");
		rv = 1;
	} else {
		rv = write_repos(&graph, compression, argv);
	}
exit:
	for (int i = graph.repos_count - 1; i >= 0; --i) {
		if (locks[i].fd) {
			xbps_repo_unlock(locks[i].fd, locks[i].name);
		}
	}
	free(locks);
	free(graph.repos);
	free_owned_strings();
	return rv;
}
