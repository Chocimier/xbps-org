#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <xbps.h>
#include "defs.h"
#include "uthash.h"

#include <picosat.h>

enum source {
	SOURCE_REPODATA = 0,
	SOURCE_STAGEDATA,
};

struct variable_t {
	char *str;
	int number;
	UT_hash_handle hh_by_name;
	UT_hash_handle hh_by_number;
};

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
	xbps_dictionary_t dict;
	int repo;
};

struct node_t {
	char *pkgname;
	struct package_t assured;
	struct package_t proposed;
	enum source source;
	UT_hash_handle hh;
};

struct repos_state_t {
	struct node_t *nodes;
	/**
	 * key is pkgname of solib name, value is array of pkgvers providing it
	 */
	xbps_dictionary_t shlib_providers;
	/**
	 * key is virtual pkgname, value is dictionary,
	 * where key is pkgname of real package, value is pkgver of virtual it provides
	 */
	xbps_dictionary_t virtual_providers;
	int repos_count;
	struct xbps_repo **repos;
	struct xbps_repo **stages;
	struct xbps_handle *xhp;
};

static struct hash_str_holder_t *owned_strings_container = NULL;

static struct variable_t *variables_by_name = NULL;
static struct variable_t *variables_by_number = NULL;

static const int VARIABLE_NUMBER_STEP = 4;
static int variable_next_number = VARIABLE_NUMBER_STEP;

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

static int
variable_by_name(const char *pkgver) {
	struct variable_t *holder = NULL;
	unsigned int len = strlen(pkgver);
	char *owned = owned_string(pkgver);

	HASH_FIND(hh_by_name, variables_by_name, owned, len, holder);
	if (!holder) {
		holder = calloc(1, sizeof *holder);
		holder->str = owned;
		holder->number = variable_next_number;
		variable_next_number += VARIABLE_NUMBER_STEP;
//		fprintf(stderr, "%d := '%s'\n", holder->number, pkgver);
		HASH_ADD_KEYPTR(hh_by_name, variables_by_name, owned, len, holder);
		HASH_ADD(hh_by_number, variables_by_number, number, sizeof holder->number, holder);
	}
	return holder->number;
}

static int
variable_real_package(const char *pkgver) {
	return variable_by_name(pkgver);
}

static int
variable_virtual_from_real(int number) {
	return number + 1;
}

static int
variable_virtual_package(const char *pkgver) {
	return variable_virtual_from_real(variable_real_package(pkgver));
}

static int
variable_shlib(const char *shlib) {
	return variable_by_name(shlib) + 2;
}

static const char*
variable_name(int number) {
	struct variable_t *holder = NULL;

	number -= number % VARIABLE_NUMBER_STEP;
	HASH_FIND(hh_by_number, variables_by_number, &number, sizeof(number), holder);
	return (holder ? holder->str : NULL);
}

static void
package_init(struct package_t *package, xbps_dictionary_t pkg, int repo_serial) {
	xbps_dictionary_get_cstring_nocopy(pkg, "pkgver", &package->pkgver);
	package->repo = repo_serial;
	xbps_object_retain(pkg);
	package->dict = pkg;
}

static void
package_release(struct package_t *package) {
	if (!package) {
		return;
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
	xbps_object_release(graph->virtual_providers);
	graph->shlib_providers = xbps_dictionary_create();
	graph->virtual_providers = xbps_dictionary_create();
}

static struct package_t*
node_package_by_source(struct node_t* node, enum source source) {
	if (!node) {
		return NULL;
	} else if (source == SOURCE_REPODATA) {
		return &node->assured;
	} else {
		return &node->proposed;
	}
}

static void
repo_state_init(struct repos_state_t *graph, struct xbps_handle *xhp, int repos_count) {
	graph->shlib_providers = xbps_dictionary_create();
	graph->virtual_providers = xbps_dictionary_create();
	graph->repos_count = repos_count;
	graph->repos = calloc(graph->repos_count, sizeof *graph->repos);
	graph->stages = calloc(graph->repos_count, sizeof *graph->stages);
	graph->xhp = xhp;
}

static void
repo_state_release(struct repos_state_t *graph) {
	repo_state_purge_graph(graph);
	xbps_object_release(graph->shlib_providers);
	xbps_object_release(graph->virtual_providers);
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
load_repo(struct repos_state_t *graph, struct xbps_repo *current_repo, enum source source, int repo_serial) {
	xbps_object_iterator_t iter = NULL;
	xbps_dictionary_keysym_t keysym = NULL;
	struct xbps_repo **repos_array = (source == SOURCE_STAGEDATA) ? graph->stages : graph->repos;

	xbps_dbg_printf(graph->xhp, "loading repo '%s'\n", current_repo->uri);
	iter = xbps_dictionary_iterator(current_repo->idx);
	while ((keysym = xbps_object_iterator_next(iter))) {
		xbps_dictionary_t pkg = xbps_dictionary_get_keysym(current_repo->idx, keysym);
		char *pkgname = owned_string(xbps_dictionary_keysym_cstring_nocopy(keysym));
		struct node_t *new_node = NULL;
		struct node_t *existing_node = NULL;
		struct package_t *existing_package = NULL;

		HASH_FIND(hh, graph->nodes, pkgname, strlen(pkgname), existing_node);
		existing_package = node_package_by_source(existing_node, source);

		if (!existing_node) {
			new_node = calloc(1, sizeof *new_node);
			new_node->pkgname = pkgname;
			package_init(node_package_by_source(new_node, source), pkg, repo_serial);
			HASH_ADD_KEYPTR(hh, graph->nodes, pkgname, strlen(pkgname), new_node);
		} else if (existing_package->pkgver) {
			const char *pkgver = NULL;
			xbps_dictionary_get_cstring_nocopy(pkg, "pkgver", &pkgver);
			//TODO: reverts, look at rindex' index_add
			if (xbps_cmpver(existing_package->pkgver, pkgver) >= 0) { // TODO check if <=
				fprintf(stderr, "'%s' from '%s' is about to push out '%s' from '%s'\n",
				    existing_package->pkgver, repos_array[existing_package->repo]->uri,
				    pkgver, repos_array[repo_serial]->uri);
				continue;
			}
			fprintf(stderr, "'%s' from '%s' is about to push out '%s' from '%s'\n",
			    pkgver, repos_array[repo_serial]->uri,
			    existing_package->pkgver, repos_array[existing_package->repo]->uri);
			package_release(existing_package);
			package_init(existing_package, pkg, repo_serial);
		} else {
			package_init(existing_package, pkg, repo_serial);
		}
	}
	xbps_object_iterator_release(iter);
	return 0;
}

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

static int
build_graph(struct repos_state_t *graph) {
	int rv = 0;

	for (int i = 0; i < graph->repos_count; ++i) {
		for (enum source source = SOURCE_REPODATA; source <= SOURCE_STAGEDATA; ++source) {
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
	}

	for (struct node_t *curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		curr_node->source = SOURCE_STAGEDATA;
		for (enum source source = SOURCE_REPODATA; source <= SOURCE_STAGEDATA; ++source) {
			struct package_t *curr_package = NULL;
			xbps_array_t shlib_provides = NULL;
			xbps_array_t provides = NULL;

			curr_package = node_package_by_source(curr_node, source);
			if (!curr_package->pkgver) {
				continue;
			}

			shlib_provides = xbps_dictionary_get(curr_package->dict, "shlib-provides");
			for (unsigned int i = 0; i < xbps_array_count(shlib_provides); i++) {
				const char *shlib = NULL;
				xbps_array_t providers;

				xbps_array_get_cstring_nocopy(shlib_provides, i, &shlib);
				providers = get_possibly_new_array(graph->shlib_providers, shlib);
				if (!providers) {
					return ENOMEM;
				}
				xbps_array_add_cstring_nocopy(providers, curr_package->pkgver);
			}

			provides = xbps_dictionary_get(curr_package->dict, "provides");
			for (unsigned int i = 0; i < xbps_array_count(provides); i++) {
				const char *virtual = NULL;
				xbps_dictionary_t providers;
				char virtual_pkgname[XBPS_NAME_SIZE] = {0};
				bool ok = false;

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
				xbps_dictionary_set_cstring_nocopy(providers, curr_package->pkgver, owned_string(virtual));
			}
		}
	}

exit:
	if (rv) {
		fprintf(stderr, "graph failed to build\n");
		repo_state_purge_graph(graph);
	}
	return rv;
}

static int
generate_constraints(struct repos_state_t *graph, PicoSAT* solver) {
	int rv = 0;
	for (struct node_t *curr_node = graph->nodes; curr_node; curr_node = curr_node->hh.next) {
		if (curr_node->assured.pkgver && curr_node->proposed.pkgver) {
			if (strcmp(curr_node->assured.pkgver, curr_node->proposed.pkgver) == 0) {
				printf("(%s)\n", curr_node->assured.pkgver);
//				fprintf(stderr, "picosat_add_arg(solver, variable_real_package(curr_node->assured.pkgver)=%d, zero);\n", variable_real_package(curr_node->assured.pkgver));
				picosat_add_arg(solver, variable_real_package(curr_node->assured.pkgver), 0);
			} else {
				int assured_variable = variable_real_package(curr_node->assured.pkgver);
				int proposed_variable = variable_real_package(curr_node->proposed.pkgver);

				printf("%s ↔ ¬ %s\n", curr_node->assured.pkgver, curr_node->proposed.pkgver);
				// p ↔ ¬q == (p → ¬q) ∧ (¬q → p) == (¬p ∨ ¬q) ∧ (q ∨ p)
				//fprintf(stderr, "picosat_add_arg(solver, assured_variable=%d, proposed_variable=%d, zero);\n", assured_variable, proposed_variable);
				picosat_add_arg(solver, assured_variable, proposed_variable, 0);
//				fprintf(stderr, "picosat_add_arg(solver, -assured_variable=%d, -proposed_variable=%d, zero);\n", -assured_variable, -proposed_variable);
				picosat_add_arg(solver, -assured_variable, -proposed_variable, 0);
				picosat_assume(solver, proposed_variable);
			}
		} else if (curr_node->assured.pkgver) {
			picosat_assume(solver, -variable_real_package(curr_node->assured.pkgver));
		} else if (curr_node->proposed.pkgver) {
			picosat_assume(solver, variable_real_package(curr_node->proposed.pkgver));
		}

		for (enum source source = SOURCE_REPODATA; source <= SOURCE_STAGEDATA; ++source) {
			struct package_t *curr_package = node_package_by_source(curr_node, source);
			xbps_array_t shlib_requires = NULL;
			xbps_array_t run_depends = NULL;

			if (!curr_package->pkgver) {
				continue;
			}

			shlib_requires = xbps_dictionary_get(curr_package->dict, "shlib-requires");
			for (unsigned int i = 0; i < xbps_array_count(shlib_requires); i++) {
				const char *shlib = NULL;
				xbps_array_get_cstring_nocopy(shlib_requires, i, &shlib);
				printf("%s → %s\n", curr_package->pkgver, shlib);
//				fprintf(stderr, "picosat_add_arg(solver, -variable_real_package(curr_package->pkgver)=%d, variable_shlib(shlib)=%d, zero);", -variable_real_package(curr_package->pkgver), variable_shlib(shlib));
				picosat_add_arg(solver, -variable_real_package(curr_package->pkgver), variable_shlib(shlib), 0);
			}

			run_depends = xbps_dictionary_get(curr_package->dict, "run_depends");
			for (unsigned int i = 0; i < xbps_array_count(run_depends); i++) {
				const char *deppattern = NULL;
				char depname[XBPS_NAME_SIZE];
				bool ok = false;
				xbps_array_get_cstring_nocopy(run_depends, i, &deppattern);
				if (strcmp(deppattern, "awk>=0") == 0) {
					puts("");
				}
				ok = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
				if (!ok) {
					ok = xbps_pkg_name(depname, sizeof depname, deppattern);
				}
				if (!ok) {
					fprintf(stderr, "'%s' requires '%s' that has no package name\n", curr_node->proposed.pkgver, deppattern);
					rv = ENXIO;
					continue;
				}

				printf("%s → (", curr_package->pkgver);
//				fprintf(stderr, "picosat_add(solver, -variable_real_package(curr_package->pkgver)=%d);\n", -variable_real_package(curr_package->pkgver));
				picosat_add(solver, -variable_real_package(curr_package->pkgver));
				{
					struct node_t *dep_node = NULL;

					HASH_FIND(hh, graph->nodes, depname, strlen(depname), dep_node);

					if (dep_node) {
						if (dep_node->assured.pkgver && xbps_pkgpattern_match(dep_node->assured.pkgver, deppattern)) {
							printf("virt(%s) ∨ ", dep_node->assured.pkgver);
//							fprintf(stderr, "picosat_add(solver, variable_virtual_package(dep_node->assured.pkgver)=%d);\n", variable_virtual_package(dep_node->assured.pkgver));
							picosat_add(solver, variable_virtual_package(dep_node->assured.pkgver));
						}
						if (dep_node->proposed.pkgver && (!dep_node->assured.pkgver || (strcmp(dep_node->assured.pkgver, dep_node->proposed.pkgver) != 0) ) && xbps_pkgpattern_match(dep_node->proposed.pkgver, deppattern)) {
							printf("virt(%s) ∨ ", dep_node->proposed.pkgver);
//							fprintf(stderr, "picosat_add(solver, variable_virtual_package(dep_node->proposed.pkgver)=%d);\n", variable_virtual_package(dep_node->proposed.pkgver));
							picosat_add(solver, variable_virtual_package(dep_node->proposed.pkgver));
						}
					}
				}
				{
					xbps_dictionary_t providers = xbps_dictionary_get(graph->virtual_providers, depname);

					if (providers) {
						xbps_object_iterator_t iter = NULL;
						xbps_dictionary_keysym_t keysym = NULL;

						iter = xbps_dictionary_iterator(providers);
						while ((keysym = xbps_object_iterator_next(iter))) {
							const char *virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, keysym));
							if (xbps_pkgpattern_match(virtual, deppattern)) {
								const char *provider = xbps_dictionary_keysym_cstring_nocopy(keysym);
								printf("virt(%s) ∨ ", provider);
//								fprintf(stderr, "picosat_add(solver, variable_virtual_package(provider)=%d);\n", variable_virtual_package(provider));
								picosat_add(solver, variable_virtual_package(provider));
							}
						}
						xbps_object_iterator_release(iter);
					}
				}
				printf("⊥) [%s]\n", deppattern);
//				fprintf(stderr, "picosat_add(solver, zero);\n");
				picosat_add(solver, 0);
			}
			{
				xbps_dictionary_t providers = xbps_dictionary_get(graph->virtual_providers, curr_node->pkgname);
				// virtual package on left side + real package on right side + providers + terminator
				int *provider_variables = calloc(xbps_dictionary_count(providers) + 3, sizeof *provider_variables);
				int pv_idx = 0;
				int curr_package_real_variable = variable_real_package(curr_package->pkgver);
				int curr_package_virtual_variable = variable_virtual_from_real(curr_package_real_variable);

				// p ↔ (q ∨ r) == (1.) ∧ (2.)
				// 1. p → (q ∨ r) == ¬p ∨ q ∨ r
				// 2. (q ∨ r) → p == (q → p) ∧ (r → p) == (¬q ∨ p) ∧ (¬r ∨ p)
				printf("virt(%s) ↔ (%s", curr_package->pkgver, curr_package->pkgver);
				provider_variables[pv_idx++] = -curr_package_virtual_variable;
				provider_variables[pv_idx++] = curr_package_real_variable;
//				fprintf(stderr, "picosat_add_arg(solver, -curr_package_real_variable=%d, curr_package_virtual_variable=%d, zero);\n", -curr_package_real_variable, curr_package_virtual_variable);
				picosat_add_arg(solver, -curr_package_real_variable, curr_package_virtual_variable, 0);
				if (providers) {
					xbps_object_iterator_t iter = xbps_dictionary_iterator(providers);
					xbps_dictionary_keysym_t keysym = NULL;

					while ((keysym = xbps_object_iterator_next(iter))) {
						const char *virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, keysym));
						if (strcmp(curr_package->pkgver, virtual) == 0) {
							const char *provider = xbps_dictionary_keysym_cstring_nocopy(keysym);
							int provider_variable = variable_real_package(provider);

							printf(" ∨ %s", provider);
							provider_variables[pv_idx++] = provider_variable;
//							fprintf(stderr, "picosat_add_arg(solver, -provider_variable=%d, curr_package_virtual_variable=%d, zero);\n", -provider_variable, curr_package_virtual_variable);
							picosat_add_arg(solver, -provider_variable, curr_package_virtual_variable, 0);
						}
					}
				}
				printf(")\n");
				picosat_add_lits(solver, provider_variables);
				free(provider_variables);
			}
		}
	}
	{
		xbps_object_iterator_t virtual_pkgs_iter = xbps_dictionary_iterator(graph->virtual_providers);
		xbps_dictionary_keysym_t virtual_pkgs_keysym = NULL;

		while ((virtual_pkgs_keysym = xbps_object_iterator_next(virtual_pkgs_iter))) {
			const char *virtual_pkgname = xbps_dictionary_keysym_cstring_nocopy(virtual_pkgs_keysym);
			xbps_dictionary_t providers = xbps_dictionary_get_keysym(graph->virtual_providers, virtual_pkgs_keysym);
			xbps_dictionary_t processed_pkgvers = xbps_dictionary_create();
			struct node_t *realpkg_node = NULL;
			xbps_object_iterator_t providers_outer_iter = NULL;
			xbps_dictionary_keysym_t providers_outer_keysym = NULL;

			HASH_FIND(hh, graph->nodes, virtual_pkgname, strlen(virtual_pkgname), realpkg_node);

			if (realpkg_node) {
				if (realpkg_node->assured.pkgver) {
					xbps_dictionary_set_bool(processed_pkgvers, realpkg_node->assured.pkgver, true);
				}
				if (realpkg_node->proposed.pkgver) {
					xbps_dictionary_set_bool(processed_pkgvers, realpkg_node->proposed.pkgver, true);
				}
			}

			providers_outer_iter = xbps_dictionary_iterator(providers);
			while ((providers_outer_keysym = xbps_object_iterator_next(providers_outer_iter))) {
				const char *outer_virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, providers_outer_keysym));
				xbps_object_iterator_t providers_inner_iter = NULL;
				xbps_dictionary_keysym_t providers_inner_keysym = NULL;
				int *provider_variables = NULL;
				int pv_idx = 0;
				bool dummy_bool = false;
				int outer_virtual_variable = variable_virtual_package(outer_virtual);

				if (xbps_dictionary_get_bool(processed_pkgvers, outer_virtual, &dummy_bool)) {
					continue;
				}
				// virtual package on left side + providers + terminator
				provider_variables = calloc(xbps_dictionary_count(providers) + 2, sizeof *provider_variables);
				printf("virt(%s) ↔ (", outer_virtual);
				provider_variables[pv_idx++] = -outer_virtual_variable;
				providers_inner_iter = xbps_dictionary_iterator(providers);
				while ((providers_inner_keysym = xbps_object_iterator_next(providers_inner_iter))) {
					const char *inner_provider = xbps_dictionary_keysym_cstring_nocopy(providers_inner_keysym);
					const char *inner_virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, providers_inner_keysym));
					if (strcmp(outer_virtual, inner_virtual) == 0) {
						int provider_variable = variable_real_package(inner_provider);

						printf("%s ∨ ", inner_provider);
						provider_variables[pv_idx++] = provider_variable;
//						fprintf(stderr, "picosat_add_arg(solver, -provider_variable=%d, outer_virtual_variable=%d, zero);\n", -provider_variable, outer_virtual_variable);
						picosat_add_arg(solver, -provider_variable, outer_virtual_variable, 0);
					}
				}
				xbps_object_iterator_release(providers_inner_iter);
				printf("⊥)\n");
				picosat_add_lits(solver, provider_variables);
				free(provider_variables);
				xbps_dictionary_set_bool(processed_pkgvers, outer_virtual, true);
			}
			xbps_object_iterator_release(providers_outer_iter);
		}
	}
	{
		xbps_object_iterator_t iter = xbps_dictionary_iterator(graph->shlib_providers);
		xbps_dictionary_keysym_t keysym = NULL;

		while ((keysym = xbps_object_iterator_next(iter))) {
			const char *shlib = xbps_dictionary_keysym_cstring_nocopy(keysym);
			xbps_array_t providers = xbps_dictionary_get_keysym(graph->shlib_providers, keysym);
			// library on left side + providers + terminator
			int *provider_variables = calloc(xbps_array_count(providers) + 2, sizeof *provider_variables);
			int pv_idx = 0;
			int shlib_variable = variable_shlib(shlib);

			printf("%s ↔ (", shlib);
			provider_variables[pv_idx++] = -shlib_variable;
			for (unsigned int i = 0; i < xbps_array_count(providers); ++i) {
				const char *provider = NULL;
				int provider_variable;

				xbps_array_get_cstring_nocopy(providers, i, &provider);
				printf("%s ∨ ", provider);
				provider_variable = variable_real_package(provider);
				provider_variables[pv_idx++] = provider_variable;
//				fprintf(stderr, "picosat_add_arg(solver, -provider_variable=%d, shlib_variable=%d, zero);\n", -provider_variable, shlib_variable);
				picosat_add_arg(solver, -provider_variable, shlib_variable, 0);
			}
			printf("⊥)\n");
				if (!*provider_variables) {
				}
			picosat_add_lits(solver, provider_variables);
			free(provider_variables);
		}
		xbps_object_iterator_release(iter);
	}
	return rv;
}

static int
update_repodata(struct repos_state_t *graph) {
	int rv = 0;
	int decision;
	const int *correcting = NULL;
	PicoSAT *solver = picosat_init();

	rv = generate_constraints(graph, solver);
	if (rv) {
		fprintf(stderr, "Failed to generate constraints: %s\n", strerror(rv));
		goto exit;
	}
//	picosat_print(solver, stdout);
	fprintf(stderr, "picosat_next_minimal_correcting_subset_of_assumptions ...\n");
	correcting = picosat_next_minimal_correcting_subset_of_assumptions(solver);
	decision = picosat_res(solver);
	if (decision != PICOSAT_SATISFIABLE) {
		switch (decision) {
			case PICOSAT_UNKNOWN:
				fprintf(stderr, "solver decision: PICOSAT_UNKNOWN\n");
				break;
			case PICOSAT_UNSATISFIABLE:
				fprintf(stderr, "solver decision: PICOSAT_UNSATISFIABLE\n");
				break;
			default:
				fprintf(stderr, "solver decision: %d\n", decision);
				break;
		}
		fprintf(stderr, "inconsistent: %d\n", picosat_inconsistent(solver));
		rv = EPROTO;
		goto exit;
	}
	xbps_dbg_printf(graph->xhp, "correcting set: %p\n",correcting);
	for (;correcting && *correcting; ++correcting) {
		struct node_t *node = NULL;
		char pkgname[XBPS_NAME_SIZE] = {0};
		const char *pkgver = variable_name(*correcting);

		xbps_pkg_name(pkgname, sizeof pkgname, pkgver);
		xbps_dbg_printf(graph->xhp, "correcting %s\n", pkgver);
		HASH_FIND(hh, graph->nodes, pkgname, strlen(pkgname), node);
		if (!node) {
			fprintf(stderr, "No package '%s' (%s) found\n", pkgname, pkgver);
			rv = EFAULT;
			goto exit;
		}
		node->source = SOURCE_REPODATA;
	}
exit:
	picosat_reset(solver);
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
		struct package_t *package = node_package_by_source(node, node->source);
		if (package && package->dict) {
			xbps_dictionary_set(dictionaries[package->repo], node->pkgname, package->dict);
			xbps_dbg_printf(graph->xhp, "Putting %s (%s) into %s \n", node->pkgname, package->pkgver, graph->repos[package->repo]->uri);
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
				graph.repos[i] = xbps_dictionary_create();
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
	rv = build_graph(&graph);
	if (rv) {
		goto exit;
	}
//	rv = update_repodata(&graph);
	(void) update_repodata;
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
