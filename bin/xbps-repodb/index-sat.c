#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <xbps.h>
#include "defs.h"
#include "uthash.h"

#include <picosat.h>

#define VARIABLE_NUMBER_STEP 4

enum source {
	SOURCE_PUBLIC = 0,
	SOURCE_STAGE,
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

struct package_t {
	const char *pkgver;
	xbps_dictionary_t dict;
	int repo;
};

struct node_t {
	char *pkgname;
	struct package_t packages[2];
	enum source source;
	UT_hash_handle hh;
};

struct repo_t {
	xbps_dictionary_t idx;
	xbps_dictionary_t meta;
	struct xbps_repo *repo;
	char *lock_name;
	int lock_fd;
};

struct repos_group_t {
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
	/** array of pairs of repo_t */
	struct repo_t (*repos)[2];
	xbps_array_t text_clauses;
	struct xbps_handle *xhp;
};

static struct hash_str_holder_t *owned_strings_container = NULL;

static struct variable_t *variables_by_name = NULL;
static struct variable_t *variables_by_number = NULL;

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
repo_group_purge_packages(struct repos_group_t *group) {
	struct node_t *current_node = NULL;
	struct node_t *tmp_node = NULL;

	HASH_ITER(hh, group->nodes,current_node, tmp_node) {
		HASH_DEL(group->nodes, current_node);
		package_release(&current_node->packages[SOURCE_PUBLIC]);
		package_release(&current_node->packages[SOURCE_STAGE]);
		free(current_node);
	}
	xbps_object_release(group->shlib_providers);
	xbps_object_release(group->virtual_providers);
	group->shlib_providers = xbps_dictionary_create();
	group->virtual_providers = xbps_dictionary_create();
}

static void
repo_group_init(struct repos_group_t *group, struct xbps_handle *xhp, int repos_count) {
	group->shlib_providers = xbps_dictionary_create();
	group->virtual_providers = xbps_dictionary_create();
	group->repos_count = repos_count;
	group->repos = calloc(group->repos_count, sizeof *group->repos);
	group->text_clauses = xbps_array_create();
	group->xhp = xhp;
}

static void
repo_group_release(struct repos_group_t *group) {
	repo_group_purge_packages(group);
	xbps_object_release(group->shlib_providers);
	xbps_object_release(group->virtual_providers);
	for(int i = 0; i < group->repos_count; ++i) {
		if (group->repos[i][SOURCE_PUBLIC].repo) {
			xbps_repo_release(group->repos[i][SOURCE_PUBLIC].repo);
		}
		if (group->repos[i][SOURCE_STAGE].repo) {
			xbps_repo_release(group->repos[i][SOURCE_STAGE].repo);
		}
	}
	free(group->repos);
	xbps_object_release(group->text_clauses);
}

static int
load_repo(struct repos_group_t *group, struct xbps_repo *current_repo, enum source source, int repo_serial) {
	xbps_object_iterator_t iter = NULL;
	xbps_dictionary_keysym_t keysym = NULL;

	xbps_dbg_printf(group->xhp, "loading repo '%s'\n", current_repo->uri);
	iter = xbps_dictionary_iterator(current_repo->idx);
	while ((keysym = xbps_object_iterator_next(iter))) {
		xbps_dictionary_t pkg = xbps_dictionary_get_keysym(current_repo->idx, keysym);
		char *pkgname = owned_string(xbps_dictionary_keysym_cstring_nocopy(keysym));
		struct node_t *new_node = NULL;
		struct node_t *existing_node = NULL;
		struct package_t *existing_package = NULL;

		HASH_FIND(hh, group->nodes, pkgname, strlen(pkgname), existing_node);
		existing_package = &existing_node->packages[source];

		if (!existing_node) {
			new_node = calloc(1, sizeof *new_node);
			new_node->pkgname = pkgname;
			package_init(&new_node->packages[source], pkg, repo_serial);
			HASH_ADD_KEYPTR(hh, group->nodes, pkgname, strlen(pkgname), new_node);
		} else if (existing_package->pkgver) {
			const char *pkgver = NULL;
			xbps_dictionary_get_cstring_nocopy(pkg, "pkgver", &pkgver);
			if (xbps_pkg_version_order(existing_package->dict, pkg) >= 0) {
				fprintf(stderr, "'%s' from '%s' is about to push out '%s' from '%s'\n",
				    existing_package->pkgver, group->repos[existing_package->repo][source].repo->uri,
				    pkgver, group->repos[repo_serial][source].repo->uri);
				continue;
			}
			fprintf(stderr, "'%s' from '%s' is about to push out '%s' from '%s'\n",
			    pkgver, group->repos[repo_serial][source].repo->uri,
			    existing_package->pkgver,group->repos[existing_package->repo][source].repo->uri);
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

static void
add_text_clause(struct repos_group_t *group, char *clause, int copies) {
	xbps_object_t obj = xbps_string_create_cstring(clause);
	xbps_dbg_printf(group->xhp, "%s [%d]\n", clause, xbps_array_count(group->text_clauses));
	while (copies--) {
		xbps_array_set(group->text_clauses, xbps_array_count(group->text_clauses), obj);
	}
	free(clause);
}

static int
build_group(struct repos_group_t *group) {
	int rv = 0;

	for (int i = 0; i < group->repos_count; ++i) {
		for (enum source source = SOURCE_PUBLIC; source <= SOURCE_STAGE; ++source) {
			struct xbps_repo *repo = group->repos[i][source].repo;
			fprintf(stderr, "loading repo %d %p '%s', source %x\n", i, repo, (repo ? repo->uri : NULL), source);
			if (!repo) {
				continue;
			}
			rv = load_repo(group, repo, source, i);
			if (rv) {
				fprintf(stderr, "can't load '%s' repo into group, exiting\n", repo->uri);
				goto exit;
			}
		}
	}

	for (struct node_t *curr_node = group->nodes; curr_node; curr_node = curr_node->hh.next) {
		curr_node->source = SOURCE_STAGE;
		for (enum source source = SOURCE_PUBLIC; source <= SOURCE_STAGE; ++source) {
			struct package_t *curr_package = NULL;
			xbps_array_t shlib_provides = NULL;
			xbps_array_t provides = NULL;

			curr_package = &curr_node->packages[source];
			if (!curr_package->pkgver) {
				continue;
			}

			shlib_provides = xbps_dictionary_get(curr_package->dict, "shlib-provides");
			for (unsigned int i = 0; i < xbps_array_count(shlib_provides); i++) {
				const char *shlib = NULL;
				xbps_array_t providers;

				xbps_array_get_cstring_nocopy(shlib_provides, i, &shlib);
				providers = get_possibly_new_array(group->shlib_providers, shlib);
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
					xbps_dbg_printf(group->xhp, "virtual '%s' (%s) provided by '%s'\n", virtual_pkgname, virtual, curr_node->pkgname);
				} else {
					xbps_dbg_printf(group->xhp, "invalid virtual pkgver '%s' provided by package '%s', ignoring\n", virtual, curr_node->pkgname);
					continue;
				}
				providers = get_possibly_new_dictionary(group->virtual_providers, owned_string(virtual_pkgname));
				if (!providers) {
					return ENOMEM;
				}
				xbps_dictionary_set_cstring_nocopy(providers, curr_package->pkgver, owned_string(virtual));
			}
		}
	}

exit:
	if (rv) {
		fprintf(stderr, "group failed to build\n");
		repo_group_purge_packages(group);
	}
	return rv;
}

static int
generate_constraints(struct repos_group_t *group, PicoSAT* solver, bool explaining) {
	int rv = 0;
	for (struct node_t *curr_node = group->nodes; curr_node; curr_node = curr_node->hh.next) {
		const char *curr_public_pkgver = curr_node->packages[SOURCE_PUBLIC].pkgver;
		const char *curr_stage_pkgver = curr_node->packages[SOURCE_STAGE].pkgver;
		if (curr_public_pkgver && curr_stage_pkgver) {
			if (strcmp(curr_public_pkgver, curr_stage_pkgver) == 0) {
				if (explaining) {
					char *clause = xbps_xasprintf("⊤ → %s", curr_public_pkgver);
					add_text_clause(group, clause, 1);
				}
				picosat_add_arg(solver, variable_real_package(curr_public_pkgver), 0);
			} else {
				int public_variable = variable_real_package(curr_public_pkgver);
				int stage_variable = variable_real_package(curr_stage_pkgver);

				if (explaining) {
					char *clause = xbps_xasprintf("%s ↔ ¬ %s", curr_public_pkgver, curr_stage_pkgver);
					add_text_clause(group, clause, 2);
				}
				// p ↔ ¬q == (p → ¬q) ∧ (¬q → p) == (¬p ∨ ¬q) ∧ (q ∨ p)
				picosat_add_arg(solver, public_variable, stage_variable, 0);
				picosat_add_arg(solver, -public_variable, -stage_variable, 0);
				if (!explaining) {
					picosat_assume(solver, stage_variable);
				}
			}
		} else if (curr_public_pkgver) {
			if (!explaining) {
				picosat_assume(solver, -variable_real_package(curr_public_pkgver));
			}
		} else if (curr_stage_pkgver) {
			if (!explaining) {
				picosat_assume(solver, variable_real_package(curr_stage_pkgver));
			}
		}

		for (enum source source = SOURCE_PUBLIC; source <= SOURCE_STAGE; ++source) {
			struct package_t *curr_package = &curr_node->packages[source];
			xbps_array_t shlib_requires = NULL;
			xbps_array_t run_depends = NULL;

			if (!curr_package->pkgver) {
				continue;
			}

			shlib_requires = xbps_dictionary_get(curr_package->dict, "shlib-requires");
			for (unsigned int i = 0; i < xbps_array_count(shlib_requires); i++) {
				const char *shlib = NULL;
				xbps_array_get_cstring_nocopy(shlib_requires, i, &shlib);
				if (explaining) {
					char *clause = xbps_xasprintf("%s → %s", curr_package->pkgver, shlib);
					add_text_clause(group, clause, 1);
				}
				picosat_add_arg(solver, -variable_real_package(curr_package->pkgver), variable_shlib(shlib), 0);
			}

			run_depends = xbps_dictionary_get(curr_package->dict, "run_depends");
			for (unsigned int i = 0; i < xbps_array_count(run_depends); i++) {
				const char *deppattern = NULL;
				char *clause = NULL;
				char *clause_part = NULL;
				char depname[XBPS_NAME_SIZE];
				bool ok = false;

				xbps_array_get_cstring_nocopy(run_depends, i, &deppattern);
				ok = xbps_pkgpattern_name(depname, sizeof depname, deppattern);
				if (!ok) {
					ok = xbps_pkg_name(depname, sizeof depname, deppattern);
				}
				if (!ok) {
					fprintf(stderr, "'%s' requires '%s' that has no package name\n", curr_package->pkgver, deppattern);
					rv = ENXIO;
					continue;
				}

				if (explaining) {
					clause = xbps_xasprintf("%s → (", curr_package->pkgver);
				}
				picosat_add(solver, -variable_real_package(curr_package->pkgver));
				{
					struct node_t *dep_node = NULL;

					HASH_FIND(hh, group->nodes, depname, strlen(depname), dep_node);

					if (dep_node) {
						const char *dep_public_pkgver = dep_node->packages[SOURCE_PUBLIC].pkgver;
						const char *dep_stage_pkgver = dep_node->packages[SOURCE_STAGE].pkgver;
						if (dep_public_pkgver && xbps_pkgpattern_match(dep_public_pkgver, deppattern)) {
							if (explaining) {
								clause_part = clause;
								clause = xbps_xasprintf("%svirt(%s) ∨ ", clause_part, dep_public_pkgver);
								free(clause_part);
							}
							picosat_add(solver, variable_virtual_package(dep_public_pkgver));
						}
						if (dep_stage_pkgver && (!dep_public_pkgver || (strcmp(dep_public_pkgver, dep_stage_pkgver) != 0) ) && xbps_pkgpattern_match(dep_stage_pkgver, deppattern)) {
							if (explaining) {
								clause_part = clause;
								clause = xbps_xasprintf("%svirt(%s) ∨ ", clause_part, dep_stage_pkgver);
								free(clause_part);
							}
							picosat_add(solver, variable_virtual_package(dep_stage_pkgver));
						}
					}
				}
				{
					xbps_dictionary_t providers = xbps_dictionary_get(group->virtual_providers, depname);

					if (providers) {
						xbps_object_iterator_t iter = NULL;
						xbps_dictionary_keysym_t keysym = NULL;

						iter = xbps_dictionary_iterator(providers);
						while ((keysym = xbps_object_iterator_next(iter))) {
							const char *virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, keysym));
							if (xbps_pkgpattern_match(virtual, deppattern)) {
								const char *provider = xbps_dictionary_keysym_cstring_nocopy(keysym);
								if (explaining) {
									clause_part = clause;
									clause = xbps_xasprintf("%svirt(%s) ∨ ", clause_part, provider);
									free(clause_part);
								}
								picosat_add(solver, variable_virtual_package(provider));
							}
						}
						xbps_object_iterator_release(iter);
					}
				}
				if (explaining) {
					clause_part = clause;
					clause = xbps_xasprintf("%s⊥) {%s}", clause_part, deppattern);
					free(clause_part);
					add_text_clause(group, clause, 1);
				}
				picosat_add(solver, 0);
			}
			{
				xbps_dictionary_t providers = xbps_dictionary_get(group->virtual_providers, curr_node->pkgname);
				// virtual package on left side + real package on right side + providers + terminator
				int *provider_variables = calloc(xbps_dictionary_count(providers) + 3, sizeof *provider_variables);
				char *clause = NULL;
				char *clause_part = NULL;
				int pv_idx = 0;
				int copies_count = 0;
				int curr_package_real_variable = variable_real_package(curr_package->pkgver);
				int curr_package_virtual_variable = variable_virtual_from_real(curr_package_real_variable);

				// p ↔ (q ∨ r) == (1.) ∧ (2.)
				// 1. p → (q ∨ r) == ¬p ∨ q ∨ r
				// 2. (q ∨ r) → p == (q → p) ∧ (r → p) == (¬q ∨ p) ∧ (¬r ∨ p)
				if (explaining) {
					clause = xbps_xasprintf("virt(%s) ↔ (%s", curr_package->pkgver, curr_package->pkgver);
					copies_count = 2;
				}
				provider_variables[pv_idx++] = -curr_package_virtual_variable;
				provider_variables[pv_idx++] = curr_package_real_variable;
				picosat_add_arg(solver, -curr_package_real_variable, curr_package_virtual_variable, 0);
				if (providers) {
					xbps_object_iterator_t iter = xbps_dictionary_iterator(providers);
					xbps_dictionary_keysym_t keysym = NULL;

					while ((keysym = xbps_object_iterator_next(iter))) {
						const char *virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, keysym));
						if (strcmp(curr_package->pkgver, virtual) == 0) {
							const char *provider = xbps_dictionary_keysym_cstring_nocopy(keysym);
							int provider_variable = variable_real_package(provider);

							if (explaining) {
								clause_part = clause;
								clause = xbps_xasprintf("%s ∨ %s", clause_part, provider);
								free(clause_part);
								++copies_count;
							}
							provider_variables[pv_idx++] = provider_variable;
							picosat_add_arg(solver, -provider_variable, curr_package_virtual_variable, 0);
						}
					}
				}
				if (explaining) {
					clause_part = clause;
					clause = xbps_xasprintf("%s)", clause_part);
					free(clause_part);
					add_text_clause(group, clause, copies_count);
				}
				picosat_add_lits(solver, provider_variables);
				free(provider_variables);
			}
		}
	}
	{
		xbps_object_iterator_t virtual_pkgs_iter = xbps_dictionary_iterator(group->virtual_providers);
		xbps_dictionary_keysym_t virtual_pkgs_keysym = NULL;

		while ((virtual_pkgs_keysym = xbps_object_iterator_next(virtual_pkgs_iter))) {
			const char *virtual_pkgname = xbps_dictionary_keysym_cstring_nocopy(virtual_pkgs_keysym);
			xbps_dictionary_t providers = xbps_dictionary_get_keysym(group->virtual_providers, virtual_pkgs_keysym);
			xbps_dictionary_t processed_pkgvers = xbps_dictionary_create();
			struct node_t *realpkg_node = NULL;
			xbps_object_iterator_t providers_outer_iter = NULL;
			xbps_dictionary_keysym_t providers_outer_keysym = NULL;

			HASH_FIND(hh, group->nodes, virtual_pkgname, strlen(virtual_pkgname), realpkg_node);

			if (realpkg_node) {
				if (realpkg_node->packages[SOURCE_PUBLIC].pkgver) {
					xbps_dictionary_set_bool(processed_pkgvers, realpkg_node->packages[SOURCE_PUBLIC].pkgver, true);
				}
				if (realpkg_node->packages[SOURCE_STAGE].pkgver) {
					xbps_dictionary_set_bool(processed_pkgvers, realpkg_node->packages[SOURCE_STAGE].pkgver, true);
				}
			}

			providers_outer_iter = xbps_dictionary_iterator(providers);
			while ((providers_outer_keysym = xbps_object_iterator_next(providers_outer_iter))) {
				const char *outer_virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, providers_outer_keysym));
				xbps_object_iterator_t providers_inner_iter = NULL;
				xbps_dictionary_keysym_t providers_inner_keysym = NULL;
				int *provider_variables = NULL;
				char *clause = NULL;
				char *clause_part = NULL;
				int pv_idx = 0;
				int copies_count = 0;
				bool dummy_bool = false;
				int outer_virtual_variable = variable_virtual_package(outer_virtual);

				if (xbps_dictionary_get_bool(processed_pkgvers, outer_virtual, &dummy_bool)) {
					continue;
				}
				// virtual package on left side + providers + terminator
				provider_variables = calloc(xbps_dictionary_count(providers) + 2, sizeof *provider_variables);
				if (explaining) {
					clause = xbps_xasprintf("virt(%s) ↔ (", outer_virtual);
					copies_count = 1;
				}
				provider_variables[pv_idx++] = -outer_virtual_variable;
				providers_inner_iter = xbps_dictionary_iterator(providers);
				while ((providers_inner_keysym = xbps_object_iterator_next(providers_inner_iter))) {
					const char *inner_provider = xbps_dictionary_keysym_cstring_nocopy(providers_inner_keysym);
					const char *inner_virtual = xbps_string_cstring_nocopy(xbps_dictionary_get_keysym(providers, providers_inner_keysym));
					if (strcmp(outer_virtual, inner_virtual) == 0) {
						int provider_variable = variable_real_package(inner_provider);

						if (explaining) {
							clause_part = clause;
							clause = xbps_xasprintf("%s%s ∨ ", clause_part, inner_provider);
							free(clause_part);
							++copies_count;
						}
						provider_variables[pv_idx++] = provider_variable;
						picosat_add_arg(solver, -provider_variable, outer_virtual_variable, 0);
					}
				}
				xbps_object_iterator_release(providers_inner_iter);
				if (explaining) {
					clause_part = clause;
					clause = xbps_xasprintf("%s⊥)", clause_part);
					free(clause_part);
					add_text_clause(group, clause, copies_count);
				}
				picosat_add_lits(solver, provider_variables);
				free(provider_variables);
				xbps_dictionary_set_bool(processed_pkgvers, outer_virtual, true);
			}
			xbps_object_iterator_release(providers_outer_iter);
		}
	}
	{
		xbps_object_iterator_t iter = xbps_dictionary_iterator(group->shlib_providers);
		xbps_dictionary_keysym_t keysym = NULL;

		while ((keysym = xbps_object_iterator_next(iter))) {
			const char *shlib = xbps_dictionary_keysym_cstring_nocopy(keysym);
			xbps_array_t providers = xbps_dictionary_get_keysym(group->shlib_providers, keysym);
			// library on left side + providers + terminator
			int *provider_variables = calloc(xbps_array_count(providers) + 2, sizeof *provider_variables);
			char *clause = NULL;
			char *clause_part = NULL;
			int pv_idx = 0;
			int copies_count = 0;
			int shlib_variable = variable_shlib(shlib);

			if (explaining) {
				clause = xbps_xasprintf("%s ↔ (", shlib);
				copies_count = 1;
			}
			provider_variables[pv_idx++] = -shlib_variable;
			for (unsigned int i = 0; i < xbps_array_count(providers); ++i) {
				const char *provider = NULL;
				int provider_variable;

				xbps_array_get_cstring_nocopy(providers, i, &provider);
				if (explaining) {
					clause_part = clause;
					clause = xbps_xasprintf("%s%s ∨ ", clause_part, provider);
					++copies_count;
				}
				provider_variable = variable_real_package(provider);
				provider_variables[pv_idx++] = provider_variable;
				picosat_add_arg(solver, -provider_variable, shlib_variable, 0);
			}
			if (explaining) {
				clause_part = clause;
				clause = xbps_xasprintf("%s⊥)", clause_part);
				free(clause_part);
				add_text_clause(group, clause, copies_count);
			}
			picosat_add_lits(solver, provider_variables);
			free(provider_variables);
		}
		xbps_object_iterator_release(iter);
	}
	return rv;
}

static int
explain_inconsistency(struct repos_group_t *group) {
	PicoSAT *solver = picosat_init();
	int rv = 0;

	picosat_enable_trace_generation(solver);
	rv = generate_constraints(group, solver, true);
	if (rv) {
		fprintf(stderr, "Failed to generate constraints for explaining: %s\n", strerror(rv));
		goto exit;
	}
	picosat_sat(solver, -1);
	fprintf(stderr, "Inconsistent clauses:\n");
	for (int i = 0; i < picosat_added_original_clauses(solver); ++i) {
		if (picosat_coreclause(solver, i)) {
			const char *clause = NULL;
			xbps_array_get_cstring_nocopy(group->text_clauses, i, &clause);
			fprintf(stderr, " %s\n", clause);
		}
	}
exit:
	picosat_reset(solver);
	return rv;
}

static int
update_repodata(struct repos_group_t *group) {
	int rv = 0;
	int decision;
	const int *correcting = NULL;
	PicoSAT *solver = picosat_init();

	rv = generate_constraints(group, solver, false);
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
		explain_inconsistency(group);
		rv = EPROTO;
		goto exit;
	}
	xbps_dbg_printf(group->xhp, "correcting set: %p\n",correcting);
	for (;correcting && *correcting; ++correcting) {
		struct node_t *node = NULL;
		char pkgname[XBPS_NAME_SIZE] = {0};
		const char *pkgver = variable_name(*correcting);

		xbps_pkg_name(pkgname, sizeof pkgname, pkgver);
		xbps_dbg_printf(group->xhp, "correcting %s\n", pkgver);
		HASH_FIND(hh, group->nodes, pkgname, strlen(pkgname), node);
		if (!node) {
			fprintf(stderr, "No package '%s' (%s) found\n", pkgname, pkgver);
			rv = EFAULT;
			goto exit;
		}
		node->source = SOURCE_PUBLIC;
	}
exit:
	picosat_reset(solver);
	return rv;
}

static int
write_repos(struct repos_group_t *group, const char *compression, char *repos[]) {
	xbps_dictionary_t* dictionaries = NULL;
	int rv = 0;

	dictionaries = calloc(group->repos_count, sizeof *dictionaries);
	if (!dictionaries) {
		fprintf(stderr, "failed to allocate memory\n");
		return 1;
	}
	for (int i = 0; i < group->repos_count; ++i) {
		dictionaries[i] = xbps_dictionary_create();
		if (!dictionaries[i]) {
			fprintf(stderr, "failed to allocate memory\n");
			rv = ENOMEM;
			goto exit;
		}
	}
	for (struct node_t *node = group->nodes; node; node = node->hh.next) {
		struct package_t *package = &node->packages[node->source];
		if (package && package->dict) {
			xbps_dictionary_set(dictionaries[package->repo], node->pkgname, package->dict);
			xbps_dbg_printf(group->xhp, "Putting %s (%s) into %s \n", node->pkgname, package->pkgver, repos[package->repo]);
		}
	}
	// make flushing atomic?
	for (int i = 0; i < group->repos_count; ++i) {
		xbps_repodata_flush(group->xhp, repos[i], "repodata", dictionaries[i], group->repos[i][SOURCE_PUBLIC].meta, compression);
	}
exit:
	for (int i = 0; i < group->repos_count; ++i) {
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
	struct repos_group_t group = {0};

	repo_group_init(&group, xhp, argc);
	for (int i = 0; i < group.repos_count; ++i) {
		const char *path = argv[i];
		struct repo_t *public = &group.repos[i][SOURCE_PUBLIC];
		struct repo_t *stage = &group.repos[i][SOURCE_STAGE];
		bool locked = xbps_repo_lock(xhp, path, &public->lock_fd, &public->lock_name);

		if (!locked) {
			rv = errno;
			fprintf(stderr, "repo '%s' failed to lock\n", path);
			goto exit;
		}
		public->repo = xbps_repo_public_open(xhp, path);
		if (public->repo) {
			public->idx = public->repo->idx;
			public->meta = public->repo->idxmeta;
		} else if (errno == ENOENT) {
			public->idx = xbps_dictionary_create();
			public->meta = NULL;
			xbps_dbg_printf(group.xhp, "repo index '%s' is not there\n", path);
		} else {
			fprintf(stderr, "repo index '%s' failed to open\n", path);
			rv = errno;
			goto exit;
		}
		stage->repo = xbps_repo_stage_open(xhp, path);
		if (stage->repo) {
			stage->idx = stage->repo->idx;
			stage->meta = stage->repo->idxmeta;
		} else if (errno == ENOENT) {
			xbps_dbg_printf(group.xhp, "repo stage '%s' is not there\n", path);
		} else {
			fprintf(stderr, "repo stage '%s' failed to open\n", path);
			rv = errno;
			goto exit;
		}
	}
	rv = build_group(&group);
	if (rv) {
		goto exit;
	}
	rv = update_repodata(&group);
	if (rv == EALREADY) {
		// no updates to apply
		rv = 0;
	} else if (!rv) {
		rv = write_repos(&group, compression, argv);
	}
exit:
	for (int i = group.repos_count - 1; i >= 0; --i) {
		if (group.repos[i][SOURCE_PUBLIC].lock_fd) {
			xbps_repo_unlock(group.repos[i][SOURCE_PUBLIC].lock_fd, group.repos[i][SOURCE_PUBLIC].lock_name);
		}
	}
	repo_group_release(&group);
	free_owned_strings();
	return rv;
}
