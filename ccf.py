# On implémente ci-dessous les algorithmes présentés dans le papier
# Dans un premier temps, on écrit le code de l'algorithme sans optimisation (algo dit 'vanilla').
# Par la suite, on s'intéresse aux manières d'optimiser cette première ébauche de CCF.
# Par curiosité, on regardera deux façons de faire cela : 
# - un algo naïf, inspiré de https://github.com/louis-monier/SparkProject/blob/master/ccf-project-pyspark.ipynb, qui 
# semble au premier abord être une implémentation correcte de la version Secondary sort (mais qui en fait ne l'est pas!)
# - un algo qui met en pratique réellement les optimisations proposées dans le papier


def ccf_dedup(rdd):
    mapped_rdd = rdd.map(lambda edge: (edge, None))

    # Shuffle & Sort va regrouper toutes les keys identiques
    # On utilise reduceByKey pour ne garder qu'une seule occurrence.
    reduced_rdd = mapped_rdd.reduceByKey(lambda v1, v2: v1)
    final_rdd = reduced_rdd.map(lambda kv: kv[0])

    # On pourra éventuellement comparer cette implémentation avec le distinct() natif en Spark par la suite

    return final_rdd


def ccf_iterate_vanilla(rdd, new_pair_accum):

    bothDirections = rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])])

    def process_vanilla(key_values):
        key, values = key_values
        val_list = list(values)  # la conversion en list() force le chargement en RAM de toutes les valeurs
        min_val = min(val_list)

        emitted = []
        if min_val < key:
            emitted.append((key, min_val))
            for v in val_list:
                if v != min_val:
                    new_pair_accum.add(1)
                    emitted.append((v, min_val))
        return emitted

    return bothDirections.groupByKey().flatMap(process_vanilla)


def ccf_iterate_SecondSort_naive(rdd, new_pair_accum):
    adj = rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])])

    def process_ss_naive(key_values):
        key, values = key_values

        # Pourquoi est-ce qu'on a ici une méthode dite naïve ?
        # On voudrait recopier exactement l'algorithme du papier, en ayant une liste triée dont on choisirait la première valeur
        # Sauf qu'en faisant ça, on stocke les valeurs en mémoire, et on demande à les trier ensuite
        # Finalement, on a une complexité spatiale O(n) en RAM + opération de tri lourde infligée au CPU
        sorted_vals = sorted(list(values))
        min_val = sorted_vals[0]

        emitted = []
        if min_val < key:
            emitted.append((key, min_val))
            for v in sorted_vals:
                if v != min_val:
                    new_pair_accum.add(1)
                    emitted.append((v, min_val))
        return emitted

    return adj.groupByKey().flatMap(process_ss_naive)


def ccf_iterate_optimised(rdd, new_pair_accum):

    bothDirections = rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])])
    mins = bothDirections.reduceByKey(lambda v1, v2: v1 if v1 < v2 else v2) #on calcule les minima locaux à chaque fois, sans surcharger la RAM
    # on préfère utiliser reduceByKey ici (pour la raison énoncée juste avant), plutôt que groupByKey.
    # En effet, groupByKey shuffle toutes les données pour que les mêmes valeurs associées à une même clé se retrouvent sur une même partition.
    # Or, ici, on n'a pas besoin de garder la liste d'adjacence complète pour chaque sommet, uniquement le sommet de valeur minimale
    # On choisit donc reduceByKey, pour calculer le minimum localement sur chaque partition avant de transférer quoi que ce soit sur le réseau


    mins_filtered = mins.filter(lambda x: x[1] < x[0])
    emit_1 = mins_filtered


    # On doit maintenant propager le minimum à tous les voisins
    # La jointure se fait sur la clé, donc le résultat du join est (key, (v, min_val))
    joined = bothDirections.join(mins_filtered)

    # Si le voisin est différent du minimum, on émet la nouvelle paire
    def process_joined(item):
        key, (v, min_val) = item
        if v != min_val:
            new_pair_accum.add(1)
            return [(v, min_val)]
        return []

    emit_2 = joined.flatMap(process_joined)
    return emit_1.union(emit_2)


def ccf(sc, rdd, method="vanilla"):

    iteration = 1

    while True:
        new_pair_accum = sc.accumulator(0)

        if method == "vanilla":
            iterated_rdd = ccf_iterate_vanilla(rdd, new_pair_accum)
        elif method == "sec_sort_naive":
            iterated_rdd = ccf_iterate_SecondSort_naive(rdd, new_pair_accum)
        elif method == "optimised":
            iterated_rdd = ccf_iterate_optimised(rdd, new_pair_accum)
        else:
            raise ValueError("Unknown method. Please choose in [vanilla, sec_sort_naive, optimised]")

        rdd = ccf_dedup(iterated_rdd) # Rappel: il faudra comparer avec distinct

        # Spark utilise la lazy evaluation: on doit forcer l'exécution avec un count()
        # pour que l'accumulateur se mette à jour avant la condition d'arrêt.
        rdd.cache()  # Mise en cache pour ne pas recalculer l'arbre à l'itération suivante
        rdd.count()

        new_pairs = new_pair_accum.value
        print(f"[{method}] Iteration {iteration}: {new_pairs} new pairs.")

        if new_pairs == 0:
            break

        iteration += 1

    return rdd, iteration