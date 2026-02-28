from pyspark.rdd import portable_hash



def ccf_iterate_secondary_sort(rdd_edges, new_pair_accum):

  """
  Une itération de CCF en version "secondary sort".

  Entrée :
    rdd_edges : RDD[(u,v)] -> arretes du graphe
    new_pair_accum : accumulateur Spark (compteur) pour savoir si on crée des nouvelles paires

  Sortie :
    RDD[(node, label)] -> nouvelles paires produites durant l'itération

  """

  # ---------------------------------------------------------------------
  # 1) On crée un RDD de paires directionnelles :
  #    Pour chaque arête (a,b), on émet (a,b) ET (b,a).
  #
  #    Pourquoi ?
  #    - Parce que CCF traite le graphe comme non orienté.
  #    - Ainsi, chaque noeud "voit" ses voisins via des couples (u, voisin).
  # ---------------------------------------------------------------------
  # Note : on met ((u,v), None) pour forcer Spark à trier sur la clé (u,v), par la suite.
  # (clé composite = (u,v)). Le None ne sert à rien, c'est juste un placeholder.
  
  bothDirections = rdd_edges.flatMap(lambda e : [((e[0],e[1]),None),((e[1],e[0]),None)])

  # ---------------------------------------------------------------------
  # 2) Pour simuler le "secondary sort" :
  #
  #    On veut que Spark trie par (u puis v).
  #    Comme notre clé est (u,v), Spark peut trier.
  #
  #    MAIS : on veut que toutes les paires ayant le même u aillent
  #    dans la même partition ! (sinon u serait éclaté sur plusieurs partitions).
  #
  #    -> partitionFunc = hash(u) seulement. On change alors le hash function.
  # ---------------------------------------------------------------------
  
  def part_by_u(key):
    """
    key = (u, v)
    On retourne un hash basé uniquement sur u.
    """
    u, v = key
    return portable_hash(u)

  # ---------------------------------------------------------------------
  # 3) repartitionAndSortWithinPartitions :
  #
  #    - "repartition"  = SHUFFLE : Spark redistribue les données sur les partitions
  #      pour que toutes les clés avec même u se retrouvent ensemble.
  #
  #    - "sortWithinPartitions" = TRI LOCAL :
  #      Dans chaque partition, Spark trie les enregistrements par clé.
  #      Ici la clé = (u,v), donc tri d'abord sur u, puis sur v.
  #
  # Ce que ça garantit dans UNE partition :
  #   ((1,3),None), ((1,5),None), ((1,9),None), ((2,1),None), ((2,4),None), ...
  #
  # Attention :
  #   - Le tri est local à la partition, pas un tri global sur tout le RDD.
  # ---------------------------------------------------------------------

  sorted_pairs = bothDirections.repartitionAndSortWithinPartitions(
        numPartitions=bothDirections.getNumPartitions(),   # nb de partitions final
        partitionFunc=part_by_u # clé primaire = u
    )


  def ccf_iterate_reduce_for_secondary_sort(it):
    # Flux trié : (u1,v1),(u1,v2),(u1,v3),(u2,w1),...
      """
      Cette fonction réalise la phase "reduce" d'une itération CCF
      en version secondary sort.

      IMPORTANT :
      - On ne reçoit PAS un groupe déjà agrégé par clé.
      - On reçoit un FLUX trié dans chaque partition :
          (u1,v1), (u1,v2), (u1,v3), (u2,v1), (u2,v4), ...

      Grâce à repartitionAndSortWithinPartitions :
        - Toutes les paires ayant le même u sont dans la même partition.
        - Elles sont triées par (u puis v).
        - Donc pour chaque u, le PREMIER v rencontré est le MINIMUM voisin.

      Logique CCF :
        Pour chaque noeud u :
          - min_v = plus petit voisin (premier v rencontré)
          - Si min_v < u :
                -> on émet (u, min_v)
                -> pour chaque autre voisin v != min_v :
                      on émet (v, min_v)
          - Sinon (min_v >= u) :
                -> aucune émission (le composant est déjà stable pour ce noeud)
      """


      cur_u = None # noeud courant
      min_v = None # plus petit voisin de cur_u
      flag_can_ignore = False # indique si on peut ignorer l’émission pour ce u
      flag_new_pair = False # indique si on a émis au moins une paire dans cette partition

      for (u, v), _ in it:

          if u != cur_u:
            # Nouveau groupe u : le premier v est le minimum (grâce au tri par (u,v))
            cur_u = u
            min_v = v
            if cur_u <= min_v :
              flag_can_ignore = True
            else:
              flag_can_ignore = False
              yield (cur_u, min_v)

          else:
            if flag_can_ignore :
              continue
            elif v != min_v:
              yield (v, min_v)
              new_pair_accum.add(1)


  # mapPartitions permet de traiter toute une partition d’un coup.
  # On en a besoin car les (u,v) arrivent triés par u,
  # et on doit parcourir le flux pour détecter les changements de u.
  # Avec map(), on traiterait un seul élément à la fois.
  return sorted_pairs.mapPartitions(ccf_iterate_reduce_for_secondary_sort), None


