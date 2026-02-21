import networkx as nx
import os


# On veut générer des graphes de tailles croissantes.
# Sauf qu'on ne peut pas les générer complètement au hasard, au risque de perdre l'interprétabilité
# Si les graphes ne ressemblent pas aux données réelles (pas de liens, peu de sommets), l'évaluation en sera faussée

def generate_small_world(num_nodes, num_edges, output_dir="data"):

    # On utilise le modèle de Barabási-Albert
    m = max(1, num_edges // num_nodes)

    G = nx.barabasi_albert_graph(num_nodes, m)

    output_file_name = f"{output_dir}/SmallWorld_{num_nodes}_{num_edges}"
    output_path = f"{output_file_name}.txt"

    with open(output_path, 'w') as f:
        for u, v in G.edges():
            f.write(f"{u}\t{v}\n")
    print(f"Small world graph was saved here : {output_path}")


# On ne teste pas qu'avec des graphes sensiblement similaires à des données réelles
# On génère également un cas pathologique. Selon l'article :

# "Worst case scenario for the number of necessary iterations
# is d+1 where d is the diameter of the network. The worst
# case happens when the min node in the largest connected
# component is an end-point of the largest shortest-path."

# Notre idée ici est donc d'évaluer l'algo avec un graphe au diamètre élevé

def generate_line_graph(num_nodes, output_dir='data'):

    output_file_name = f"{output_dir}/LineGraph_{num_nodes}"

    output_path = f"{output_file_name}.txt"
    with open(output_path, 'w') as f:
        for i in range(1, num_nodes):
            f.write(f"{i}\t{i + 1}\n")

    print(f"Line graph was saved here: {output_path}")


def load_graph_to_rdd(sc, file_path):

    raw_rdd = sc.textFile(file_path)

    filtered_rdd = raw_rdd.filter(lambda line: len(line.strip()) > 0 and not line.startswith('#'))

    def parse_line(line):
        edge = line.split()
        return int(edge[0]), int(edge[1])

    return filtered_rdd.map(parse_line)


if __name__ == '__main__':
    print("Generating graphs")