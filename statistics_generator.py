import argparse
import sys
import csv
import neo4j
from neo4j import GraphDatabase
from collections import OrderedDict
from datetime import date, timedelta
import re

class Connector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def setQuery(self, query):
        self.query = query

    def getQuery(self):
        return connection.query

    def transaction_session(self):
        with self.driver.session() as session:
            query_result = session.write_transaction(connection.create_and_return_result)
            return query_result

    @staticmethod
    def create_and_return_result(tx: neo4j.Transaction):
        result = tx.run(connection.query)
        return [row for row in result]

def queries():
    #############################
    # INTERACTIONS DISTRIBUTION #
    #############################

    # language=cypher
    n_ary_query = "MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence) " \
                        "WITH i, count( * ) as n WHERE n > 1 " \
                        "RETURN date(dateTime(i.createdDate)) as date, count(i) as amount ORDER BY date"
    connection.setQuery(n_ary_query)
    n_ary_response = connection.transaction_session()

    # language=cypher
    binary_query = "MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence)" \
                              "RETURN date(dateTime(b.createdDate)) as date, count(DISTINCT b), count(DISTINCT i) ORDER BY date"
    connection.setQuery(binary_query)
    binary_response = connection.transaction_session()

    # language=cypher
    true_binary_query = "MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence) " \
                         "WITH i, count( * ) as n WHERE n = 1 " \
                         "RETURN date(dateTime(i.createdDate)) as date, count(i) as amount ORDER BY date"
    connection.setQuery(true_binary_query)
    true_bianry_response = connection.transaction_session()

    interaction_distribution_result = process_interactions(n_ary_response, binary_response, true_bianry_response)

    ##############################
    # PUBLICATIONS - EXPERIMENTS #
    ##############################
    # language=cypher
    publication_experiment_query = "MATCH (p:GraphPublication)-->(e:GraphExperiment)" \
                           "RETURN date(datetime(p.releasedDate)) as date, count(distinct p) as Publications, " \
                    "count(distinct e) as Experiments order by date"
    connection.setQuery(publication_experiment_query)
    publication_experiment_response = connection.transaction_session()
    publication_experiment_result = process_pub_exp(publication_experiment_response)

    ################################
    # CURATION SOURCE DISTRIBUTION #
    ################################
    # language=cypher
    curation_request_query = "Match (b:GraphBinaryInteractionEvidence)--(i:GraphInteractionEvidence)--(ex:GraphExperiment)--(p:GraphPublication)--(a:GraphAnnotation)-[:topic]-(c:GraphCvTerm{shortName:\"curation request\"})" \
                        "return date(dateTime(b.createdDate)) as date, count(distinct b) as evidence order by date"
    connection.setQuery(curation_request_query)
    curation_request_response = connection.transaction_session()

    # language=cypher
    author_submission_query = "Match (b:GraphBinaryInteractionEvidence)--(i:GraphInteractionEvidence)--(ex:GraphExperiment)--(p:GraphPublication)--(a:GraphAnnotation)-[:topic]-(c:GraphCvTerm{shortName:\"author submitted\"})" \
                        "return date(dateTime(b.createdDate)) as date, count(distinct b) as evidence order by date"
    connection.setQuery(author_submission_query)
    author_submission_response = connection.transaction_session()

    # language=cypher
    all_curations_query = "MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence) " \
                  "RETURN date(dateTime(b.createdDate)) as date, count(DISTINCT b) as evidence ORDER BY date"
    connection.setQuery(all_curations_query)
    all_curations_response = connection.transaction_session()

    curation_source_result = process_curations(curation_request_response, author_submission_response,
                                               all_curations_response)

    #######################
    # METHOD DISTRIBUTION #
    #######################
    # language=cypher
    method_distribution_query = "MATCH (o:GraphOrganism{taxId:9606})--(p:GraphProtein)--(b:GraphBinaryInteractionEvidence)-[:interactionEvidence]-(i:GraphInteractionEvidence)-[:experiment]-(ex:GraphExperiment)-[int:interactionDetectionMethod]-(c:GraphCvTerm)" \
                              "RETURN c.mIIdentifier as method, c.fullName as name,  count(distinct b) as evidence ORDER BY evidence desc"
    connection.setQuery(method_distribution_query)
    method_distribution_response = connection.transaction_session()
    method_distribution_result = process_methods(method_distribution_response)

    ########################
    # TOP 10 SPECIES COVER #
    ########################
    # language=cypher
    species_cover_query = "MATCH (o:GraphOrganism)--(ge:GraphProtein) " \
                          "WHERE Not ge.uniprotName contains \"-PRO\" " \
                          "RETURN count(distinct ge.uniprotName) as proteins, collect(DISTINCT ge.uniprotName) as upgene, o.scientificName as name ORDER BY proteins DESC  LIMIT 10 " \
                          "UNION "\
                                "MATCH (o:GraphOrganism{taxId:2697049})--(ge:GraphProtein)" \
                                "WHERE Not ge.uniprotName contains \"-PRO\" " \
                                "RETURN count(distinct ge.uniprotName) as proteins, collect(DISTINCT ge.uniprotName) as upgene, o.scientificName as name"
    connection.setQuery(species_cover_query)
    species_cover_response = connection.transaction_session()
    species_cover_result = process_proteome_coverage(species_cover_response)

    #################
    # SUMMARY TABLE #
    #################
    # language=cypher
    summary_table_query = "Match (c:GraphCvTerm) " \
                         "return \"Controlled Vocabulary Terms\" as name, count(distinct c) as amount " \
                         "UNION " \
                         "Match (p:GraphPublication) " \
                         "return \"Publications\" as name, count(distinct p) as amount " \
                         "UNION  " \
                         "Match (b:GraphBinaryInteractionEvidence) " \
                         "return \"Binary Interactions\" as name, count(distinct b) as amount " \
                         "UNION " \
                         "Match (i:GraphInteractor) " \
                         "return \"Interactors\" as name, count(distinct i) as amount " \
                         "UNION " \
                         "match (f:GraphFeature)-[t:type]-(c:GraphCvTerm) " \
                         "WHERE c.mIIdentifier in [\"MI:0118\", \"MI:0119\", \"MI:0573\", \"MI:1129\", \"MI:0429\", \"MI:1128\", \"MI:1133\", \"MI:1130\", \"MI:2333\", \"MI:0382\", \"MI:1132\", \"MI:1131\", \"MI:2226\", \"MI:2227\"] " \
                         "return \"Mutation features\" as name, count(distinct f) as amount " \
                         "UNION  " \
                         "Match (ie:GraphInteractionEvidence) " \
                         "return \"Interactions\" as name, count(distinct ie) as amount " \
                         "UNION " \
                         "Match (ex:GraphExperiment) " \
                         "return \"Experiments\" as name, count(distinct ex) as amount " \
                         "UNION  " \
                         "Match (o:GraphOrganism) " \
                         "return \"Organisms\" as name, count(distinct o) as amount " \
                         "UNION " \
                         "Match (ex:GraphExperiment)-[int:interactionDetectionMethod]-(c:GraphCvTerm) " \
                         "return \"Interaction Dectection Methods\" as name, count(distinct c.mIIdentifier) as amount " \
                         "UNION " \
                         "Match (ge:GraphGene) " \
                         "return \"Genes\" as name, count(distinct ge) as amount " \
                         "UNION " \
                         "Match (pro:GraphProtein) " \
                         "return \"Proteins\" as name, count(distinct pro) as amount " \
                         "UNION " \
                         "Match (nu:GraphNucleicAcid) " \
                         "return \"Nucleic Acids\" as name, count(distinct nu) as amount "
    connection.setQuery(summary_table_query)
    summary_table_response = connection.transaction_session()
    summary_table_result = process_summary_table(summary_table_response)

    return interaction_distribution_result, publication_experiment_result, curation_source_result, method_distribution_result, species_cover_result, summary_table_result

def process_interactions(n_ary_response, binary_response, true_binary_response):
    n_ary = binary = inter = true_binary = 0
    start_day = date(2003, 8, 1)
    delta = date.today() - start_day
    # n_ary_values = binary_values = true_binary_values = OrderedDict()
    interaction_data = OrderedDict(((start_day + timedelta(days=day)).isoformat(), [0, 0, 0, 0]) for day in range(delta.days + 1))

    for i_record in n_ary_response:
        n_ary += i_record[1]
        # n_ary_values[i_record[0].iso_format()] = n_ary
        interaction_data[i_record[0].iso_format()][0] = n_ary

    for b_record in binary_response:
        binary += b_record[1]
        inter += b_record[2]
        # binary_values[b_record[0].iso_format()] = [binary, inter]
        interaction_data[b_record[0].iso_format()][1] = binary
        interaction_data[b_record[0].iso_format()][2] = inter

    for tb_record in true_binary_response:
        true_binary += tb_record[1]
        # true_binary_values[tb_record[0].iso_format()] = true_binary
        interaction_data[tb_record[0].iso_format()][3] = true_binary

    with open('output_data/interactions.csv', 'w') as interactions_file:
        writer1 = csv.writer(interactions_file)
        writer1.writerow(['Date', 'ary_interactions_over_time', 'All_interactions_after_spoke_expension', 'All_interaction_reports', 'Binary_interaction_reports'])
        previous = [0, 0, 0, 0]
        for key, value in interaction_data.items():
            if value == [0, 0, 0, 0]:
                continue
            value = [value[i] if value[i] != 0 else previous[i] for i in range(len(value))]
            writer1.writerow([key, *value])
            previous = value

    return interaction_data

def process_pub_exp(publication_experiment_response):
    exp_pub_data = OrderedDict()
    publications = experiments = 0
    for pub_exp in publication_experiment_response:
        publications += pub_exp.values()[1]
        experiments += pub_exp.values()[2]
        exp_pub_data[pub_exp.values()[0].iso_format()] = [publications, experiments]

    with open('output_data/publication_experiment.csv', 'w') as publication_experiment_file:
        writer = csv.writer(publication_experiment_file)
        writer.writerow(['Date', 'Publications', 'Experiments'])
        for key, value in exp_pub_data.items():
            writer.writerow([key, *value])

    return exp_pub_data

def process_curations(curation_request_response, author_submission_response, all_curations_response):
    start_day = date(2003, 1, 1)
    delta = date.today() - start_day
    request = author = all = 0

    curation_data = OrderedDict(((start_day + timedelta(days=day)).isoformat(),
                                 [0, 0, 0]) for day in range(delta.days + 1))

    for request_record in curation_request_response:
        request += request_record[1]
        curation_data[request_record[0].iso_format()][0] = request

    for submitted_record in author_submission_response:
        author += submitted_record[1]
        curation_data[submitted_record[0].iso_format()][1] = author

    for all_record in all_curations_response:
        datum = all_record[0].iso_format()
        all += all_record[1]
        curation_data[datum][2] = all

    with open('output_data/curation_distribution.csv', 'w') as curation_distribution_file:
        writer3 = csv.writer(curation_distribution_file)
        writer3.writerow(['Date', 'Curation_requested_by_author', 'Author_submitted', 'Curator_choice/Funding_priority'])
        previous = [0, 0, 0]
        for key, value in curation_data.items():
            if value == [0, 0, 0]:
                continue
            value = [value[i] if value[i] != 0 else previous[i] for i in range(len(value))]
            value[2] -= (value[0] + value[1])
            writer3.writerow([key, *value])
            previous = value

    return curation_data

def process_methods(method_distribution_response):
    method_distribution_data = []
    for method in method_distribution_response:
        values = [i for i in method.values()]
        values[1] = method.values()[1].capitalize()
        method_distribution_data.append(values)

    with open('output_data/method_distribution_human_pub.csv', 'w') as method_distribution_file:
        writer = csv.writer(method_distribution_file)
        writer.writerow(['Method_ID', 'label', 'amount'])
        writer.writerows(method_distribution_data)

    return (method_distribution_data)

def process_proteome_coverage(species_cover_response):
    species_cover_data = OrderedDict()
    proteome_reference = {"Homo sapiens": [20360], "Mus musculus": [17085],
                          "Arabidopsis thaliana (Mouse-ear cress)": [16058],
                          "Saccharomyces cerevisiae": [6050],
                          "Escherichia coli (strain K12)": [4390],
                          "Drosophila melanogaster (Fruit fly)": [3625],
                          "Rattus norvegicus (Rat)": [8133],
                          "Caenorhabditis elegans": [4305],
                          "Synechocystis sp. (strain PCC 6803  Kazusa)": [1085],
                          "Campylobacter jejuni subsp. jejuni serotype O:2 (strain NCTC 11168)": [467],
                          "SARS-CoV-2": [16]}

    for organism in species_cover_response:
        organism_name = organism.values()[2].replace('/','')
        coverage = proteome_compare(organism.values()[1], f'reference_files/{organism_name}_uniprot.csv')
        species_cover_data[organism_name] = [coverage]
    for organism in species_cover_data.keys():
        percentage = species_cover_data[organism][0] / proteome_reference[organism][0] * 100
        proteome_reference[organism].append("{:.2f}".format(percentage))
        proteome_reference[organism].append(species_cover_data[organism][0])

    with open('output_data/species_cover.csv', 'w') as species_cover_file:
        writer = csv.writer(species_cover_file)
        writer.writerow(['Organism', 'Reference', 'Percentage', 'Proteins'])
        for key, value in proteome_reference.items():
            split = key.split(' ')
            short_name = f'{split[0]}' if split[0] == 'SARS-CoV-2' else f'{split[0]} {split[1]}' \
                if split[0] == 'Synechocystis' else f'{split[0][0]}. {split[1]}'
            full_data_list = value
            full_data_list.insert(0, short_name)
            writer.writerow(full_data_list)

    return (species_cover_data)

def process_summary_table(summary_table_response):
    summary_data = []
    for feature in summary_table_response:
        summary_data.append(feature.values())

    with open('output_data/summary_table.csv', 'w') as summary_table_file:
        writer = csv.writer(summary_table_file)
        writer.writerow(['Feature', 'Count'])
        writer.writerows(summary_data)
    return summary_data

def proteome_compare(result, reference):
    up_proteins = set()
    intact_proteins = set()

    with open(reference) as up_file:
        for line in up_file:
            up_proteins.add(line.replace('\ufeff', '').replace('\n', ''))

    for x in result:
        intact_proteins.add(re.sub('\-[0-9]', '', x).replace('\ufeff',''))

    return(len(intact_proteins.intersection(up_proteins)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', help='Provide the neo4j database to connect to.')
    parser.add_argument('--user', help='Provide the user name for the database connection.')
    parser.add_argument('--pw', help='Provide the password for the database connection.')
    args = parser.parse_args()
    print(args)
    # connection = Connector(args.database, args.user, args.pw)

    # "https://www.uniprot.org/uniprot/?query=proteome:UP000005640%20reviewed:yes&format=tab"
    # DATABASE = "bolt://intact-neo4j-001.ebi.ac.uk:7687"
    # USER = "neo4j"
    # PW = "neo4j123"
    # GIT_REP = "statistics_dev"
    # connection = Connector(DATABASE, USER, PW)


    # queries()
    # connection.close()
