import argparse
import csv
import re
import urllib.request
from collections import OrderedDict
from datetime import date, timedelta
from typing import Union, Dict, List

from neo4j import GraphDatabase


class Connector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.query = ""

    def close(self):
        self.driver.close()

    def transaction_session(self):
        with self.driver.session() as session:
            query_result = session.write_transaction(lambda tx: [row for row in tx.run(self.query)])
            return query_result


def queries(c: Connector):
    #############################
    # INTERACTIONS DISTRIBUTION #
    #############################

    # language=cypher
    n_ary_query = '''
    MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence)
    WITH i, COUNT(*) AS n
      WHERE n > 1
    RETURN date(dateTime(i.createdDate)) AS date, count(i) AS amount
      ORDER BY date
  '''
    c.query = n_ary_query
    n_ary_response = c.transaction_session()

    # language=cypher
    binary_query = '''
    MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence)
    RETURN date(dateTime(b.createdDate)) AS date, count(DISTINCT b), count(DISTINCT i) 
      ORDER BY date
'''
    c.query = binary_query
    binary_response = c.transaction_session()

    # language=cypher
    true_binary_query = '''
    MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence)
    WITH i, COUNT(*) AS n
      WHERE n = 1
    RETURN date(dateTime(i.createdDate)) AS date, count(i) AS amount
      ORDER BY date
    '''
    c.query = true_binary_query
    true_binary_response = c.transaction_session()

    interaction_distribution_result = process_interactions(n_ary_response, binary_response, true_binary_response)

    ##############################
    # PUBLICATIONS - EXPERIMENTS #
    ##############################
    # language=cypher
    publication_experiment_query = '''
    MATCH (p:GraphPublication)-->(e:GraphExperiment)
    RETURN date(datetime(p.releasedDate)) AS date, count(DISTINCT p) AS Publications, count(DISTINCT e) AS Experiments
        ORDER BY date
    '''
    c.query = publication_experiment_query
    publication_experiment_response = c.transaction_session()
    publication_experiment_result = process_pub_exp(publication_experiment_response)

    ################################
    # CURATION SOURCE DISTRIBUTION #
    ################################
    # language=cypher
    curation_request_query = '''
    MATCH(b:GraphBinaryInteractionEvidence)--(i:GraphInteractionEvidence)--(ex:GraphExperiment)--(p:GraphPublication)
      --(a:GraphAnnotation)-[:topic]-(c:GraphCvTerm {shortName: 'curation request'})
    RETURN date(dateTime(b.createdDate)) AS date, count(DISTINCT b) AS evidence
      ORDER BY date
    '''
    c.query = curation_request_query
    curation_request_response = c.transaction_session()

    # language=cypher
    author_submission_query = '''
    MATCH(b:GraphBinaryInteractionEvidence)--(i:GraphInteractionEvidence)--(ex:GraphExperiment)--(p:GraphPublication)
         --(a:GraphAnnotation)-[:topic]-(c:GraphCvTerm {shortName: 'author submitted'})
    RETURN date(dateTime(b.createdDate)) AS date, count(DISTINCT b) AS evidence
      ORDER BY date
    '''
    c.query = author_submission_query
    author_submission_response = c.transaction_session()

    # language=cypher
    all_curations_query = '''
    MATCH (i:GraphInteractionEvidence)--(b:GraphBinaryInteractionEvidence)
    RETURN date(dateTime(b.createdDate)) AS date, count(DISTINCT b), count(DISTINCT i)
      ORDER BY date
    '''
    c.query = all_curations_query
    all_curations_response = c.transaction_session()

    curation_source_result = process_curations(curation_request_response, author_submission_response,
                                               all_curations_response)

    #######################
    # METHOD DISTRIBUTION #
    #######################
    # language=cypher
    method_distribution_query = '''
    MATCH(b:GraphBinaryInteractionEvidence)-[:interactionEvidence]-(i:GraphInteractionEvidence)
           -[:experiment]-(ex:GraphExperiment)-[int:interactionDetectionMethod]-(c:GraphCvTerm)
    RETURN c.mIIdentifier AS method, c.fullName AS name, count(DISTINCT b) AS evidence
      ORDER BY evidence DESC
    '''
    c.query = method_distribution_query
    method_distribution_response = c.transaction_session()
    method_distribution_result = process_methods(method_distribution_response)

    #######################
    # TOP 10 SPECIES COVER #
    #######################
    # language=cypher
    species_cover_query = '''
    MATCH (o:GraphOrganism)--(ge:GraphProtein)
      WHERE NOT ge.uniprotName CONTAINS '-PRO'
    RETURN count(DISTINCT ge.uniprotName) AS proteins, collect(DISTINCT ge.uniprotName) AS upGene,
        o.scientificName AS name
      ORDER BY proteins DESC
      LIMIT 10 UNION
    MATCH (o:GraphOrganism {taxId: 2697049})--(ge:GraphProtein)
      WHERE NOT ge.uniprotName CONTAINS '-PRO'
    RETURN count(DISTINCT ge.uniprotName) AS proteins, collect(DISTINCT ge.uniprotName) AS upGene,
        o.scientificName AS name
    '''
    c.query = species_cover_query
    species_cover_response = c.transaction_session()
    species_cover_result = process_proteome_coverage(species_cover_response)

    #################
    # SUMMARY TABLE #
    #################
    # language=cypher
    summary_table_query = '''
    MATCH (c:GraphCvTerm)
    RETURN 'Controlled Vocabulary Terms' AS name, count(DISTINCT c) AS amount UNION
    MATCH (p:GraphPublication)
    RETURN 'Publications' AS name, count(DISTINCT p) AS amount UNION
    MATCH (b:GraphBinaryInteractionEvidence)
    RETURN 'Binary Interactions' AS name, count(DISTINCT b) AS amount UNION
    MATCH (i:GraphInteractor)
    RETURN 'Interactors' AS name, count(DISTINCT i) AS amount UNION
    MATCH (f:GraphFeature)-[t:type]-(c:GraphCvTerm)
      WHERE c.mIIdentifier IN ['MI:0118', 'MI:0119', 'MI:0573', 'MI:1129', 'MI:0429', 'MI:1128', 'MI:1133', 'MI:1130',
        'MI:2333', 'MI:0382', 'MI:1132', 'MI:1131', 'MI:2226', 'MI:2227']
    RETURN 'Mutation features' AS name, count(DISTINCT f) AS amount UNION
    MATCH (ie:GraphInteractionEvidence)
    RETURN 'Interactions' AS name, count(DISTINCT ie) AS amount UNION
    MATCH (ex:GraphExperiment)
    RETURN 'Experiments' AS name, count(DISTINCT ex) AS amount UNION
    MATCH (o:GraphOrganism)
    RETURN 'Organisms' AS name, count(DISTINCT o) AS amount UNION
    MATCH (ex:GraphExperiment)-[int:interactionDetectionMethod]-(c:GraphCvTerm)
    RETURN 'Interaction Dectection Methods' AS name, count(DISTINCT c.mIIdentifier) AS amount UNION
    MATCH (ge:GraphGene)
    RETURN 'Genes' AS name, count(DISTINCT ge) AS amount UNION
    MATCH (pro:GraphProtein)
    RETURN 'Proteins' AS name, count(DISTINCT pro) AS amount UNION
    MATCH (nu:GraphNucleicAcid)
    RETURN 'Nucleic Acids' AS name, count(DISTINCT nu) AS amount 
    '''
    c.query = summary_table_query
    summary_table_response = c.transaction_session()
    summary_table_result = process_summary_table(summary_table_response)

    return interaction_distribution_result, publication_experiment_result, curation_source_result, \
           method_distribution_result, species_cover_result, summary_table_result


def process_interactions(n_ary_response, binary_response, true_binary_response):
    n_ary = binary = inter = true_binary = 0
    start_day = date(2003, 8, 1)
    delta = date.today() - start_day
    interaction_data = OrderedDict(
        ((start_day + timedelta(days=day)).isoformat(), [0, 0, 0, 0]) for day in range(delta.days + 1))

    for i_record in n_ary_response:
        n_ary += i_record[1]
        interaction_data[i_record[0].iso_format()][0] = n_ary

    for b_record in binary_response:
        binary += b_record[1]
        inter += b_record[2]
        interaction_data[b_record[0].iso_format()][1] = binary
        interaction_data[b_record[0].iso_format()][2] = inter

    for tb_record in true_binary_response:
        true_binary += tb_record[1]
        interaction_data[tb_record[0].iso_format()][3] = true_binary

    with open('output_data/interactions.csv', 'w') as interactions_file:
        writer1 = csv.writer(interactions_file)
        writer1.writerow(
            ['Date', 'ary_interactions_over_time', 'All_interactions_after_spoke_expansion', 'All_interaction_reports',
             'Binary_interaction_reports'])
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
    request = author = total = 0

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
        total += all_record[1]
        curation_data[datum][2] = total

    with open('output_data/curation_distribution.csv', 'w') as curation_distribution_file:
        writer3 = csv.writer(curation_distribution_file)
        writer3.writerow(
            ['Date', 'Curation_requested_by_author', 'Author_submitted', 'Curator_choice/Funding_priority'])
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

    with open('output_data/method_distribution.csv', 'w') as method_distribution_file:
        writer = csv.writer(method_distribution_file)
        writer.writerow(['Method_ID', 'label', 'amount'])
        writer.writerows(method_distribution_data)

    return method_distribution_data


def process_proteome_coverage(species_cover_response):
    species_cover_data = OrderedDict()
    proteome_reference: Dict[str, List[Union[str, int]]] = {}

    for organism in species_cover_response:
        organism_name = organism.values()[2].replace('/', '')
        reference = reference_proteome(organism_name)
        proteome_reference[organism_name] = [len(reference)]
        coverage = proteome_compare(organism.values()[1], reference)
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

    return species_cover_data


def process_summary_table(summary_table_response):
    summary_data = []
    for feature in summary_table_response:
        summary_data.append(feature.values())

    with open('output_data/summary_table.csv', 'w') as summary_table_file:
        writer = csv.writer(summary_table_file)
        writer.writerow(['Feature', 'Count'])
        writer.writerows(summary_data)
    return summary_data


def reference_proteome(organism):
    species_to_proteome_id = {"Homo sapiens": 'UP000005640', "Mus musculus": 'UP000000589',
                              "Arabidopsis thaliana (Mouse-ear cress)": 'UP000006548',
                              "Saccharomyces cerevisiae": 'UP000002311',
                              "Escherichia coli (strain K12)": 'UP000000625',
                              "Drosophila melanogaster (Fruit fly)": 'UP000000803',
                              "Rattus norvegicus (Rat)": 'UP000002494',
                              "Caenorhabditis elegans": 'UP000001940',
                              "Synechocystis sp. (strain PCC 6803  Kazusa)": 'UP000001425',
                              "Campylobacter jejuni subsp. jejuni serotype O:2 (strain NCTC 11168)": 'UP000000799',
                              "SARS-CoV-2": 'UP000464024'}
    proteome_id = species_to_proteome_id[organism]
    with urllib.request.urlopen(
            f'https://www.uniprot.org/uniprot/?query=proteome:{proteome_id}%20reviewed:yes&format=tab') as url_file:
        proteins = [i.decode('utf-8').split('\t')[0] for i in url_file]
        proteins.pop(0)
    return proteins


def proteome_compare(result, reference):
    up_proteins = set(reference)
    intact_proteins = set()

    for x in result:
        intact_proteins.add(re.sub('-[0-9]', '', x).replace('\ufeff', ''))

    return len(intact_proteins.intersection(up_proteins))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', help='Provide the neo4j database to connect to.')
    parser.add_argument('--user', help='Provide the user name for the database connection.')
    parser.add_argument('--pw', help='Provide the password for the database connection.')
    args = parser.parse_args()
    connection = Connector(args.database, args.user, args.pw)
    queries(connection)
    connection.close()
