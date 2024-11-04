import os
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import re

""" 
COLUMNS: 
    'Authors', 
    'Author full names', (references nao tem)
    'Author(s) ID', 
    'Title', 
    'Year', 
    'Cited by', 
    'DOI', 
    'Link', 
    'Affiliations', 
    'Authors with affiliations', 
    'References' 
"""
load_dotenv()

AUTHORS_DIR = "./authors"
MINIMUM_ARTICLE_YEAR = 2022
MAXIMUM_ARTICLE_YEAR = 2025
SCOPUS_API_KEY = os.getenv('SCOPUS_API_KEY')

yes, no, error, scopus_error = 0,0,0,0
new_authors = 0

def read_csv(path):
    with open(path, "r",  encoding='utf-8-sig') as file:
        csv = file.read()
        file.close()
    return csv

def write_csv(path,line):
    try:
        with open(path, "a") as file:
            file.write(line)
            file.close()
    except FileNotFoundError:
        print(f"Arquivo {path} não encontrado.")
        print(line)

def write_replace_csv(path,line):
    with open(path, "w") as file:
        file.write(line)
        file.close()

def create_citation_relationship(article_doi:str, cited_authors:dict, citing_article:dict):
    for cited_author in cited_authors:
        for citing_author in citing_article['Author(s) ID']:
            cited_author_id = cited_author['scopus_id']
            cited_author_dir = os.path.join(AUTHORS_DIR, cited_author_id)
            cited_author_rel_file = os.path.join(cited_author_dir, f'{cited_author_id}_rel.csv')

            citing_author_id = citing_author.strip()
            citing_author_dir = os.path.join(AUTHORS_DIR, citing_author_id)
            citing_author_rel_file = os.path.join(citing_author_dir, f'{citing_author_id}_rel.csv')

            if(cited_author_id != citing_author_id): #evitando autorelacionamento
                write_csv(cited_author_rel_file, f"{citing_author_id}&was cited&{article_doi}\n") #escreve no arquivo do autor citado que outro autor o citou
                write_csv(citing_author_rel_file, f"{cited_author_id}&cites&{article_doi}\n") #escreve no arquivo do autor que citou que ele cita o outro autor

def create_collab_relationship(article_doi:str, authors:dict):
    for author in authors:
        author_id = str(author['scopus_id']).strip()

        author_dir = os.path.join(AUTHORS_DIR, author_id)
        author_rel_file = os.path.join(author_dir, f'{author_id}_rel.csv')

        for colab_author in authors:
            colab_author = str(colab_author['scopus_id']).strip()
            if(colab_author != author_id): write_csv(author_rel_file, f"{colab_author}&collaborated&{article_doi}\n")

def generate_authors_files(article_doi:str, article_cite:str, authors:dict, citing_article:dict):
    global new_authors

    for author in authors:
        author_id = author['scopus_id']
        author_dir = os.path.join(AUTHORS_DIR, author_id)
        author_file = os.path.join(author_dir, f'{author_id}.csv')
        author_rel_file = os.path.join(author_dir, f'{author_id}_rel.csv')
        
        author_names = str(author['name']).strip() #nome do autor na publicacao
        author_affiliation = author['affiliation'].strip() #afiliação do autor
        author_type =  author['type'] #nacional ou internacional
        author_importance = author['importance'] #importancia do autor na autoria do artigo

        if not os.path.isdir(author_dir):
            new_authors += 1

            print(f'Creating folder for author {author_id}')            
            os.makedirs(author_dir)            

            #criando o arquivo author.csv
            write_csv(author_file, "scopus_id&names&affiliation&type&DOIs&cite_counts&importance\n")
            write_csv(author_file, f"{author_id}&{str([author_names])}&{author_affiliation}&{author_type}&{str([article_doi])}&{str([article_cite])}&{str([author_importance])}\n")

            #criando o arquivo author_rel.csv
            write_csv(author_rel_file, "author_id&type&article\n")            
        else:
            print(f'Author {author_id} already exists')

            file_content = read_csv(author_file).split('\n')[1]
            file_content = file_content.split('&')
            #print(file_content)

            if not article_doi in file_content[4]: #então esse artigo desse autor ainda não foi analisado
                if not author_names in file_content[1]:
                    new_author_names = file_content[1].replace("]", "") + f", '{author_names}']"
                else:
                    new_author_names = file_content[1]

                author_affiliation = file_content[2]
                author_type =  file_content[3] 
                new_article_doi = file_content[4].replace(']', "") + f", '{article_doi}']"
                new_article_cite = file_content[5].replace(']', "") + f", '{article_cite}']"
                new_author_importance = file_content[6].replace(']', "") + f", '{author_importance}']"
                
                #adicionando as informações desse artigo no arquivo
                write_replace_csv(author_file, "scopus_id&names&affiliation&type&DOIs&cite_counts&importance\n")
                write_csv(author_file, f"{author_id}&{new_author_names}&{author_affiliation}&{author_type}&{new_article_doi}&{new_article_cite}&{new_author_importance}\n")
            else:
                print(f'Author`s article {article_doi} already included')

    create_collab_relationship(article_doi, authors)
    create_citation_relationship(article_doi, authors, citing_article)

def get_affiliation(affiliation_name):
    response = requests.get(url=f"https://api.ror.org/organizations?affiliation={affiliation_name}")

    json = response.json()

    try:
        if(len(json["items"]) > 0):
            country = json["items"][0]["organization"]["country"]
            #print(country)
        else:
            raise KeyError
        return country
    except KeyError: #afiliacao nao foi encontrada
        print(f"Afiliação {affiliation_name} não encontrada na API.")
        return ""

def get_article(article_name):
    global scopus_error
    response = requests.get(url=f"https://api.elsevier.com/content/search/scopus?query=TITLE({article_name})", 
                            headers={'X-ELS-APIKey': SCOPUS_API_KEY})
    
    try:
        article_doi = response.json()["search-results"]['entry'][0]['prism:doi']
        article_citations = response.json()["search-results"]['entry'][0]['citedby-count']
        links = response.json()["search-results"]['entry'][0]['link']
        authors_link = links[1]["@href"]

        print(article_doi, authors_link)
        response_authors = requests.get(url=f"{authors_link}", 
                            headers={'X-ELS-APIKey': SCOPUS_API_KEY})        
        #print(response_authors.content)
        
        root = ET.fromstring(response_authors.content)
        namespaces = {
            'default': 'http://www.elsevier.com/xml/svapi/abstract/dtd',
            'ce': 'http://www.elsevier.com/xml/ani/common'
        }
 
        affiliations = {}
        for affiliation in root.findall('default:affiliation', namespaces):
            affil_id = affiliation.attrib['id']
            affil_name = affiliation.find('default:affilname', namespaces).text
            
            affiliations[affil_id] = {
                'name': affil_name,
            }
        
        #print(affiliations)

        authors = []
        for i, author in enumerate(root.findall('default:authors/default:author', namespaces)):
            auid = author.attrib['auid']
            indexed_name = author.find('ce:indexed-name', namespaces).text
            affiliation_id = author.find('default:affiliation', namespaces).attrib['id']
            author_affiliation = affiliations.get(affiliation_id, {})
            country_affiliation = get_affiliation(author_affiliation['name'])

            authors.append({
                'scopus_id': auid,
                'name': indexed_name,
                'affiliation': author_affiliation['name'],
                'type': "National" if country_affiliation == "Brazil" else "International",
                'importance': str(i+1)
            })            

        #print(authors)
        return article_doi, article_citations, authors
    except AttributeError:
        print(f"Artigo {article_name} foi retornado incompleto pelo scopus.")
        scopus_error += 1
        return None, None, None
    except KeyError: #artigo nao foi encontrado
        print(f"Artigo {article_name} não encontrado no Scopus.")
        scopus_error += 1
        return None, None, None
    except Exception:
        print(f"Random exception!")
        error += 1
        return None, None, None


def analyse_articles_references(article:dict):
    article_references = article["References"]
    global yes, no, error
    
    if len(article['Author(s) ID']) == len(article["Authors with affiliations"]): #evitando IndexError
        for reference in article_references:
            reference_split = reference.split(',')
            if len(reference_split) > 2 : #tem que ser pelo menos [author, title, year]
                try:
                    # print(reference_split)
                    reference_year = 0
                    # reference_year = str(reference_split[len(reference_split)-1]).strip()[1:-1]
                    match = re.search(r"\((\d{4})\)", reference)
                    if match: reference_year = match.group(1)
                    #print(reference_year)

                    if int(reference_year) > MINIMUM_ARTICLE_YEAR and int(reference_year) < MAXIMUM_ARTICLE_YEAR:
                        #print("yeah")
                        #print(reference_split, reference_year)  
                        yes += 1  
                        for item in reference_split:
                            #print(item[-1])
                            if(item[-1] != '.'):
                                #print(item)
                                
                                # article_doi, article_cite, authors = get_article(item)
                                # if authors != None:
                                #     generate_authors_files(article_doi, article_cite, authors, article)                            
                                break                            
                    else:
                        no += 1  
                except ValueError: #não tem ano
                    #print(f"Erro na referência: {reference_split}")
                    error += 1     
                    continue
            else:
                no += 1  

def split_data(csv:str):
    splitted = csv.split('\n')
    columns = splitted[0].replace('\"',"").split(',')
    
    #print(f'columns: {columns}')
    
    for i, row in enumerate(splitted):
        if i > 0 and (i < len(splitted)-1):
        # if i > 0 and i < 2:
            try:
                print(f"{i}th article")
                #print(row)
                entries = row[1:len(row)-1].split('\",\"')
                #print(entries)
                dict = {columns[i]: entries[i].split(';') for i in range(len(columns))}
                #print(dict)
                analyse_articles_references(dict)    
            except IndexError as exc:
                print("INDEX ERROR: ", exc) 
    
    print(f"saldo: yes {yes} no {no} error {error} scopus error {scopus_error}")    
    print(f"created {new_authors} new authors")     
           
ic_csv = read_csv("./export-query4-references.csv")
split_data(ic_csv)