import os
import requests
import xml.etree.ElementTree as ET

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

AUTHORS_DIR = "./authors"
MINIMUM_ARTICLE_YEAR = 2019
SCOPUS_API_KEY = "be20c8511c99fdbff7c5ea8b9c53803f"
global yes, no, error, scopus_error
yes, no, error, scopus_error = 0,0,0,0


def read_csv(path):
    with open(path, "r",  encoding='utf-8-sig') as file:
        csv = file.read()
        file.close()
    return csv

def write_csv(path,line):
    with open(path, "a") as file:
        file.write(line)
        file.close()

def write_replace_csv(path,line):
    with open(path, "w") as file:
        file.write(line)
        file.close()

def get_affiliation(affiliation_name):
    response = requests.get(url=f"https://api.ror.org/organizations?affiliation={affiliation_name}")

    json = response.json()

    try:
        country = json["items"][0]["organization"]["country"]
        #print(country)
        return country
    except KeyError: #afiliacao n foi encontrada
        print(f"Afiliação {affiliation_name} não encontrada na API.")
        return None

def get_article(article_name):
    global scopus_error
    response = requests.get(url=f"https://api.elsevier.com/content/search/scopus?query=TITLE({article_name})", 
                            headers={'X-ELS-APIKey': SCOPUS_API_KEY})
    
    try:
        article_doi = response.json()["search-results"]['entry'][0]['prism:doi']
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

        print(authors)

        return article_doi, authors
    except KeyError: #artigo nao foi encontrado
        print(f"Artigo {article_name} não encontrado no Scopus.")
        scopus_error += 1
        return

def analyse_articles_references(article:dict):
    article_references = article["References"]
    global yes, no, error
    
    for reference in article_references:
        reference_split = reference.split(',')
        if len(reference_split) > 2 : #tem que ser pelo menos [author, title, year]
            try:
                reference_year = str(reference_split[len(reference_split)-1]).strip()[1:-1]
                
                if int(reference_year) > MINIMUM_ARTICLE_YEAR:
                    #print("yeah")
                    #print(article["Year"])
                    #print(reference_split, reference_year)  
                    yes += 1  
                    for item in reference_split:
                        #print(item[-1])
                        if(item[-1] != '.'):
                            print(item)
                            get_article(item)
                            return
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
        #if i > 0 and i < len(splitted)-1:
        if i == 5:
            entries = row[1:len(row)-1].split('\",\"')
            dict = {columns[i]: entries[i].split(';') for i in range(len(columns))}
            #print(dict)
            analyse_articles_references(dict)
            
    
    print(f"saldo: yes {yes} no {no} error {error} scopus error {scopus_error}")         
   
        
ic_csv = read_csv("./export-query3.csv")
split_data(ic_csv)