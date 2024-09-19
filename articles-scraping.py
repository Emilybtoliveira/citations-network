import os

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

def create_collab_relationship(article:dict):
    article_doi = article["DOI"][0] #DOI do artigo

    for author_id in article['Author(s) ID']:
        author_id = str(author_id).strip()

        author_dir = os.path.join(AUTHORS_DIR, author_id)
        author_rel_file = os.path.join(author_dir, f'{author_id}_rel.csv')

        for colab_author in article["Author(s) ID"]:
            colab_author = str(colab_author).strip()
            if(colab_author != author_id): write_csv(author_rel_file, f"{colab_author}&collaboration&{article_doi}\n")


def analyse_article(article:dict):
    article_doi = article["DOI"][0] #DOI do artigo
    article_cite = article["Cited by"][0] #contagem de citações do artigo

    for i, author_id in enumerate(article['Author(s) ID']):
        author_id = str(author_id).strip()

        author_dir = os.path.join(AUTHORS_DIR, author_id)
        author_file = os.path.join(author_dir, f'{author_id}.csv')
        author_rel_file = os.path.join(author_dir, f'{author_id}_rel.csv')
        
        author_names = str(article["Authors"][i]).strip() #nome do autor na publicacao
        author_affiliation = "".join(article["Authors with affiliations"][i].split(",")[1:]).strip() #afiliação do autor
        author_type =  "National" if "Brazil" in author_affiliation else "International" #nacional ou internacional
        author_importance = str(i+1) #importancia do autor na autoria do artigo

        if not os.path.isdir(author_dir):
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
            print(file_content)

            if not author_names in file_content[1]:
                author_names = file_content[1].replace("]", "") + f", '{author_names}']"
            else:
                author_names = file_content[1]

            author_affiliation = file_content[2]
            author_type =  file_content[3] 
            article_doi = file_content[4].replace(']', "") + f", '{article_doi}']"
            article_cite = file_content[5].replace(']', "") + f", '{article_cite}']"
            author_importance = file_content[6].replace(']', "") + f", '{author_importance}']"
            
            #adicionando as informações desse artigo no arquivo
            write_replace_csv(author_file, "scopus_id&names&affiliation&type&DOIs&cite_counts&importance\n")
            write_csv(author_file, f"{author_id}&{author_names}&{author_affiliation}&{author_type}&{article_doi}&{article_cite}&{author_importance}\n")

    create_collab_relationship(article)

def analyse_articles_references(article:dict):
    article_references = article["References"]
    
    for reference in article_references:
        reference_split = reference.split(',')
        if len(reference_split) > 1 :
            reference_year = str(reference_split[len(reference_split)-1]).strip()[1:-1]
            print(reference_split, reference_year)
            
            if int(reference_year) >= MINIMUM_ARTICLE_YEAR:
                print("yeah")


def split_data(csv:str):
    splitted = csv.split('\n')
    columns = splitted[0].replace('\"',"").split(',')
    
    print(f'columns: {columns}')
    
    for i, row in enumerate(splitted):
        if i == 1:
            entries = row[1:len(row)-1].split('\",\"')
            dict = {columns[i]: entries[i].split(';') for i in range(len(columns))}
            
            analyse_article(dict)
            #analyse_articles_references(dict)
   
        

ic_csv = read_csv("./export-query3.csv")
split_data(ic_csv)