import os
import ast

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
            if(colab_author != author_id): write_csv(author_rel_file, f"{colab_author}&collaborated&{article_doi}\n")


def analyse_article(article:dict):
    article_doi = article["DOI"][0] #DOI do artigo
    article_cite = article["Cited by"][0] #contagem de citações do artigo

    if len(article['Author(s) ID']) < 20 and len(article['Author(s) ID']) == len(article["Authors with affiliations"]): #evitando IndexError
        for i, author_id in enumerate(article['Author(s) ID']):
            try:
                author_id = str(author_id).strip()

                author_dir = os.path.join(AUTHORS_DIR, author_id)
                author_file = os.path.join(author_dir, f'{author_id}.csv')
                author_rel_file = os.path.join(author_dir, f'{author_id}_rel.csv')
                
                author_name = str(article["Authors"][i]).strip() #nome do autor na publicacao
                author_affiliation = "".join(article["Authors with affiliations"][i].split(",")[1:]).strip().replace("&", "and") #afiliação do autor
                author_type =  "National" if "Brazil" in author_affiliation else "International" #nacional ou internacional
                author_importance = str(i+1) #importancia do autor na autoria do artigo

                if not os.path.isdir(author_dir):
                    print(f'Creating folder for author {author_id}')            
                    os.makedirs(author_dir)            

                    #criando o arquivo author.csv
                    write_csv(author_file, "scopus_id&names&affiliation&type&DOIs&cite_counts&importance\n")
                    write_csv(author_file, f"{author_id}&{str([author_name])}&{author_affiliation}&{author_type}&{str([article_doi])}&{str([article_cite])}&{str([author_importance])}\n")

                    #criando o arquivo author_rel.csv
                    write_csv(author_rel_file, "author_id&type&article\n")            
                else:
                    print(f'Author {author_id} already exists')

                    file_content = read_csv(author_file).split('\n')[1]
                    file_content = file_content.split('&')
                    print(file_content)

                    current_names = ast.literal_eval(file_content[1])
                    current_dois = ast.literal_eval(file_content[4])
                    current_citations = ast.literal_eval(file_content[5])
                    current_importance = ast.literal_eval(file_content[6])

                    if not article_doi in current_dois: #então esse artigo desse autor ainda não foi analisado
                        if not author_name in current_names:
                            current_names.append(author_name)

                        author_affiliation = file_content[2]
                        author_type =  file_content[3] 
                        current_dois.append(article_doi)
                        current_citations.append(article_cite)
                        current_importance.append(author_importance)
                        
                        #adicionando as informações desse artigo no arquivo
                        write_replace_csv(author_file, "scopus_id&names&affiliation&type&DOIs&cite_counts&importance\n")
                        write_csv(author_file, f"{author_id}&{str(current_names)}&{author_affiliation}&{author_type}&{str(current_dois)}&{str(current_citations)}&{str(current_importance)}\n")
                    else:
                        print(f'Author`s article {article_doi} already included')
            except IndexError:
                print(len(article['Author(s) ID']), len(article["Authors with affiliations"]))

        create_collab_relationship(article)

def split_data(csv:str):
    splitted = csv.split('\n')
    columns = splitted[0].replace('\"',"").split(',')
    
    #print(f'columns: {columns}')
    
    for i, row in enumerate(splitted):
        if i > 0 and i < len(splitted)-1:
        #if i == 5:
            entries = row[1:len(row)-1].split('\",\"')
            dict = {columns[i]: entries[i].split(';') for i in range(len(columns))}
            
            analyse_article(dict) 
        

ic_csv = read_csv("./export-query3.csv")
split_data(ic_csv)