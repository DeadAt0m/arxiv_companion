import os
import arxiv
import pickle
import time
import tqdm
import random
import re
#pip install python-Levenshtein
from Levenshtein import distance



    
class ArxivArxiv(object):
    def __init__(self,path='./arxiv_local/arxiv.db'):
        self.reload_database(path)
        self.set_download_path()
        
    def _check_db_existence(self,path):
        res = os.path.exists(path)
        self.empty_db_flag = not res
        if res:
            try:
                db_file = open(path, 'rb')
                self.db = pickle.load(db_file)
                db_file.close()
                print('Database load successefuly from ',path,' and contains {0} elements'.format(len(self.db)))
            except:
                print('Something went wrong during database load.',
                      'Local database assumed empty.',
                      'Please reload or import from file.')     
        else:
            print(path,' does not exists!')
            
    def reload_database(self,path):
        self.db_path = path
        self.db = {}
        self.empty_db_flag = True
        self._check_db_existence(path)
        
        
        
    def set_download_path(self,path='./arxiv_local/downloads'):
        os.makedirs(path, exist_ok=True)
        self.download_path = path if path[-1] == '/' else path+'/'
        print('Download path set to ',self.download_path)
        
    def import_from_file(self,path):
        def extract_id_from_string(str_):
            find_url_start = str_.find('href=') + 6
            find_url_end = str_.find(' ',find_url_start) - 1
            url = str_[find_url_start:find_url_end]
            id_start = url.rfind('/') + 1
            return url[id_start:]
        def _check_id(str_):
            r = re.compile('([0-9]{4})\.([0-9]{5,7})')
            return bool(r.match(str_)) 
        
        if not os.path.exists(path):
            print('File {0} does not exists'.format(path))
        else:
            html_list = []
            with open(path) as fin:
                for line in fin:
                    if not 'bioarxiv' in line and 'arxiv' in line:
                        html_list.append(line)
            id_list = [extract_id_from_string(s) for s in html_list if s.find('href=') > -1 and _check_id(extract_id_from_string(s))]          
            print('File {0} successefuly loaded.'.format(path))
            id_list = list(set(id_list) - set(self.db.keys()))            
            self._add_to_db(id_list)
            
    def import_from_id_list(self,id_list):
        if id_list:
            self._add_to_db(id_list)
            
    def _add_to_db(self, id_list):
        print(f'Make {len(id_list)} queries to arxiv.org. Can take a while...')
        papers_meta = []
        for _id in tqdm.tqdm(id_list):
            try:
                temp = arxiv.query(id_list = [_id])
                #               if len(temp) > 1:
# 			        print('[Debug] Strange query: ',temp)
                papers_meta.append(temp[0])
            except:
                 print(_id)
     
        print('Something went wrong during queries. {0}% of articles is missing'.format(int(100*(1-len(papers_meta)/len(id_list)))))
        print('[Debug] ID_list length: {0}, Query length: {1}'.format(len(id_list),len(papers_meta)))
        print(f'Queried: {list(map(lambda x: x["id"].split("/")[-1],papers_meta))}\nFull: {id_list}')
        print('Adding to exiting database...', end=' ')
        local_db = {}
        for idx,meta_info in enumerate(papers_meta):
            temp_dict = {}
            temp_dict['authors'] = meta_info['authors']
            temp_dict['title'] = self._correct_article_title(meta_info)
            temp_dict['pdf_url'] = meta_info['pdf_url']
            temp_dict['date'] = meta_info['published_parsed']
            local_db[id_list[idx]] = temp_dict
        self.db = {**self.db, **local_db}
        print('Done.')
        self._save_db()
        del local_db          
            
        
    def _save_db(self):
        path_to_check, filename = os.path.split(self.db_path)
        if path_to_check:
            os.makedirs(path_to_check, exist_ok=True)
        db_file = open(self.db_path, 'wb')
        pickle.dump(self.db, db_file, 2)
        db_file.close()
        self.empty_db_flag = not bool(self.db)
        print('Database was saved in ',self.db_path)

    def _correct_article_title(self,query):
        title = query['title']
        title = title.replace(':', '.')
        title = title.replace('\t', ' ')
        title = title.replace('\n', '')
        title = title.replace('/', ' ')
        return title[:-1] if title[-1] == '.' else title

    def _extract_date(self,query,need_version=True):
        year = time.strftime('%Y',query['date'])
        version = 'v'
        dot_pos = query['pdf_url'].rfind('.')
        v_pos = query['pdf_url'].rfind('v')
        if v_pos > dot_pos:
            version += query['pdf_url'][v_pos+1]
        else:
            version += '1'
        return '(' + year + (version if need_version else '') +'). '
    
    def _standartize_author(self,str_):
        def get_capital_indices(s):
            return [i for i, c in enumerate(s) if c.isupper()]
        surname = str_[str_.rfind(" ") + 1:]
        first_capital_letter = get_capital_indices(str_)[0]
        return surname + ', ' + str_[first_capital_letter]+'.'
        
    def _extract_authors(self,query):
        authors_list = query['authors']
        authors_string = ''
        for i,author in enumerate(authors_list):
            if i < 2:
                authors_string += self._standartize_author(author) + ', '
            else:
                authors_string += self._standartize_author(authors_list[-1]) + ', etc. '
                break
        return authors_string        
            
        
    def prepare_for_download(self):
        if self.empty_db_flag:
            print('Database is empty! Cannot download anything!')
        else:
            print('Checking download path...')
            dir_stamp = os.listdir(self.download_path)
            existing_articles = [elem for elem in dir_stamp  if '.pdf' in elem]
            if existing_articles:
                existing_articles = [(elem[elem.find(').')+3:-4],'v'+elem[elem.find(').')-1]) for elem in existing_articles]
            self.to_download = []
            print('[Debug] Existing articles number: ',len(existing_articles))
            print('Checking existing articles on coincidence...')
            for key,elem in tqdm.tqdm(self.db.items()):
                flag = True
                for i,title in enumerate(existing_articles):
                    if distance(title[0],elem['title']) < 5:
                        flag = False
                        version = elem['pdf_url'][elem['pdf_url'].rfind('v') + 1:]
                        flag = title[1][1:] != version and version.isdigit() and title[1][1:].isdigit()
                        #delete old version
                        if flag:
                            print('Article: ',title[0], ' - old version:',title[1][1:],', new version: ',version)
                            target_fn = dir_stamp[i]
                            os.remove(os.path.join(self.download_path,target_fn))
                            break
                if flag:
                    self.to_download.append({'pdf_url':elem['pdf_url'], 
                                        'title':self._extract_authors(elem)+self._extract_date(elem)+elem['title']})
            print('Checking finished. {0} articles ready for download!'.format(len(self.to_download)))
            
    def download(self,slugify,reload_download_list=False):
        if reload_download_list:
            self.prepare_for_download()
        if not self.to_download:
            print('Download list is empty. Trying to reload.')
            self.prepare_for_download()
            self.download()
        else:
            print('Download started...')
            for elem in tqdm.tqdm(self.to_download):
                try:
                    arxiv.download(elem,slugify=slugify, dirpath=self.download_path)
                except:
                    print('Something went wrong during downloading {0} by url {1}. Passed'.format(elem['title'],
                                                                                          elem['pdf_url']))    
            print('Download finished!')

            
            
