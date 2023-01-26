from scholarly import scholarly
from scholarly import ProxyGenerator
import pandas as pd
import logging
import concurrent.futures
import coloredlogs

coloredlogs.install()
logging.basicConfig(level=logging.DEBUG)

class RetryFailedException(Exception):
    def __init__(self, message):
        super(RetryFailedException, self).__init__(message)
        self.message = message

class GetProfessorsPapersJob:
   def __init__(self):
      self.proxy_timeout_seconds = 1 
      self.proxy_wait_time_seconds = 60 
      self.publication_limit_per_author = 5
      self.use_new_proxy_retry_limit = 3 
      
      self._use_new_proxy()
      self.rows = []
      self.failed_publication_titles = []
      self.failed_professor_names = []

   def populate_results(self, professor_names):
      professor_names_set = set(professor_names)
      logging.info('Total professor names: ' + str(len(professor_names_set)))
      for professor_name in professor_names_set:
         try:
            self._populate_results_for_professor(professor_name)
            logging.info('Successfully scrapped professor: ' + professor_name)
         except Exception as e:
            print(e.message)
            logging.error(e.message)
            self.failed_professor_names.append(professor_name)
            continue
      logging.error('Failed professor names: ' + str(self.failed_professor_names) + ' Failed publication titles: ' + str(self.failed_publication_titles))
      return self.rows

   def _use_new_proxy(self):
      logging.info('Using new proxy')
      proxyGenerator = ProxyGenerator()
      proxyGenerator.FreeProxies(timeout=self.proxy_timeout_seconds, wait_time=self.proxy_wait_time_seconds)
      scholarly.use_proxy(proxyGenerator)
      logging.info('Using new proxy done')
   
   def _search_author(self, author_name):
      logging.info('Searching author: ' + author_name)
      for i in range (self.use_new_proxy_retry_limit):
         search_query = scholarly.search_author(author_name)
         if search_query:
            return search_query
         else:
            logging.info('Failed to search author: ' + author_name + ' retry counter: ' + i)
            self._use_new_proxy()
      raise RetryFailedException('Failed to search author: ' + author_name)
   
   def _fill_scholarly_author(self, scholarly_author):
      logging.info('Filling author: ' + scholarly_author['name'])
      for i in range (self.use_new_proxy_retry_limit):
         # By default this is sorted="citedby"
         filled_author = scholarly.fill(scholarly_author, sortby='year', publication_limit=self.publication_limit_per_author)
         if filled_author:
            return filled_author
         else:
            logging.info('Failed to fill author: ' + scholarly_author['name'] + ' retry counter: ' + i)
            self._use_new_proxy()
      raise RetryFailedException('Failed to fill author: ' + scholarly_author['name'])
   
   def _fill_publication(self, publication):
      logging.info('Filling publication for bib: ' + publication['bib']['title'])
      for i in range (self.use_new_proxy_retry_limit):
         filled_publication = scholarly.fill(publication)
         if filled_publication:
            return filled_publication  
         else:
            logging.info('Failed to fill publication: ' + publication['bib']['title'] + ' retry counter: ' + i)
            self._use_new_proxy()
      raise RetryFailedException('Failed to fill publication: ' + publication['bib']['title'])

   def _populate_results_for_professor(self, professor_name):
      logging.info('Getting publications for professor: ' + professor_name)
      search_query = self._search_author(professor_name)
      scholarly_author = next(search_query)
      # By default this is sorted="citedby"
      author = self._fill_scholarly_author(scholarly_author)
      publications = author['publications']
      for publication in publications:
         logging.info(f'Getting publication for bib: {publication["bib"]["title"]}')
         filled_bib = publication['bib']
         if 'abstract' not in filled_bib or 'title' not in filled_bib:
            filled_bib = self._fill_publication(publication)['bib']
         if 'pub_timestamp' in filled_bib:
            publication_time = filled_bib['pub_timestamp']
         elif 'pub_year' in filled_bib:
            publication_time = filled_bib['pub_year']
         else:
            logging.warn('Defaulting to 2023 for publication time')
            publication_time = '2023'

         if 'abstract' in filled_bib and 'title' in filled_bib:
            self.rows.append([author['name'], filled_bib['abstract'], filled_bib['title'], publication_time])
            logging.info(f'Got publication for filled bib: {filled_bib["title"]}')
         else:
            self.failed_publication_titles.append(filled_bib['title'])
            logging.error(f'Failed to get publication for filled bib: {filled_bib["title"]}')

############################## Multi-Threading Begins ########################################

def to_excel(rows):
   #column titles from: https://docs.google.com/spreadsheets/d/1IyUszSqo4EPYwmJJRrrOwBM3yjNGrrFM/edit?usp=sharing&ouid=109276556324270790784&rtpof=true&sd=true
   results = pd.DataFrame(rows, columns=['professor name', 'paper_abstract', 'title', 'date'])   
   results.to_excel('professor_papers.xlsx')

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

'''Prototype: https://stackoverflow.com/questions/5442910/python-multiprocessing-pool-map-for-multiple-arguments'''
def thread_worker(professor_names):
   logging.info('Starting to process ' + str(len(professor_names)) + ' professors')
   job = GetProfessorsPapersJob()
   rows = job.populate_results(professor_names) 
   logging.info('Finished processing ' + str(len(rows)) + ' publications for ' + str(len(professor_names)) + ' professors with ')
   return rows

all_professor_names = ['Ishtiaque Ahmed','Jimmy Ba','Anthony Bonner','Michael Brudno','Marsha Chechik','Eyal de Lara','Sven Dickinson','Murat Erdogdu','Amir-massoud Farahmand','Azadeh Farzan','Animesh Garg','Igor Gilitschenski','Nick Koudas','David Levin','David Lindell','Fan Long','Chris Maddison','Peter Marbach','Alex Mariakakis','Maryam Mehri Dehnavi','Aleksandar Nikolov','Gennady Pekhimenko','Gerald Penn','Frank Rudzicz','Sushant Sachdeva','Nisarg Shah','Florian Shkurti','Babak Taati','Nandita Vijaykumar','Bo Wang','Daniel Wigdor','Kieran Campbell','Mark Chignell','Baochun Li','David Lie','Kelly Lyons','Alan Moses','Vardan Papyan','Scott Sanner','Eric Yu','Shurui Zhou','Benjamin Haibe-Kains','Christopher Beck','Michael Guerzhoy','Periklis Andritsos','Steven L. Waslander','Dehan Kong','Linbo Wang','Rahul G. Krishnan','Radu Craiu','Annie Lee','Robert Soden','Kirill Serkh','Leonard Wong','Fanny Chevalier','Eldan Cohen','Sheldon Lin','Ben Liang','Kyros Kutulakos','Nancy Reid','Andrei Badescu','Arvind Gupta','Eitan Grinspun','Elias Khalil ','Fae Azhari','Igor Jurisica','Jessica Gronsbell','Lueder Kahrs','Rohan Alexander','Samin Aref','Sebastian Jaimungal','Yun William Yu']
thread_pool_limit = 5
professor_names_to_process = all_professor_names[-5:] #last 5 professors
nested_professor_names = list(chunks(professor_names_to_process, thread_pool_limit))

def main():
   all_rows = []
   logging.info('Starting to process ' + str(len(professor_names_to_process)) + ' professors with ' + str(thread_pool_limit) + ' processes')
   # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor
   with concurrent.futures.ThreadPoolExecutor(max_workers=thread_pool_limit) as executor:
      try:    
         for rows in executor.map(thread_worker, nested_professor_names):
            logging.info('Finished processing ' + str(len(rows)) + ' publications')
            all_rows.extend(rows)
            
         logging.info('Total: Finished processing ' + str(len(all_rows)) + ' publications for ' + str(len(professor_names_to_process)) + ' professors')
         to_excel(all_rows)
      except Exception as e:
         to_excel(all_rows)
         logging.error('Failed to process ' + str(len(professor_names_to_process)) + ' professors with ' + str(thread_pool_limit) + ' processes')
         logging.error(e)

if __name__ == '__main__':
   main()