from scholarly import scholarly
from scholarly import ProxyGenerator
import pandas as pd

'''Take in a set of professor names and return .xlsx file with the papers they have published'''

class GetProfessorsPapersJob:
   _pg = ProxyGenerator()

   def __init__(self):
      self.publication_limit = 2
      self.retry_count = 3 
      self._get_and_use_proxy()
      self.data = []
      #column titles from: https://docs.google.com/spreadsheets/d/1IyUszSqo4EPYwmJJRrrOwBM3yjNGrrFM/edit?usp=sharing&ouid=109276556324270790784&rtpof=true&sd=true
      self.columns = ['professor name', 'paper_abstract', 'title', 'date']
      self.failed_professor_names = set()

   def populate_results(self, professor_names):
      professor_names_set = set(professor_names)

      for professor_name in professor_names_set:
         try:
            self._populate_results_for_professor(professor_name)
         except Exception as e:
            print(e)
            print(f'Failed to get professor {professor_name} papers')
            self.failed_professor_names.add(professor_name)
            continue
      results = pd.DataFrame(self.data, columns=self.columns)   
      results.to_excel('papers.xlsx')
      print(f'Failed to get papers for {self.failed_professor_names}')

   def _get_and_use_proxy(self):
      while True:
         if GetProfessorsPapersJob._pg.FreeProxies():
            break
      scholarly.use_proxy(GetProfessorsPapersJob._pg)
   
   def _is_good_result_refresh_proxy(self, result):
      if result is None:
         self._get_and_use_proxy()
         return False, result
      return True, result
   
   def _search_author(self, author_name):
      for i in range (self.retry_count):
         search_query = scholarly.search_author(author_name)
         is_good_result, search_query = self._is_good_result_refresh_proxy(search_query)
         if is_good_result:
            break
      return search_query
   
   def _fill_scholarly_author(self, scholarly_author):
      for i in range (self.retry_count):
         # By default this is sorted="citedby"
         filled_author = scholarly.fill(scholarly_author, sortby='year', publication_limit=self.publication_limit)
         is_good_result, filled_author = self._is_good_result_refresh_proxy(filled_author)
         if is_good_result:
            break
      return filled_author
   
   def _fill_publication(self, publication):
      for i in range (self.retry_count):
         filled_publication = scholarly.fill(publication)
         is_good_result, filled_publication = self._is_good_result_refresh_proxy(filled_publication)
         if is_good_result:
            break
      return filled_publication  

   def _populate_results_for_professor(self, professor_name):
      search_query = self._search_author(professor_name)
      scholarly_author = next(search_query)
      # By default this is sorted="citedby"
      author = self._fill_scholarly_author(scholarly_author)
      publications = author['publications']
      for publication in publications:
         fill_publication = self._fill_publication(publication)
         if 'pub_timestamp' in fill_publication['bib']:
            publication_time = fill_publication['bib']['pub_timestamp']
         else:
            publication_time = fill_publication['bib']['pub_year']
         self.data.append([author['name'], fill_publication['bib']['abstract'], fill_publication['bib']['title'], publication_time])

if __name__ == '__main__':
   job = GetProfessorsPapersJob()
   job.populate_results(['Ishtiaque Ahmed', 'Jimmy Ba', 'Michael Brudno'])