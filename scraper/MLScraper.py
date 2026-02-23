import requests
import pandas as pd
import time
from datetime import datetime
import logging
import sys
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubMLScraper:
    def __init__(self, github_token=None):

        self.base_url = "https://api.github.com"
        self.headers = {'Accept': 'application/vnd.github+json'}
        
        if github_token:
            self.headers['Authorization'] = f'token {github_token}'
        
        self.description_keywords = [
            'machine learning', 'deep learning', 'artificial intelligence', 'neural network',
            'computer vision', 'natural language processing', 'nlp', 'data science',
            'reinforcement learning', 'supervised learning', 'unsupervised learning',
            'tensorflow', 'pytorch', 'keras', 'scikit-learn', 'sklearn', 'opencv',
            'transformer', 'bert', 'gpt', 'gan', 'cnn', 'rnn', 'lstm', 'autoencoder',
            'classification', 'regression', 'clustering', 'recommendation system',
            'feature engineering', 'model training', 'neural', 'ai', 'ml'
        ]
        
        self.ml_topics = [
            'machine-learning', 'deep-learning', 'artificial-intelligence', 'neural-networks',
            'computer-vision', 'natural-language-processing', 'data-science', 'tensorflow',
            'pytorch', 'keras', 'scikit-learn', 'opencv', 'reinforcement-learning',
            'supervised-learning', 'unsupervised-learning', 'classification', 'regression',
            'clustering', 'recommendation-system', 'feature-engineering', 'model-training',
            'ai', 'ml', 'nlp', 'cv', 'gan', 'cnn', 'rnn', 'lstm', 'autoencoder'
        ]
    
    def search_repositories(self, query, max_results=1000):

        repos = []
        page = 1
        per_page = 100
        
        while len(repos) < max_results:
            url = f"{self.base_url}/search/repositories"
            params = {
                'q': query,
                'sort': 'stars',
                'order': 'desc',
                'per_page': per_page,
                'page': page
            }
            
            try:
                response = requests.get(url, headers=self.headers, params=params)
                
                if response.status_code == 403:
                    logger.warning("API limit reached, waiting 60 seconds...")
                    time.sleep(60)
                    continue
                
                if response.status_code != 200:
                    logger.error(f"request failed: {response.status_code} - {response.text}")
                    break
                
                data = response.json()
                items = data.get('items', [])
                
                if not items:
                    logger.info(f"page {page} has no more results")
                    break
                
                repos.extend(items)
                logger.info(f"page {page} has {len(repos)} repos")
                
                if len(items) < per_page or page >= 10:
                    break
                
                page += 1
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"request exception: {e}")
                break
        
        return repos[:max_results]
    
    def is_ml_related(self, repo):

        score = 0
        match_details = {
            'description_matches': [],
            'topic_matches': [],
            'name_matches': []
        }
        
        name = repo.get('name', '').lower()
        for keyword in ['ml', 'ai', 'neural', 'deep', 'learning', 'vision', 'nlp']:
            if keyword in name:
                score += 1
                match_details['name_matches'].append(keyword)
        
        description = repo.get('description', '')
        if description:
            description_lower = description.lower()
            for keyword in self.description_keywords:
                if keyword in description_lower:
                    score += 2
                    match_details['description_matches'].append(keyword)
        
        topics = repo.get('topics', [])
        for topic in topics:
            if topic in self.ml_topics:
                score += 3
                match_details['topic_matches'].append(topic)
        
        language = repo.get('language', '')
        if language in ['Python', 'Jupyter Notebook', 'R', 'MATLAB']:
            score += 1
        
        return score >= 3, score, match_details
    
    def extract_repo_info(self, repo):

        return {
            'id': repo.get('id'),
            'name': repo.get('name'),
            'full_name': repo.get('full_name'),
            'owner': repo.get('owner', {}).get('login'),
            'owner_type': repo.get('owner', {}).get('type'),
            'description': repo.get('description'),
            'url': repo.get('html_url'),
            'clone_url': repo.get('clone_url'),
            'stars': repo.get('stargazers_count', 0),
            'forks': repo.get('forks_count', 0),
            'watchers': repo.get('watchers_count', 0),
            'open_issues': repo.get('open_issues_count', 0),
            'language': repo.get('language'),
            'topics': repo.get('topics', []),
            'created_at': repo.get('created_at'),
            'updated_at': repo.get('updated_at'),
            'pushed_at': repo.get('pushed_at'),
            'size': repo.get('size', 0),
            'default_branch': repo.get('default_branch'),
            'is_fork': repo.get('fork', False),
            'license': repo.get('license', {}).get('name') if repo.get('license') else None,
            'archived': repo.get('archived', False),
            'disabled': repo.get('disabled', False)
        }
    
    def search_ml_repos(self, year, max_results=1000):

        logger.info(f"start searching ml related repos in {year}...")
        
        search_queries = [
            f"topic:machine-learning pushed:{year}-01-01..{year}-12-31",
            f"topic:deep-learning pushed:{year}-01-01..{year}-12-31", 
            f"topic:artificial-intelligence pushed:{year}-01-01..{year}-12-31",
            f"topic:neural-networks pushed:{year}-01-01..{year}-12-31",
            f"topic:computer-vision pushed:{year}-01-01..{year}-12-31",
            f"topic:natural-language-processing pushed:{year}-01-01..{year}-12-31",
            f"topic:data-science pushed:{year}-01-01..{year}-12-31",
            
            f"machine learning pushed:{year}-01-01..{year}-12-31",
            f"deep learning pushed:{year}-01-01..{year}-12-31",
            f"neural network pushed:{year}-01-01..{year}-12-31",
            f"computer vision pushed:{year}-01-01..{year}-12-31",
            
            f"tensorflow pushed:{year}-01-01..{year}-12-31",
            f"pytorch pushed:{year}-01-01..{year}-12-31",
            f"keras pushed:{year}-01-01..{year}-12-31",
            f"scikit-learn pushed:{year}-01-01..{year}-12-31",
            
            f"language:python machine learning pushed:{year}-01-01..{year}-12-31",
            f"language:jupyter-notebook pushed:{year}-01-01..{year}-12-31"
        ]
        
        all_repos = {}
        ml_repos_data = []
        
        for i, query in enumerate(search_queries, 1):
            logger.info(f"query {i}/{len(search_queries)}: {query}")
            
            try:
                repos = self.search_repositories(query, max_results=200)
                logger.info(f"found {len(repos)} repos")
                
                for repo in repos:
                    full_name = repo.get('full_name')
                    if full_name and full_name not in all_repos:
                        is_related, score, match_details = self.is_ml_related(repo)
                        
                        if is_related:
                            repo_info = self.extract_repo_info(repo)
                            repo_info['ml_score'] = score
                            repo_info['match_details'] = str(match_details)
                            
                            all_repos[full_name] = repo_info
                            ml_repos_data.append(repo_info)
                            
                logger.info(f"current ml related repos: {len(ml_repos_data)}")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"query failed: {e}")
                continue
        
        df = pd.DataFrame(ml_repos_data)
        
        if not df.empty:    
            df = df.sort_values('stars', ascending=False).reset_index(drop=True)
            logger.info(f"collected {len(df)} ml related repos")
        else:
            logger.warning("did not find ml related repos")
            
        return df
    
    def save_to_parquet(self, df, year, filename=None, save_dir="/home/strrl/ssd/repo"):

        if filename is None:
            filename = f"github_ml_repos_{year}.parquet"
            
        os.makedirs(save_dir, exist_ok=True)
        
        full_path = os.path.join(save_dir, filename)
            
        try:
            df.to_parquet(full_path, index=False)
            logger.info(f"data saved to {full_path}")
            
            logger.info(f"total repos: {len(df)}")
            logger.info(f"average stars: {df['stars'].mean():.1f}")
            logger.info(f"most popular language: {df['language'].value_counts().head()}")
            
        except Exception as e:
            logger.error(f"failed to save file: {e}")

def main():

    if len(sys.argv) > 1:
        try:
            year = int(sys.argv[1])
        except ValueError:
            print("please input a valid year")
            return
    else:
        try:
            year = int(input("please input a year: "))
        except ValueError:
            print("please input a valid year")
            return
    
    current_year = datetime.now().year
    if year < 2008 or year > current_year:
        print(f"year should be between 2008 and {current_year}")
        return
    
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        logger.warning("did not find github token")
        logger.info("please set github token in .env file")
        return
    
    scraper = GitHubMLScraper(github_token=github_token)
    
    df = scraper.search_ml_repos(year, max_results=1000)
    
    if not df.empty:
        scraper.save_to_parquet(df, year)
        
        print(f"\ntop 10 ml repos in {year}:")
        print(df[['full_name', 'stars', 'language', 'description']].head(10).to_string())
        
        all_topics = []
        for topics in df['topics']:
            all_topics.extend(topics)
        
        if all_topics:
            topics_df = pd.Series(all_topics).value_counts().head(10)
            print(f"\ntop 10 topics:")
            print(topics_df)
    else:
        print(f"did not find ml related repos in {year}")

if __name__ == "__main__":
    main()