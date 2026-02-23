import requests
import time
import json
from typing import List, Dict, Any
import logging
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FrontendRepoScraper:
    def __init__(self):
        load_dotenv()
        self.github_token = os.getenv('GITHUB_TOKEN')
        
        if not self.github_token:
            raise ValueError("Please set GITHUB_TOKEN in .env file")
            
        self.headers = {
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        self.base_url = 'https://api.github.com'
        self.repos = []
        
    def search_frontend_repos(self, year: int, per_page: int = 100, max_repos: int = 5000) -> List[Dict[str, Any]]:

        search_queries = [
            f'react language:javascript pushed:{year}-01-01..{year}-12-31 stars:>20',
            f'vue language:javascript pushed:{year}-01-01..{year}-12-31 stars:>20', 
            f'angular language:typescript pushed:{year}-01-01..{year}-12-31 stars:>20',
            f'frontend language:javascript pushed:{year}-01-01..{year}-12-31 stars:>10',
            f'web app language:javascript pushed:{year}-01-01..{year}-12-31 stars:>10',
            f'dashboard language:javascript pushed:{year}-01-01..{year}-12-31 stars:>10',
            f'svelte language:javascript pushed:{year}-01-01..{year}-12-31 stars:>10',
            f'next.js language:javascript pushed:{year}-01-01..{year}-12-31 stars:>10',
            f'nuxt language:javascript pushed:{year}-01-01..{year}-12-31 stars:>10',
            f'language:javascript created:{year}-01-01..{year}-12-31 stars:>5',
            f'language:typescript created:{year}-01-01..{year}-12-31 stars:>5',
            f'webpack language:javascript pushed:{year}-01-01..{year}-12-31 stars:>5',
            f'vite language:javascript pushed:{year}-01-01..{year}-12-31 stars:>5',
            f'tailwind language:javascript pushed:{year}-01-01..{year}-12-31 stars:>5',
            f'bootstrap language:javascript pushed:{year}-01-01..{year}-12-31 stars:>5',
            f'electron language:javascript pushed:{year}-01-01..{year}-12-31 stars:>5'
        ]
        
        all_repos = []
        repo_urls = set()
        
        for query in search_queries:
            if len(all_repos) >= max_repos:
                logger.info(f"reached max repos {max_repos}, stop searching")
                break
                
            logger.info(f"searching keyword: {query}")
            
            page = 1
            while len(all_repos) < max_repos:
                try:
                    search_url = f"{self.base_url}/search/repositories"
                    params = {
                        'q': query,
                        'sort': 'stars',
                        'order': 'desc',
                        'per_page': per_page,
                        'page': page
                    }
                    
                    response = requests.get(search_url, headers=self.headers, params=params)
                    
                    if response.status_code == 403:
                        logger.warning("API limit, waiting 60 seconds...")
                        time.sleep(60)
                        continue
                    
                    if response.status_code == 422:
                        logger.warning(f"reached GitHub limit (max 1000 results), skip next page: {query}")
                        break
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    if not data.get('items'):
                        logger.info(f"no more results for '{query}'")
                        break
                    
                    logger.info(f"page {page} for '{query}', got {len(data['items'])} results")
                    
                    for repo in data['items']:
                        repo_url = repo['html_url']
                        
                        if repo_url in repo_urls:
                            continue
                            
                        if self._is_valid_frontend_repo(repo) and self._is_active_in_year(repo, year):
                            repo_info = self._extract_repo_info(repo)
                            all_repos.append(repo_info)
                            repo_urls.add(repo_url)
                            
                            if len(all_repos) >= max_repos:
                                break
                    
                    logger.info(f"collected {len(all_repos)} active frontend repos in {year}")
                    page += 1
                    
                    time.sleep(1)
                    
                except requests.RequestException as e:
                    logger.error(f"search error: {e}")
                    time.sleep(5)
                    continue
                except Exception as e:
                    logger.error(f"error: {e}")
                    continue
                    
                if page >= 10:
                    logger.info(f"reached GitHub API page limit (10 pages), continue next query: {query}")
                    break
                    
                if len(all_repos) >= max_repos:
                    logger.info(f"collected enough repos: {len(all_repos)}")
                    break
        
        logger.info(f"collected {len(all_repos)} active frontend repos in {year}")
        self.repos = all_repos
        return all_repos
    
    def _is_active_in_year(self, repo: Dict[str, Any], year: int) -> bool:

        try:
            created_at = repo.get('created_at', '')
            updated_at = repo.get('updated_at', '')
            pushed_at = repo.get('pushed_at', '')
            
            for date_str in [created_at, updated_at, pushed_at]:
                if date_str:
                    date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    if date_obj.year == year:
                        return True
            
            return False
            
        except Exception as e:
            logger.warning(f"error checking {year} active: {e}")
            return False
    
    def _is_valid_frontend_repo(self, repo: Dict[str, Any]) -> bool:
        if repo.get('fork', False):
            return False
            
        if repo.get('stargazers_count', 0) < 3:
            return False
            
        description = (repo.get('description') or '').lower()
        name = repo.get('name', '').lower()
        
        frontend_keywords = [
            'react', 'vue', 'angular', 'svelte', 'frontend', 'web', 'ui', 'dashboard',
            'admin', 'app', 'website', 'client', 'javascript', 'typescript', 'next',
            'nuxt', 'webpack', 'vite', 'component', 'bootstrap', 'material'
        ]
        
        exclude_keywords = [
            'backend', 'server', 'database', 'ml', 'machine learning', 'ai', 'neural',
            'deep learning', 'pytorch', 'tensorflow', 'model', 'algorithm', 'data science',
            'devops', 'infrastructure', 'kubernetes', 'docker only', 'cli tool'
        ]
        
        has_frontend_keyword = any(keyword in description or keyword in name 
                                 for keyword in frontend_keywords)
        
        has_exclude_keyword = any(keyword in description for keyword in exclude_keywords)
        
        return has_frontend_keyword and not has_exclude_keyword
    
    def _verify_frontend_repo(self, repo_info: Dict[str, Any]) -> bool:

        try:
            languages_url = f"{self.base_url}/repos/{repo_info['full_name']}/languages"
            response = requests.get(languages_url, headers=self.headers)
            
            if response.status_code != 200:
                return True
                
            languages = response.json()
            
            if not languages:
                return False
                
            total_bytes = sum(languages.values())
            
            frontend_languages = ['JavaScript', 'TypeScript', 'HTML', 'CSS', 'SCSS', 'Vue', 'Svelte']
            frontend_bytes = sum(languages.get(lang, 0) for lang in frontend_languages)
            
            backend_languages = ['Python', 'Java', 'Go', 'C#', 'PHP', 'Ruby', 'Rust', 'C++', 'C']
            backend_bytes = sum(languages.get(lang, 0) for lang in backend_languages)
            
            frontend_ratio = frontend_bytes / total_bytes if total_bytes > 0 else 0
            backend_ratio = backend_bytes / total_bytes if total_bytes > 0 else 0

            repo_info['languages'] = languages
            repo_info['frontend_ratio'] = frontend_ratio
            repo_info['backend_ratio'] = backend_ratio
            
            is_frontend = frontend_ratio > 0.4 and backend_ratio < 0.6
            
            time.sleep(0.5)
            return is_frontend
            
        except Exception as e:
            logger.warning(f"error verifying {repo_info['full_name']}: {e}")
            return True
    
    def _extract_repo_info(self, repo: Dict[str, Any]) -> Dict[str, Any]:

        return {
            'name': repo.get('name'),
            'full_name': repo.get('full_name'),
            'description': repo.get('description'),
            'html_url': repo.get('html_url'),
            'stargazers_count': repo.get('stargazers_count'),
            'forks_count': repo.get('forks_count'),
            'language': repo.get('language'),
            'topics': repo.get('topics', []),
            'created_at': repo.get('created_at'),
            'updated_at': repo.get('updated_at'),
            'has_issues': repo.get('has_issues'),
            'open_issues_count': repo.get('open_issues_count'),
            'default_branch': repo.get('default_branch', 'main')
        }
    
    def save_repos_parquet(self, year: int, filename: str = None, save_dir="/home/strrl/ssd/repo"):
        if not self.repos:
            logger.warning("no repos data to save")
            return
        
        if filename is None:
            filename = f'github_frontend_repos_{year}.parquet'
        full_path = os.path.join(save_dir, filename)

        df = pd.DataFrame(self.repos)
        
        df.to_parquet(full_path, index=False)
        logger.info(f"saved {len(self.repos)} repos to {full_path}")
          
    def load_repos_parquet(self, year: int, filename: str = None):

        if filename is None:
            filename = f'github_frontend_repos_{year}.parquet'
            
        try:
            df = pd.read_parquet(filename)
            self.repos = df.to_dict('records')
            logger.info(f"loaded {len(self.repos)} repos from {filename}")
        except FileNotFoundError:
            logger.warning(f"{filename} not found")
        except Exception as e:
            logger.error(f"error loading {filename}: {e}")
    
    def save_repos(self, filename: str = 'frontend_repos.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.repos, f, indent=2, ensure_ascii=False)
        logger.info(f"saved {len(self.repos)} repos to {filename}")
    
    def load_repos(self, filename: str = 'frontend_repos.json'):

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                self.repos = json.load(f)
            logger.info(f"loaded {len(self.repos)} repos from {filename}")
        except FileNotFoundError:
            logger.warning(f"{filename} not found")
        except Exception as e:
            logger.error(f"error loading {filename}: {e}")
    
    def get_repo_stats(self) -> Dict[str, Any]:

        if not self.repos:
            return {}
            
        total_repos = len(self.repos)
        total_stars = sum(repo.get('stargazers_count', 0) for repo in self.repos)
        total_forks = sum(repo.get('forks_count', 0) for repo in self.repos)
        
        languages = {}
        for repo in self.repos:
            lang = repo.get('language')
            if lang:
                languages[lang] = languages.get(lang, 0) + 1
        
        frontend_ratios = [repo.get('frontend_ratio', 0) for repo in self.repos if 'frontend_ratio' in repo]
        avg_frontend_ratio = sum(frontend_ratios) / len(frontend_ratios) if frontend_ratios else 0
        
        return {
            'total_repos': total_repos,
            'total_stars': total_stars,
            'total_forks': total_forks,
            'avg_stars': total_stars / total_repos if total_repos > 0 else 0,
            'avg_forks': total_forks / total_repos if total_repos > 0 else 0,
            'languages': dict(sorted(languages.items(), key=lambda x: x[1], reverse=True)),
            'avg_frontend_ratio': avg_frontend_ratio
        }

if __name__ == '__main__':

    if len(sys.argv) > 1:
        try:
            year = int(sys.argv[1])
        except ValueError:
            print("please input a valid year number")
            exit(1)
    else:
        try:
            year = int(input("please input a year: "))
        except ValueError:
            print("please input a valid year number")
            exit(1)
    
    current_year = datetime.now().year
    if year < 2008 or year > current_year:
        print(f"year should be between 2008 and {current_year}")
        exit(1)
    
    scraper = FrontendRepoScraper()
    
    repos = scraper.search_frontend_repos(year, max_repos=10000)
    
    scraper.save_repos_parquet(year)
    
    stats = scraper.get_repo_stats()
    print(f"\n=== {year} frontend repos stats ===")
    print(f"total repos: {stats['total_repos']}")
    print(f"total stars: {stats['total_stars']}")
    print(f"avg stars: {stats['avg_stars']:.1f}")
    print(f"avg frontend ratio: {stats['avg_frontend_ratio']:.1%}")
    print(f"main languages: {list(stats['languages'].keys())[:5]}")
