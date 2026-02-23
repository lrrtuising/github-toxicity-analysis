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

class GitHubMobileScraper:
    def __init__(self, github_token=None):

        self.base_url = "https://api.github.com"
        self.headers = {'Accept': 'application/vnd.github+json'}
        
        if github_token:
            self.headers['Authorization'] = f'token {github_token}'
        
        self.description_keywords = [
            'mobile app', 'android app', 'ios app', 'mobile development', 'mobile application',
            'react native', 'flutter', 'ionic', 'xamarin', 'phonegap', 'cordova',
            'android studio', 'xcode', 'swift', 'kotlin', 'java android', 'objective-c',
            'mobile game', 'mobile ui', 'mobile framework', 'cross platform', 'hybrid app',
            'app development', 'mobile sdk', 'mobile library', 'android library', 'ios library',
            'mobile component', 'mobile widget', 'mobile navigation', 'mobile animation',
            'mobile responsive', 'mobile first', 'pwa', 'progressive web app',
            'mobile testing', 'mobile automation', 'mobile ci/cd', 'mobile deployment',
            'mobile performance', 'mobile optimization', 'mobile security', 'mobile auth',
            'push notification', 'mobile analytics', 'mobile crash', 'mobile monitoring',
            'mobile backend', 'mobile api', 'mobile database', 'mobile storage',
            'augmented reality', 'ar', 'virtual reality', 'vr', 'arkit', 'arcore',
            'firebase', 'realm', 'sqlite', 'core data', 'room database',
            'jetpack compose', 'swiftui', 'uikit', 'android jetpack', 'material design'
        ]
        
        self.mobile_topics = [
            'mobile', 'android', 'ios', 'mobile-app', 'mobile-development', 'app-development',
            'react-native', 'flutter', 'ionic', 'xamarin', 'phonegap', 'cordova',
            'swift', 'kotlin', 'java', 'objective-c', 'dart', 'javascript',
            'android-app', 'ios-app', 'mobile-application', 'cross-platform',
            'hybrid-app', 'native-app', 'mobile-game', 'mobile-ui', 'mobile-framework',
            'mobile-library', 'android-library', 'ios-library', 'mobile-component',
            'mobile-widget', 'mobile-navigation', 'mobile-animation', 'mobile-responsive',
            'pwa', 'progressive-web-app', 'mobile-first', 'responsive-design',
            'mobile-testing', 'mobile-automation', 'mobile-ci-cd', 'mobile-deployment',
            'mobile-performance', 'mobile-optimization', 'mobile-security', 'mobile-auth',
            'push-notifications', 'mobile-analytics', 'mobile-crash', 'mobile-monitoring',
            'mobile-backend', 'mobile-api', 'mobile-database', 'mobile-storage',
            'augmented-reality', 'ar', 'virtual-reality', 'vr', 'arkit', 'arcore',
            'firebase', 'realm', 'sqlite', 'core-data', 'room-database',
            'jetpack-compose', 'swiftui', 'uikit', 'android-jetpack', 'material-design',
            'mobile-sdk', 'android-sdk', 'ios-sdk', 'mobile-tools', 'mobile-utilities',
            'android-studio', 'xcode', 'mobile-editor', 'mobile-ide',
            'wear-os', 'watchos', 'tvos', 'android-tv', 'android-auto', 'carplay',
            'mobile-commerce', 'mobile-payment', 'mobile-banking', 'fintech-mobile',
            'mobile-health', 'mobile-fitness', 'mobile-education', 'mobile-social',
            'mobile-entertainment', 'mobile-productivity', 'mobile-utility'
        ]
        
        self.mobile_languages = [
            'Swift', 'Kotlin', 'Java', 'Objective-C', 'Dart', 'JavaScript',
            'TypeScript', 'C#', 'C++', 'Python', 'Ruby', 'HTML', 'CSS'
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
                logger.info(f"page {page} collected {len(repos)} repos")
                
                if len(items) < per_page or page >= 10:
                    break
                
                page += 1
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"request exception: {e}")
                break
        
        return repos[:max_results]
    
    def is_mobile_related(self, repo):

        score = 0
        match_details = {
            'description_matches': [],
            'topic_matches': [],
            'name_matches': [],
            'language_matches': []
        }
        
        name = repo.get('name', '').lower()
        mobile_name_keywords = [
            'mobile', 'android', 'ios', 'app', 'flutter', 'react-native', 'ionic',
            'swift', 'kotlin', 'xamarin', 'phone', 'tablet', 'ar', 'vr'
        ]
        for keyword in mobile_name_keywords:
            if keyword in name:
                score += 2
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
            if topic in self.mobile_topics:
                score += 4
                match_details['topic_matches'].append(topic)
        
        language = repo.get('language', '')
        if language in self.mobile_languages:
            score += 1
            match_details['language_matches'].append(language)
            
            if language in ['Swift', 'Kotlin', 'Dart', 'Objective-C']:
                score += 2
        
        if description:
            mobile_frameworks = [
                'react native', 'flutter', 'ionic', 'xamarin', 'cordova',
                'phonegap', 'jetpack compose', 'swiftui'
            ]
            for framework in mobile_frameworks:
                if framework in description.lower():
                    score += 3
        
        return score >= 4, score, match_details
    
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
    
    def search_mobile_repos(self, year, max_results=1000):

        logger.info(f"start searching mobile development related repos in {year}...")
        
        search_queries = [
            f"topic:mobile pushed:{year}-01-01..{year}-12-31",
            f"topic:android pushed:{year}-01-01..{year}-12-31",
            f"topic:ios pushed:{year}-01-01..{year}-12-31",
            f"topic:mobile-app pushed:{year}-01-01..{year}-12-31",
            f"topic:react-native pushed:{year}-01-01..{year}-12-31",
            f"topic:flutter pushed:{year}-01-01..{year}-12-31",
            f"topic:ionic pushed:{year}-01-01..{year}-12-31",
            f"topic:xamarin pushed:{year}-01-01..{year}-12-31",
            f"topic:mobile-development pushed:{year}-01-01..{year}-12-31",
            f"topic:cross-platform pushed:{year}-01-01..{year}-12-31",
            
            f"mobile app pushed:{year}-01-01..{year}-12-31",
            f"android app pushed:{year}-01-01..{year}-12-31",
            f"ios app pushed:{year}-01-01..{year}-12-31",
            f"mobile development pushed:{year}-01-01..{year}-12-31",
            f"react native pushed:{year}-01-01..{year}-12-31",
            f"flutter app pushed:{year}-01-01..{year}-12-31",
            
            f"language:swift pushed:{year}-01-01..{year}-12-31",
            f"language:kotlin pushed:{year}-01-01..{year}-12-31",
            f"language:dart pushed:{year}-01-01..{year}-12-31",
            f"language:objective-c pushed:{year}-01-01..{year}-12-31",
            f"language:java android pushed:{year}-01-01..{year}-12-31",
            f"language:javascript react-native pushed:{year}-01-01..{year}-12-31",
            f"language:typescript react-native pushed:{year}-01-01..{year}-12-31",
            
            f"jetpack compose pushed:{year}-01-01..{year}-12-31",
            f"swiftui pushed:{year}-01-01..{year}-12-31",
            f"firebase pushed:{year}-01-01..{year}-12-31",
            f"realm pushed:{year}-01-01..{year}-12-31",
            
            f"topic:android-library pushed:{year}-01-01..{year}-12-31",
            f"topic:ios-library pushed:{year}-01-01..{year}-12-31",
            f"topic:mobile-game pushed:{year}-01-01..{year}-12-31",
            f"topic:pwa pushed:{year}-01-01..{year}-12-31",
            f"topic:progressive-web-app pushed:{year}-01-01..{year}-12-31"
        ]
        
        all_repos = {}
        mobile_repos_data = []
        
        for i, query in enumerate(search_queries, 1):
            logger.info(f"query {i}/{len(search_queries)}: {query}")
            
            try:
                repos = self.search_repositories(query, max_results=150)
                logger.info(f"found {len(repos)} repos")
                
                for repo in repos:
                    full_name = repo.get('full_name')
                    if full_name and full_name not in all_repos:
                        is_related, score, match_details = self.is_mobile_related(repo)
                        
                        if is_related:
                            repo_info = self.extract_repo_info(repo)
                            repo_info['mobile_score'] = score
                            repo_info['match_details'] = str(match_details)
                            
                            all_repos[full_name] = repo_info
                            mobile_repos_data.append(repo_info)
                            
                logger.info(f"current mobile related repos: {len(mobile_repos_data)}")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"query failed: {e}")
                continue
        
        df = pd.DataFrame(mobile_repos_data)
        
        if not df.empty:
            df = df.sort_values('stars', ascending=False).reset_index(drop=True)
            logger.info(f"collected {len(df)} mobile development related repos")
        else:
            logger.warning("did not find mobile development related repos")
            
        return df
    
    def save_to_parquet(self, df, year, filename=None, save_dir= "/home/strrl/ssd/repo"):

        if filename is None:
            filename = f"github_mobile_repos_{year}.parquet"
        
        full_path = os.path.join(save_dir, filename)
        try:
            df.to_parquet(full_path, index=False)
            logger.info(f"data saved to {full_path}")
            
            logger.info(f"total repos: {len(df)}")
            logger.info(f"average stars: {df['stars'].mean():.1f}")
            logger.info(f"most popular languages: {df['language'].value_counts().head()}")
            
            if 'topics' in df.columns:
                all_topics = []
                for topics in df['topics']:
                    all_topics.extend(topics)
                if all_topics:
                    topics_series = pd.Series(all_topics)
                    mobile_topics_count = topics_series.value_counts().head(10)
                    logger.info(f"top mobile topics: {mobile_topics_count}")
            
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
    
    scraper = GitHubMobileScraper(github_token=github_token)
    
    df = scraper.search_mobile_repos(year, max_results=5000)
    
    if not df.empty:
        scraper.save_to_parquet(df, year)
        
        print(f"\ntop 10 mobile development repos in {year}:")
        print(df[['full_name', 'stars', 'language', 'description']].head(10).to_string())
        
        print(f"\nlanguage distribution:")
        print(df['language'].value_counts().head(10))
        
        all_topics = []
        for topics in df['topics']:
            all_topics.extend(topics)
        
        if all_topics:
            topics_df = pd.Series(all_topics).value_counts().head(15)
            print(f"\ntop 15 mobile development topics:")
            print(topics_df)
            
        android_repos = df[df['topics'].apply(lambda x: 'android' in x or 'android-app' in x)]
        ios_repos = df[df['topics'].apply(lambda x: 'ios' in x or 'ios-app' in x)]
        cross_platform_repos = df[df['topics'].apply(lambda x: any(topic in x for topic in ['react-native', 'flutter', 'ionic', 'xamarin', 'cross-platform']))]
        
        print(f"\nplatform distribution:")
        print(f"Android repos: {len(android_repos)}")
        print(f"iOS repos: {len(ios_repos)}")
        print(f"Cross-platform repos: {len(cross_platform_repos)}")
        
    else:
        print(f"did not find mobile development related repos in {year}")

if __name__ == "__main__":
    main()