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

class GitHubGameScraper:
    def __init__(self, github_token=None):
        self.base_url = "https://api.github.com"
        self.headers = {'Accept': 'application/vnd.github+json'}
        
        if github_token:
            self.headers['Authorization'] = f'token {github_token}'
        
        self.description_keywords = [
            'game', 'gaming', 'game development', 'game engine', 'game framework',
            'video game', 'indie game', 'mobile game', 'web game', 'browser game',
            'arcade game', 'puzzle game', 'rpg', 'strategy game', 'action game',
            'platformer', 'shooter', 'racing game', 'simulation game', 'sports game',
            'unity', 'unreal engine', 'godot', 'game maker', 'construct', 'defold',
            'phaser', 'pixi.js', 'three.js', 'babylonjs', 'cocos2d', 'libgdx',
            'pygame', 'panda3d', 'love2d', 'monogame', 'xna', 'sfml', 'sdl',
            'opengl', 'vulkan', 'directx', 'webgl', 'metal', 'graphics programming',
            'game physics', 'collision detection', 'pathfinding', 'ai game',
            'game networking', 'multiplayer', 'mmo', 'real time strategy', 'rts',
            'first person shooter', 'fps', 'third person', 'side scroller',
            'game ui', 'game ux', 'game design', 'level editor', 'game tools',
            'game assets', 'sprite', 'animation', 'shader', 'particle system',
            'game audio', 'sound effect', 'game music', 'audio engine',
            'game state', 'game loop', 'input handling', 'game controller',
            'virtual reality game', 'vr game', 'augmented reality game', 'ar game',
            'retro game', 'pixel art', '2d game', '3d game', 'isometric',
            'turn based', 'real time', 'roguelike', 'metroidvania', 'bullet hell',
            'tower defense', 'match three', 'endless runner', 'clicker game',
            'visual novel', 'dating sim', 'quiz game', 'educational game',
            'serious game', 'gamification', 'game jam', 'ludum dare',
            'steam', 'itch.io', 'google play games', 'app store games',
            'nintendo switch', 'playstation', 'xbox', 'pc gaming', 'console game',
            'cross platform game', 'html5 game', 'flash game', 'canvas game'
        ]
        
        self.game_topics = [
            'game', 'games', 'gaming', 'game-development', 'game-engine', 'game-framework',
            'video-game', 'indie-game', 'mobile-game', 'web-game', 'browser-game',
            'arcade-game', 'puzzle-game', 'rpg', 'strategy-game', 'action-game',
            'platformer', 'shooter', 'racing-game', 'simulation-game', 'sports-game',
            'unity', 'unity3d', 'unreal-engine', 'godot', 'game-maker', 'construct',
            'defold', 'phaser', 'pixijs', 'threejs', 'babylonjs', 'cocos2d', 'libgdx',
            'pygame', 'panda3d', 'love2d', 'monogame', 'xna', 'sfml', 'sdl',
            'opengl', 'vulkan', 'directx', 'webgl', 'metal', 'graphics-programming',
            'game-physics', 'collision-detection', 'pathfinding', 'ai-game',
            'game-networking', 'multiplayer', 'mmo', 'real-time-strategy', 'rts',
            'first-person-shooter', 'fps', 'third-person', 'side-scroller',
            'game-ui', 'game-ux', 'game-design', 'level-editor', 'game-tools',
            'game-assets', 'sprite', 'animation', 'shader', 'particle-system',
            'game-audio', 'sound-effect', 'game-music', 'audio-engine',
            'game-state', 'game-loop', 'input-handling', 'game-controller',
            'vr-game', 'ar-game', 'virtual-reality', 'augmented-reality',
            'retro-game', 'pixel-art', '2d-game', '3d-game', 'isometric',
            'turn-based', 'real-time', 'roguelike', 'metroidvania', 'bullet-hell',
            'tower-defense', 'match-three', 'endless-runner', 'clicker-game',
            'visual-novel', 'dating-sim', 'quiz-game', 'educational-game',
            'serious-game', 'gamification', 'game-jam', 'ludum-dare',
            'steam', 'itch-io', 'google-play-games', 'app-store-games',
            'nintendo-switch', 'playstation', 'xbox', 'pc-gaming', 'console-game',
            'cross-platform-game', 'html5-game', 'flash-game', 'canvas-game',
            'game-server', 'game-client', 'game-bot', 'game-ai', 'procedural-generation',
            'game-mechanics', 'game-balance', 'game-analytics', 'game-monetization',
            'game-testing', 'game-optimization', 'game-performance', 'game-security',
            'esports', 'competitive-gaming', 'game-tournament', 'leaderboard',
            'achievements', 'game-progression', 'inventory-system', 'crafting-system',
            'dialogue-system', 'quest-system', 'skill-tree', 'character-customization'
        ]
        
        self.game_languages = [
            'C#', 'C++', 'C', 'JavaScript', 'TypeScript', 'Python', 'Java',
            'Lua', 'GDScript', 'ActionScript', 'UnityScript', 'HLSL', 'GLSL',
            'Rust', 'Go', 'Swift', 'Kotlin', 'Dart', 'Haxe', 'Assembly',
            'HTML', 'CSS', 'WebAssembly', 'Objective-C', 'F#', 'Scala'
        ]
        
        self.game_engines = [
            'unity', 'unreal', 'godot', 'phaser', 'pixi', 'three.js',
            'babylonjs', 'cocos2d', 'libgdx', 'pygame', 'love2d', 'monogame'
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
    
    def is_game_related(self, repo):

        score = 0
        match_details = {
            'description_matches': [],
            'topic_matches': [],
            'name_matches': [],
            'language_matches': [],
            'engine_matches': []
        }
        
        name = repo.get('name', '').lower()
        game_name_keywords = [
            'game', 'games', 'gaming', 'unity', 'unreal', 'godot', 'phaser',
            'arcade', 'puzzle', 'rpg', 'shooter', 'racing', 'platformer',
            'engine', 'framework', 'roguelike', 'tower', 'defense', 'match',
            'runner', 'clicker', 'simulator', 'tycoon', 'adventure'
        ]
        for keyword in game_name_keywords:
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
            if topic in self.game_topics:
                score += 4
                match_details['topic_matches'].append(topic)
        
        language = repo.get('language', '')
        if language in self.game_languages:
            score += 1
            match_details['language_matches'].append(language)
            
            if language in ['C#', 'C++', 'JavaScript', 'GDScript', 'Lua']:
                score += 1
        
        if description:
            description_lower = description.lower()
            for engine in self.game_engines:
                if engine in description_lower:
                    score += 3
                    match_details['engine_matches'].append(engine)
        
        if topics:
            high_value_topics = ['game', 'games', 'game-development', 'unity', 'unreal-engine', 'godot']
            if any(topic in high_value_topics for topic in topics):
                score += 2
        
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
    
    def search_game_repos(self, year, max_results=1000):

        logger.info(f"start searching game development related repos in {year}...")
        
        search_queries = [
            f"topic:game pushed:{year}-01-01..{year}-12-31",
            f"topic:games pushed:{year}-01-01..{year}-12-31",
            f"topic:game-development pushed:{year}-01-01..{year}-12-31",
            f"topic:game-engine pushed:{year}-01-01..{year}-12-31",
            f"topic:unity pushed:{year}-01-01..{year}-12-31",
            f"topic:unity3d pushed:{year}-01-01..{year}-12-31",
            f"topic:unreal-engine pushed:{year}-01-01..{year}-12-31",
            f"topic:godot pushed:{year}-01-01..{year}-12-31",
            f"topic:phaser pushed:{year}-01-01..{year}-12-31",
            f"topic:pygame pushed:{year}-01-01..{year}-12-31",
            f"topic:libgdx pushed:{year}-01-01..{year}-12-31",
            f"topic:cocos2d pushed:{year}-01-01..{year}-12-31",
            f"topic:monogame pushed:{year}-01-01..{year}-12-31",
            f"topic:love2d pushed:{year}-01-01..{year}-12-31",
            
            f"topic:indie-game pushed:{year}-01-01..{year}-12-31",
            f"topic:mobile-game pushed:{year}-01-01..{year}-12-31",
            f"topic:web-game pushed:{year}-01-01..{year}-12-31",
            f"topic:browser-game pushed:{year}-01-01..{year}-12-31",
            f"topic:arcade-game pushed:{year}-01-01..{year}-12-31",
            f"topic:puzzle-game pushed:{year}-01-01..{year}-12-31",
            f"topic:rpg pushed:{year}-01-01..{year}-12-31",
            f"topic:platformer pushed:{year}-01-01..{year}-12-31",
            f"topic:shooter pushed:{year}-01-01..{year}-12-31",
            f"topic:racing-game pushed:{year}-01-01..{year}-12-31",
            f"topic:roguelike pushed:{year}-01-01..{year}-12-31",
            f"topic:tower-defense pushed:{year}-01-01..{year}-12-31",
            f"topic:visual-novel pushed:{year}-01-01..{year}-12-31",
            
            f"game pushed:{year}-01-01..{year}-12-31",
            f"game development pushed:{year}-01-01..{year}-12-31",
            f"game engine pushed:{year}-01-01..{year}-12-31",
            f"video game pushed:{year}-01-01..{year}-12-31",
            f"indie game pushed:{year}-01-01..{year}-12-31",
            f"mobile game pushed:{year}-01-01..{year}-12-31",
            f"web game pushed:{year}-01-01..{year}-12-31",
            f"unity game pushed:{year}-01-01..{year}-12-31",
            f"unreal game pushed:{year}-01-01..{year}-12-31",
            f"godot game pushed:{year}-01-01..{year}-12-31",
            f"phaser game pushed:{year}-01-01..{year}-12-31",
            
            f"language:c# game pushed:{year}-01-01..{year}-12-31",
            f"language:c++ game pushed:{year}-01-01..{year}-12-31",
            f"language:javascript game pushed:{year}-01-01..{year}-12-31",
            f"language:python game pushed:{year}-01-01..{year}-12-31",
            f"language:java game pushed:{year}-01-01..{year}-12-31",
            f"language:lua game pushed:{year}-01-01..{year}-12-31",
            f"language:gdscript pushed:{year}-01-01..{year}-12-31",
            f"language:typescript game pushed:{year}-01-01..{year}-12-31",
            
            f"topic:opengl pushed:{year}-01-01..{year}-12-31",
            f"topic:vulkan pushed:{year}-01-01..{year}-12-31",
            f"topic:webgl pushed:{year}-01-01..{year}-12-31",
            f"topic:directx pushed:{year}-01-01..{year}-12-31",
            f"topic:game-physics pushed:{year}-01-01..{year}-12-31",
            f"topic:collision-detection pushed:{year}-01-01..{year}-12-31",
            f"topic:pathfinding pushed:{year}-01-01..{year}-12-31",
            f"topic:game-ai pushed:{year}-01-01..{year}-12-31",
            f"topic:multiplayer pushed:{year}-01-01..{year}-12-31",
            f"topic:game-networking pushed:{year}-01-01..{year}-12-31",
            f"topic:pixel-art pushed:{year}-01-01..{year}-12-31",
            f"topic:2d-game pushed:{year}-01-01..{year}-12-31",
            f"topic:3d-game pushed:{year}-01-01..{year}-12-31",
            f"topic:vr-game pushed:{year}-01-01..{year}-12-31",
            f"topic:ar-game pushed:{year}-01-01..{year}-12-31",
            
            f"topic:game-jam pushed:{year}-01-01..{year}-12-31",
            f"topic:ludum-dare pushed:{year}-01-01..{year}-12-31",
            f"topic:html5-game pushed:{year}-01-01..{year}-12-31",
            f"topic:canvas-game pushed:{year}-01-01..{year}-12-31"
        ]
        
        all_repos = {}
        game_repos_data = []
        
        for i, query in enumerate(search_queries, 1):
            logger.info(f"query {i}/{len(search_queries)}: {query}")
            
            try:
                repos = self.search_repositories(query, max_results=120)
                logger.info(f"found {len(repos)} repos")
                
                for repo in repos:
                    full_name = repo.get('full_name')
                    if full_name and full_name not in all_repos:
                        is_related, score, match_details = self.is_game_related(repo)
                        
                        if is_related:
                            repo_info = self.extract_repo_info(repo)
                            repo_info['game_score'] = score
                            repo_info['match_details'] = str(match_details)
                            
                            all_repos[full_name] = repo_info
                            game_repos_data.append(repo_info)
                            
                logger.info(f"current game related repos: {len(game_repos_data)}")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"query failed: {e}")
                continue
                
        df = pd.DataFrame(game_repos_data)
        
        if not df.empty:
            df = df.sort_values('stars', ascending=False).reset_index(drop=True)
            logger.info(f"collected {len(df)} game development related repos")
        else:
            logger.warning("did not find game development related repos")
            
        return df
    
    def save_to_parquet(self, df, year, filename=None, save_dir="/home/strrl/ssd/repo"):

        if filename is None:
            filename = f"github_game_repos_{year}.parquet"
            
        os.makedirs(save_dir, exist_ok=True)
        
        full_path = os.path.join(save_dir, filename)
            
        try:
            df.to_parquet(full_path, index=False)
            logger.info(f"data saved to {full_path}")
            
            # show basic stats
            logger.info(f"total repos: {len(df)}")
            logger.info(f"average stars: {df['stars'].mean():.1f}")
            logger.info(f"most popular languages: {df['language'].value_counts().head()}")
            
            if 'topics' in df.columns:
                all_topics = []
                for topics in df['topics']:
                    all_topics.extend(topics)
                if all_topics:
                    topics_series = pd.Series(all_topics)
                    game_topics_count = topics_series.value_counts().head(15)
                    logger.info(f"top game topics: {game_topics_count}")
            
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
    
    scraper = GitHubGameScraper(github_token=github_token)
    
    df = scraper.search_game_repos(year, max_results=1000)
    
    if not df.empty:
        scraper.save_to_parquet(df, year)
        
        print(f"\ntop 10 game development repos in {year}:")
        print(df[['full_name', 'stars', 'language', 'description']].head(10).to_string())
        
        print(f"\nlanguage distribution:")
        print(df['language'].value_counts().head(10))
        
        all_topics = []
        for topics in df['topics']:
            all_topics.extend(topics)
        
        if all_topics:
            topics_df = pd.Series(all_topics).value_counts().head(20)
            print(f"\ntop 20 game development topics:")
            print(topics_df)
            
        unity_repos = df[df['topics'].apply(lambda x: 'unity' in x or 'unity3d' in x)]
        unreal_repos = df[df['topics'].apply(lambda x: 'unreal-engine' in x)]
        godot_repos = df[df['topics'].apply(lambda x: 'godot' in x)]
        phaser_repos = df[df['topics'].apply(lambda x: 'phaser' in x)]
        pygame_repos = df[df['topics'].apply(lambda x: 'pygame' in x)]
        
        print(f"\ngame engine distribution:")
        print(f"Unity repos: {len(unity_repos)}")
        print(f"Unreal Engine repos: {len(unreal_repos)}")
        print(f"Godot repos: {len(godot_repos)}")
        print(f"Phaser repos: {len(phaser_repos)}")
        print(f"Pygame repos: {len(pygame_repos)}")
        
        indie_repos = df[df['topics'].apply(lambda x: 'indie-game' in x)]
        mobile_repos = df[df['topics'].apply(lambda x: 'mobile-game' in x)]
        web_repos = df[df['topics'].apply(lambda x: 'web-game' in x or 'browser-game' in x)]
        vr_repos = df[df['topics'].apply(lambda x: 'vr-game' in x or 'ar-game' in x)]
        
        print(f"\ngame type distribution:")
        print(f"Indie games: {len(indie_repos)}")
        print(f"Mobile games: {len(mobile_repos)}")
        print(f"Web games: {len(web_repos)}")
        print(f"VR/AR games: {len(vr_repos)}")
        
    else:
        print(f"did not find game development related repos in {year}")

if __name__ == "__main__":
    main()