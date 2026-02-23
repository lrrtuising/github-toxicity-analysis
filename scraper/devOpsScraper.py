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

class GitHubDevOpsScraper:
    def __init__(self, github_token=None):
        self.base_url = "https://api.github.com"
        self.headers = {'Accept': 'application/vnd.github+json'}
        
        if github_token:
            self.headers['Authorization'] = f'token {github_token}'
        
        self.description_keywords = [
            'devops', 'ci/cd', 'continuous integration', 'continuous deployment',
            'continuous delivery', 'pipeline', 'automation', 'infrastructure',
            'infrastructure as code', 'iac', 'configuration management',
            'containerization', 'orchestration', 'microservices', 'deployment',
            'monitoring', 'logging', 'observability', 'alerting', 'metrics',
            'cloud', 'aws', 'azure', 'gcp', 'google cloud', 'cloud native',
            'serverless', 'lambda', 'function as a service', 'faas',
            'docker', 'kubernetes', 'k8s', 'container', 'podman', 'containerd',
            'helm', 'kustomize', 'operator', 'service mesh', 'istio', 'linkerd',
            'terraform', 'ansible', 'puppet', 'chef', 'salt', 'cloudformation',
            'pulumi', 'vagrant', 'packer', 'consul', 'vault', 'nomad',
            'jenkins', 'gitlab ci', 'github actions', 'circleci', 'travis ci',
            'azure devops', 'bamboo', 'teamcity', 'buildkite', 'drone',
            'argocd', 'flux', 'spinnaker', 'tekton', 'flagger', 'gitops',
            'prometheus', 'grafana', 'jaeger', 'zipkin', 'elastic', 'elk',
            'splunk', 'datadog', 'new relic', 'dynatrace', 'sentry',
            'nginx', 'apache', 'traefik', 'envoy', 'haproxy', 'load balancer',
            'reverse proxy', 'api gateway', 'service discovery', 'etcd', 'zookeeper',
            'redis', 'rabbitmq', 'kafka', 'message queue', 'event streaming',
            'backup', 'disaster recovery', 'high availability', 'scalability',
            'security', 'compliance', 'policy', 'governance', 'audit',
            'testing', 'integration testing', 'end to end testing', 'smoke testing',
            'performance testing', 'load testing', 'stress testing', 'chaos engineering',
            'site reliability engineering', 'sre', 'platform engineering',
            'infrastructure monitoring', 'application monitoring', 'log management',
            'incident response', 'on-call', 'runbook', 'playbook', 'automation',
            'build automation', 'release management', 'version control', 'git workflow',
            'feature flag', 'blue green deployment', 'canary deployment', 'rolling deployment',
            'immutable infrastructure', 'cattle not pets', 'phoenix servers',
            'cloud migration', 'lift and shift', 'refactoring', 'modernization',
            'cost optimization', 'resource management', 'capacity planning',
            'environment management', 'staging', 'production', 'development environment'
        ]
        
        self.devops_topics = [
            'devops', 'ci-cd', 'continuous-integration', 'continuous-deployment',
            'continuous-delivery', 'pipeline', 'automation', 'infrastructure',
            'infrastructure-as-code', 'iac', 'configuration-management',
            'containerization', 'orchestration', 'microservices', 'deployment',
            'monitoring', 'logging', 'observability', 'alerting', 'metrics',
            'cloud', 'aws', 'azure', 'gcp', 'google-cloud', 'cloud-native',
            'serverless', 'lambda', 'faas', 'function-as-a-service',
            'docker', 'kubernetes', 'k8s', 'container', 'podman', 'containerd',
            'helm', 'kustomize', 'operator', 'service-mesh', 'istio', 'linkerd',
            'terraform', 'ansible', 'puppet', 'chef', 'salt', 'cloudformation',
            'pulumi', 'vagrant', 'packer', 'consul', 'vault', 'nomad',
            'jenkins', 'gitlab-ci', 'github-actions', 'circleci', 'travis-ci',
            'azure-devops', 'bamboo', 'teamcity', 'buildkite', 'drone',
            'argocd', 'flux', 'spinnaker', 'tekton', 'flagger', 'gitops',
            'prometheus', 'grafana', 'jaeger', 'zipkin', 'elastic', 'elk',
            'splunk', 'datadog', 'new-relic', 'dynatrace', 'sentry',
            'nginx', 'apache', 'traefik', 'envoy', 'haproxy', 'load-balancer',
            'reverse-proxy', 'api-gateway', 'service-discovery', 'etcd', 'zookeeper',
            'redis', 'rabbitmq', 'kafka', 'message-queue', 'event-streaming',
            'backup', 'disaster-recovery', 'high-availability', 'scalability',
            'security', 'compliance', 'policy', 'governance', 'audit',
            'testing', 'integration-testing', 'e2e-testing', 'smoke-testing',
            'performance-testing', 'load-testing', 'stress-testing', 'chaos-engineering',
            'site-reliability-engineering', 'sre', 'platform-engineering',
            'infrastructure-monitoring', 'application-monitoring', 'log-management',
            'incident-response', 'on-call', 'runbook', 'playbook', 'automation',
            'build-automation', 'release-management', 'version-control', 'git-workflow',
            'feature-flag', 'blue-green-deployment', 'canary-deployment', 'rolling-deployment',
            'immutable-infrastructure', 'cattle-not-pets', 'phoenix-servers',
            'cloud-migration', 'lift-and-shift', 'refactoring', 'modernization',
            'cost-optimization', 'resource-management', 'capacity-planning',
            'environment-management', 'staging', 'production', 'development-environment',
            'yaml', 'json', 'configuration', 'config', 'template', 'manifest',
            'cluster', 'node', 'pod', 'service', 'ingress', 'deployment-strategy',
            'workflow', 'pipeline-as-code', 'build-pipeline', 'release-pipeline',
            'secret-management', 'certificate-management', 'tls', 'ssl',
            'networking', 'vpc', 'subnet', 'firewall', 'security-group',
            'database', 'storage', 'volume', 'persistent-volume', 'statefulset',
            'cronjob', 'job', 'batch', 'scheduler', 'cron', 'timer'
        ]
        
        self.devops_languages = [
            'YAML', 'JSON', 'Shell', 'Python', 'Go', 'JavaScript', 'TypeScript',
            'PowerShell', 'Bash', 'Dockerfile', 'HCL', 'Groovy', 'Ruby',
            'Perl', 'Lua', 'Makefile', 'Jsonnet', 'Starlark', 'Jinja',
            'Terraform', 'Ansible', 'Puppet', 'Chef', 'Salt'
        ]
        
        self.devops_tools = [
            'docker', 'kubernetes', 'terraform', 'ansible', 'jenkins', 'gitlab',
            'prometheus', 'grafana', 'helm', 'istio', 'argocd', 'flux',
            'vault', 'consul', 'nginx', 'traefik', 'envoy'
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
    
    def is_devops_related(self, repo):
        score = 0
        match_details = {
            'description_matches': [],
            'topic_matches': [],
            'name_matches': [],
            'language_matches': [],
            'tool_matches': []
        }
        
        name = repo.get('name', '').lower()
        devops_name_keywords = [
            'devops', 'ci', 'cd', 'deploy', 'infrastructure', 'automation',
            'docker', 'kubernetes', 'k8s', 'terraform', 'ansible', 'jenkins',
            'pipeline', 'monitoring', 'prometheus', 'grafana', 'helm',
            'operator', 'controller', 'config', 'manifest', 'chart',
            'template', 'tool', 'cli', 'server', 'agent', 'daemon'
        ]
        for keyword in devops_name_keywords:
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
            if topic in self.devops_topics:
                score += 4
                match_details['topic_matches'].append(topic)
        
        language = repo.get('language', '')
        if language in self.devops_languages:
            score += 1
            match_details['language_matches'].append(language)
            
            if language in ['YAML', 'HCL', 'Dockerfile', 'Shell', 'Go']:
                score += 2
        
        if description:
            description_lower = description.lower()
            for tool in self.devops_tools:
                if tool in description_lower:
                    score += 3
                    match_details['tool_matches'].append(tool)
        
        if topics:
            high_value_topics = [
                'devops', 'ci-cd', 'kubernetes', 'docker', 'terraform',
                'ansible', 'jenkins', 'infrastructure-as-code', 'monitoring'
            ]
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
    
    def search_devops_repos(self, year, max_results=1000):

        logger.info(f"start searching DevOps related repos in {year}...")

        search_queries = [
            f"topic:devops pushed:{year}-01-01..{year}-12-31",
            f"topic:ci-cd pushed:{year}-01-01..{year}-12-31",
            f"topic:continuous-integration pushed:{year}-01-01..{year}-12-31",
            f"topic:continuous-deployment pushed:{year}-01-01..{year}-12-31",
            f"topic:infrastructure-as-code pushed:{year}-01-01..{year}-12-31",
            f"topic:kubernetes pushed:{year}-01-01..{year}-12-31",
            f"topic:docker pushed:{year}-01-01..{year}-12-31",
            f"topic:terraform pushed:{year}-01-01..{year}-12-31",
            f"topic:ansible pushed:{year}-01-01..{year}-12-31",
            f"topic:jenkins pushed:{year}-01-01..{year}-12-31",
            f"topic:helm pushed:{year}-01-01..{year}-12-31",
            f"topic:monitoring pushed:{year}-01-01..{year}-12-31",
            f"topic:prometheus pushed:{year}-01-01..{year}-12-31",
            f"topic:grafana pushed:{year}-01-01..{year}-12-31",
            f"topic:gitops pushed:{year}-01-01..{year}-12-31",
            f"topic:argocd pushed:{year}-01-01..{year}-12-31",
            f"topic:flux pushed:{year}-01-01..{year}-12-31",
            
            f"topic:aws pushed:{year}-01-01..{year}-12-31",
            f"topic:azure pushed:{year}-01-01..{year}-12-31",
            f"topic:gcp pushed:{year}-01-01..{year}-12-31",
            f"topic:google-cloud pushed:{year}-01-01..{year}-12-31",
            f"topic:cloud-native pushed:{year}-01-01..{year}-12-31",
            f"topic:serverless pushed:{year}-01-01..{year}-12-31",
            f"topic:lambda pushed:{year}-01-01..{year}-12-31",
            
            f"devops pushed:{year}-01-01..{year}-12-31",
            f"ci/cd pushed:{year}-01-01..{year}-12-31",
            f"continuous integration pushed:{year}-01-01..{year}-12-31",
            f"continuous deployment pushed:{year}-01-01..{year}-12-31",
            f"infrastructure as code pushed:{year}-01-01..{year}-12-31",
            f"kubernetes operator pushed:{year}-01-01..{year}-12-31",
            f"docker compose pushed:{year}-01-01..{year}-12-31",
            f"terraform module pushed:{year}-01-01..{year}-12-31",
            f"ansible playbook pushed:{year}-01-01..{year}-12-31",
            f"jenkins plugin pushed:{year}-01-01..{year}-12-31",
            f"helm chart pushed:{year}-01-01..{year}-12-31",
            f"monitoring tool pushed:{year}-01-01..{year}-12-31",
            f"deployment automation pushed:{year}-01-01..{year}-12-31",
            f"infrastructure automation pushed:{year}-01-01..{year}-12-31",
            
            f"topic:github-actions pushed:{year}-01-01..{year}-12-31",
            f"topic:gitlab-ci pushed:{year}-01-01..{year}-12-31",
            f"topic:circleci pushed:{year}-01-01..{year}-12-31",
            f"topic:travis-ci pushed:{year}-01-01..{year}-12-31",
            f"topic:azure-devops pushed:{year}-01-01..{year}-12-31",
            f"topic:buildkite pushed:{year}-01-01..{year}-12-31",
            f"topic:tekton pushed:{year}-01-01..{year}-12-31",
            f"topic:spinnaker pushed:{year}-01-01..{year}-12-31",
            
            f"topic:puppet pushed:{year}-01-01..{year}-12-31",
            f"topic:chef pushed:{year}-01-01..{year}-12-31",
            f"topic:salt pushed:{year}-01-01..{year}-12-31",
            f"topic:consul pushed:{year}-01-01..{year}-12-31",
            f"topic:vault pushed:{year}-01-01..{year}-12-31",
            f"topic:etcd pushed:{year}-01-01..{year}-12-31",
            
            f"topic:observability pushed:{year}-01-01..{year}-12-31",
            f"topic:logging pushed:{year}-01-01..{year}-12-31",
            f"topic:alerting pushed:{year}-01-01..{year}-12-31",
            f"topic:metrics pushed:{year}-01-01..{year}-12-31",
            f"topic:jaeger pushed:{year}-01-01..{year}-12-31",
            f"topic:zipkin pushed:{year}-01-01..{year}-12-31",
            f"topic:elastic pushed:{year}-01-01..{year}-12-31",
            f"topic:elk pushed:{year}-01-01..{year}-12-31",
            f"topic:fluentd pushed:{year}-01-01..{year}-12-31",
            f"topic:logstash pushed:{year}-01-01..{year}-12-31",
            
            f"topic:nginx pushed:{year}-01-01..{year}-12-31",
            f"topic:traefik pushed:{year}-01-01..{year}-12-31",
            f"topic:envoy pushed:{year}-01-01..{year}-12-31",
            f"topic:istio pushed:{year}-01-01..{year}-12-31",
            f"topic:linkerd pushed:{year}-01-01..{year}-12-31",
            f"topic:service-mesh pushed:{year}-01-01..{year}-12-31",
            f"topic:api-gateway pushed:{year}-01-01..{year}-12-31",
            f"topic:load-balancer pushed:{year}-01-01..{year}-12-31",
            
            f"language:yaml kubernetes pushed:{year}-01-01..{year}-12-31",
            f"language:hcl terraform pushed:{year}-01-01..{year}-12-31",
            f"language:dockerfile pushed:{year}-01-01..{year}-12-31",
            f"language:shell devops pushed:{year}-01-01..{year}-12-31",
            f"language:go kubernetes pushed:{year}-01-01..{year}-12-31",
            f"language:python ansible pushed:{year}-01-01..{year}-12-31",
            f"language:groovy jenkins pushed:{year}-01-01..{year}-12-31",
            
            f"topic:chaos-engineering pushed:{year}-01-01..{year}-12-31",
            f"topic:performance-testing pushed:{year}-01-01..{year}-12-31",
            f"topic:load-testing pushed:{year}-01-01..{year}-12-31",
            f"topic:integration-testing pushed:{year}-01-01..{year}-12-31",
            f"topic:security-scanning pushed:{year}-01-01..{year}-12-31",
            f"topic:vulnerability-scanning pushed:{year}-01-01..{year}-12-31",
            
            f"topic:blue-green-deployment pushed:{year}-01-01..{year}-12-31",
            f"topic:canary-deployment pushed:{year}-01-01..{year}-12-31",
            f"topic:rolling-deployment pushed:{year}-01-01..{year}-12-31",
            f"topic:feature-flag pushed:{year}-01-01..{year}-12-31",
            f"topic:release-management pushed:{year}-01-01..{year}-12-31"
        ]
        
        all_repos = {}
        devops_repos_data = []
        
        for i, query in enumerate(search_queries, 1):
            logger.info(f"query {i}/{len(search_queries)}: {query}")
            
            try:
                repos = self.search_repositories(query, max_results=100)
                logger.info(f"found {len(repos)} repos")
                
                for repo in repos:
                    full_name = repo.get('full_name')
                    if full_name and full_name not in all_repos:
                        is_related, score, match_details = self.is_devops_related(repo)
                        
                        if is_related:
                            repo_info = self.extract_repo_info(repo)
                            repo_info['devops_score'] = score
                            repo_info['match_details'] = str(match_details)
                            
                            all_repos[full_name] = repo_info
                            devops_repos_data.append(repo_info)
                            
                logger.info(f"current DevOps related repos: {len(devops_repos_data)}")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"query failed: {e}")
                continue
        
        df = pd.DataFrame(devops_repos_data)
        
        if not df.empty:
            df = df.sort_values('stars', ascending=False).reset_index(drop=True)
            logger.info(f"collected {len(df)} DevOps related repos")
        else:
            logger.warning("did not find DevOps related repos")
            
        return df
    
    def save_to_parquet(self, df, year, filename=None, save_dir="/home/strrl/ssd/repo"):
        if filename is None:
            filename = f"github_devops_repos_{year}.parquet"
            
        os.makedirs(save_dir, exist_ok=True)
        
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
                    devops_topics_count = topics_series.value_counts().head(15)
                    logger.info(f"top DevOps topics: {devops_topics_count}")
            
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
    
    scraper = GitHubDevOpsScraper(github_token=github_token)
    
    df = scraper.search_devops_repos(year, max_results=1000)
    
    if not df.empty:
        scraper.save_to_parquet(df, year)
        
        print(f"\ntop 10 DevOps repos in {year}:")
        print(df[['full_name', 'stars', 'language', 'description']].head(10).to_string())
        
        print(f"\nlanguage distribution:")
        print(df['language'].value_counts().head(10))
        
        all_topics = []
        for topics in df['topics']:
            all_topics.extend(topics)
        
        if all_topics:
            topics_df = pd.Series(all_topics).value_counts().head(20)
            print(f"\ntop 20 DevOps topics:")
            print(topics_df)
            
        kubernetes_repos = df[df['topics'].apply(lambda x: 'kubernetes' in x or 'k8s' in x)]
        docker_repos = df[df['topics'].apply(lambda x: 'docker' in x or 'container' in x)]
        terraform_repos = df[df['topics'].apply(lambda x: 'terraform' in x or 'infrastructure-as-code' in x)]
        ansible_repos = df[df['topics'].apply(lambda x: 'ansible' in x)]
        jenkins_repos = df[df['topics'].apply(lambda x: 'jenkins' in x)]
        monitoring_repos = df[df['topics'].apply(lambda x: any(topic in x for topic in ['prometheus', 'grafana', 'monitoring', 'observability']))]
        
        print(f"\nDevOps tools distribution:")
        print(f"Kubernetes repos: {len(kubernetes_repos)}")
        print(f"Docker repos: {len(docker_repos)}")
        print(f"Terraform repos: {len(terraform_repos)}")
        print(f"Ansible repos: {len(ansible_repos)}")
        print(f"Jenkins repos: {len(jenkins_repos)}")
        print(f"Monitoring repos: {len(monitoring_repos)}")
        
        aws_repos = df[df['topics'].apply(lambda x: 'aws' in x)]
        azure_repos = df[df['topics'].apply(lambda x: 'azure' in x)]
        gcp_repos = df[df['topics'].apply(lambda x: 'gcp' in x or 'google-cloud' in x)]
        cloud_native_repos = df[df['topics'].apply(lambda x: 'cloud-native' in x or 'serverless' in x)]
        
        print(f"\ncloud platform distribution:")
        print(f"AWS repos: {len(aws_repos)}")
        print(f"Azure repos: {len(azure_repos)}")
        print(f"GCP repos: {len(gcp_repos)}")
        print(f"Cloud Native repos: {len(cloud_native_repos)}")
        
        github_actions_repos = df[df['topics'].apply(lambda x: 'github-actions' in x)]
        gitlab_ci_repos = df[df['topics'].apply(lambda x: 'gitlab-ci' in x)]
        jenkins_ci_repos = df[df['topics'].apply(lambda x: 'jenkins' in x)]
        circleci_repos = df[df['topics'].apply(lambda x: 'circleci' in x)]
        
        print(f"\nCI/CD platform distribution:")
        print(f"GitHub Actions repos: {len(github_actions_repos)}")
        print(f"GitLab CI repos: {len(gitlab_ci_repos)}")
        print(f"Jenkins repos: {len(jenkins_ci_repos)}")
        print(f"CircleCI repos: {len(circleci_repos)}")
        
    else:
        print(f"did not find DevOps related repos in {year}")

if __name__ == "__main__":
    main()