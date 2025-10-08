show secrets;
show api integrations;

    CREATE OR REPLACE GIT REPOSITORY WorkspaceFinancial
    API_INTEGRATION = TSPANNSECUREGITHUB
    GIT_URL = 'https://github.com/tspannhw/SnowflakeWorkspaceFinancial.git'
    SECRET = 'DEMO.AGENTS.githubdemo';


    USE ROLE ACCOUNTADMIN;
USE SCHEMA DEMO.DEMO;

CREATE OR REPLACE API INTEGRATION demo_github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/tspannhw') 
  ENABLED = TRUE;

  
  CREATE OR REPLACE SECRET demo_github_secret
   TYPE = password
   USERNAME = 'tspannhw' 
   PASSWORD = 'DFDF'; 



  CREATE OR REPLACE API INTEGRATION demo_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/tspannhw')
  ALLOWED_AUTHENTICATION_SECRETS = (demo_github_secret)
  ENABLED = TRUE;
  
CREATE OR REPLACE GIT REPOSITORY demo_github_repo
  API_INTEGRATION = demo_github_api_integration
  GIT_CREDENTIALS = demo_github_secret
  ORIGIN = 'https://github.com/tspannhw/SnowflakeWorkspaceFinancial.git';

