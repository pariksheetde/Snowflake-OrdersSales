-- Create API Integration
CREATE OR REPLACE API INTEGRATION github_api
API_PROVIDER = git_https_api
API_ALLOWED_PREFIXES = ('https://github.com/pariksheetde/')
ENABLED = TRUE;

-- Create Git Repository
CREATE OR REPLACE GIT REPOSITORY `Snowflake-OrdersSales`
API_INTEGRATION = github_api
ORIGIN = 'https://github.com/pariksheetde/Snowflake-OrdersSales.git';

-- Pull latest content
ALTER GIT REPOSITORY `Snowflake-OrdersSales` FETCH;

-- List branches
SHOW GIT BRANCHES IN `Snowflake-OrdersSales`;

-- Browse files
-- LS @'Snowflake-OrdersSales'/branches/main;