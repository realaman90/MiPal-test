import os

def get_env_variable(var_name: str) -> str:
    """
    Get an environment variable and provide helpful error messages.
    """
    value = os.getenv(var_name)
    if value is None:
        print(f"Warning: {var_name} is not set in the environment.")
        return ""
    return value

def get_openai_api_key() -> str:
    """Get the OpenAI API key from the environment."""
    return get_env_variable('OPENAI_API_KEY')

def get_serpapi_key() -> str:
    """Get the SerpApi key from the environment."""
    return get_env_variable('SERPAPI_KEY')

def get_fc_api_key() -> str:
    """Get the Firecrawl API key from the environment."""
    return get_env_variable('FIRECRAWL_API_KEY')

# Debug function to print all environment variables
def print_all_env_variables():
    print("All environment variables:")
    for key, value in os.environ.items():
        print(f"{key}: {value}")

# Call this function to see all environment variables
print_all_env_variables()
