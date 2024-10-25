from dotenv import load_dotenv
load_dotenv()

from autogen import AssistantAgent, UserProxyAgent, GroupChat, GroupChatManager
import aiohttp
import asyncio
from typing import Dict
from urllib.parse import urlparse
from utils import get_openai_api_key, get_serpapi_key
from openai import AsyncOpenAI
import pandas as pd
import numpy as np

# API Keys
OPENAI_API_KEY = get_openai_api_key()
SERPAPI_KEY = get_serpapi_key()


print("API Keys:")
print(f"OPENAI_API_KEY: {OPENAI_API_KEY}")
print(f"SERPAPI_KEY: {SERPAPI_KEY}")


# LLM config
llm_config = {
    "model": "gpt-4o",
    "config_list": [{"model": "gpt-4o", "api_key": OPENAI_API_KEY}],
}

client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# System messages for each specialized agent
PROFILER_SYSTEM_MSG = """You are a Data Profiling expert. Your tasks are:
1. Analyze CSV data structure
2. Identify data types and patterns
3. Detect potential issues
4. Suggest preprocessing steps
Be concise and focus on actionable insights."""

STRATEGY_SYSTEM_MSG = """You are a Data Visualization Strategist. Your tasks are:
1. Recommend appropriate visualizations based on data
2. Consider statistical relationships
3. Suggest meaningful combinations of variables
4. Focus on business value and insights.
"""




CODE_GENERATOR_SYSTEM_MSG = """You are a Python Code Generation expert. Your tasks are:
1. Read the data from the provided CSV file path
2. Generate Plotly/Dash code for visualizations using the data from the CSV file
3. Implement preprocessing steps if necessary
4. Create interactive dashboard layouts
5. Handle data transformations as needed
Ensure code is efficient, well-documented, and ALWAYS reads data from the provided CSV file path. Do not create mock data."""

QA_SYSTEM_MSG = """Critic: You are a helpful assistant highly skilled in evaluating the quality of a given visualization code by providing a score from 1 (bad) - 10 (good) while providing clear rationale. YOU MUST CONSIDER VISUALIZATION BEST PRACTICES for each evaluation. Specifically, you can carefully evaluate the code across the following dimensions
- bugs (bugs):  are there bugs, logic errors, syntax error or typos? Are there any reasons why the code may fail to compile? How should it be fixed? If ANY bug exists, the bug score MUST be less than 5.
- Data transformation (transformation): Is the data transformed appropriately for the visualization type? E.g., is the dataset appropriated filtered, aggregated, or grouped  if needed? If a date field is used, is the date field first converted to a date object etc?
- Goal compliance (compliance): how well the code meets the specified visualization goals?
- Visualization type (type): CONSIDERING BEST PRACTICES, is the visualization type appropriate for the data and intent? Is there a visualization type that would be more effective in conveying insights? If a different visualization type is more appropriate, the score MUST BE LESS THAN 5.
- Data encoding (encoding): Is the data encoded appropriately for the visualization type?
- aesthetics (aesthetics): Are the aesthetics of the visualization appropriate for the visualization type and the data?

YOU MUST PROVIDE A SCORE for each of the above dimensions.
{bugs: 0, transformation: 0, compliance: 0, type: 0, encoding: 0, aesthetics: 0}
Do not suggest code.
Finally, based on the critique above, suggest a concrete list of actions that the coder should take to improve the code.
When you are done, say "TERMINATE"."""

class AnalyticsPALGroup:
    def __init__(self):
        # Initialize the agents
        self.profiler = AssistantAgent(
            name="data_profiler",
            system_message=PROFILER_SYSTEM_MSG,
            llm_config=llm_config
        )
        
        self.strategist = AssistantAgent(
            name="viz_strategist",
            system_message=STRATEGY_SYSTEM_MSG,
            llm_config=llm_config
        )
        
        self.code_generator = AssistantAgent(
            name="Coder",
            
            llm_config=llm_config
        )
        
        self.qa_agent = AssistantAgent(
            name="Critic",
            system_message=QA_SYSTEM_MSG,
            llm_config=llm_config
        )
        
        # User proxy for executing code
        self.executor = UserProxyAgent(
            name="executor",
            human_input_mode="NEVER",
            code_execution_config={"work_dir": "output", "use_docker": False},
            llm_config=llm_config,
            is_termination_msg=lambda x: "TERMINATE" in x["content"].upper()
        )

        # Setup GroupChat
        self.group_chat = GroupChat(
            agents=[
                self.profiler,
                self.strategist,
                self.code_generator,
                self.executor,
                self.qa_agent
            ],
            messages=[],
            max_round=50,
            speaker_selection_method="auto"
        )

        # Initialize manager
        self.manager = GroupChatManager(
            groupchat=self.group_chat,
            llm_config=llm_config
        )

    def analyze_dataset(self, csv_path: str):
        """Start the group chat analysis process"""
        try:
            # Read sample of data
            df_sample = pd.read_csv(csv_path, nrows=1000)
            data_info = self._get_data_info(df_sample)
            
            # Initiate the group chat with data info
            chat_result = self.executor.initiate_chat(
                self.manager,
                message=f"""
                Please analyze the dataset at '{csv_path}' and create visualizations. Here's the data profile:
                {data_info}
                
                Required steps:
                1. Data Profiler: Analyze and report findings
                2. Viz Strategist: Suggest visualizations (don't use too dense charts; for example, if it's daily data, use weekly visualizations)
                3. Code Generator: Create Plotly code that reads data from '{csv_path}'
                4. Executor: Run the code
                5. QA: Verify results
                Plot appropriate visualizations.
                Save the plot to a file. Print the fields in the dataset before visualizing it.
                Each agent should wait for input from the previous agent before proceeding.
                """
            )
            
            return self._extract_results(chat_result)
            
        except Exception as e:
            return {"error": str(e)}
    
    def _get_data_info(self, df):
        """Generate data profile"""
        return {
            "shape": df.shape,
            "dtypes": df.dtypes.to_dict(),
            "missing": df.isnull().sum().to_dict(),
            "numeric_summary": df.describe().to_dict(),
            "categorical_counts": {
                col: df[col].value_counts().head(5).to_dict()
                for col in df.select_dtypes(include=['object']).columns
            }
        }
    
    def _extract_results(self, chat_result):
        """Extract results from the group chat"""
        messages = chat_result.messages
        return {
            "profile": next((m["content"] for m in messages 
                           if m["role"] == "data_profiler"), None),
            "strategy": next((m["content"] for m in messages 
                            if m["role"] == "viz_strategist"), None),
            "code": next((m["content"] for m in messages 
                         if m["role"] == "code_generator"), None),
            "execution": next((m["content"] for m in messages 
                             if m["role"] == "executor"), None),
            "qa_report": next((m["content"] for m in messages 
                             if m["role"] == "qa_agent"), None)
        }

# Example usage
if __name__ == "__main__":
    # Initialize the PAL Group
    analytics_group = AnalyticsPALGroup()
    
    # Example with Store Sales dataset
    results = analytics_group.analyze_dataset("car_prices.csv")
    
    print("\nAnalysis Results:")
    for key, value in results.items():
        print(f"\n{key.upper()}:")
        print(value)
