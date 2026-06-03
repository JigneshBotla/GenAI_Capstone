from agent_core import AgentCore

def test_agent_locally():
    print("Initializing Deco Agent Core...")
    agent = AgentCore()
    
    prompts = [
        "What is the schema and PII masking status of staging.stg_users?",
        "Show me data lineage for marts.fct_user_churn.",
        "My pipeline broke. Can you check what happened and suggest a fix?",
        "Trigger a data quality check on staging.stg_users"
    ]
    
    for i, p in enumerate(prompts):
        print(f"\n--- Prompt {i+1}: '{p}' ---")
        response = agent.run_agent(p)
        print(response)
        
if __name__ == "__main__":
    test_agent_locally()
