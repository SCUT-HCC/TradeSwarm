from langchain_openai import ChatOpenAI
import os
from dotenv import load_dotenv
from langchain.messages import HumanMessage, AIMessage, SystemMessage

load_dotenv()

llm = ChatOpenAI(
    # 若没有配置环境变量，请用阿里云百炼API Key将下行替换为：api_key="sk-xxx",
    # 新加坡和北京地域的API Key不同。获取API Key：https://help.aliyun.com/zh/model-studio/get-api-key
    api_key=os.getenv("DASHSCOPE_API_KEY"),
    # 以下是北京地域base_url，如果使用新加坡地域的模型，需要将base_url替换为：https://dashscope-intl.aliyuncs.com/compatible-mode/v1
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    model="qwen-plus"    # 此处以qwen-plus为例，可按需更换模型名称。模型列表：https://help.aliyun.com/zh/model-studio/getting-started/models
    )

system_msg = SystemMessage("You are a helpful assistant.")
human_msg = HumanMessage("how are you")

# Use with chat models
messages = [system_msg, human_msg]

# response = llm.invoke(messages)

# 测试代码已注释
# from langchain.agents import create_agent
# 
# agent = create_agent(model=llm)
# response = agent.invoke(input= {
#     "messages": messages
# })
# 
# print(response)




