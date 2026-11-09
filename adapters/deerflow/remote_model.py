"""配置驱动的真实OpenAI兼容研究模型，保留原生工具调用。"""
import os
from langchain_openai import ChatOpenAI


class OpenAIResearchModel(ChatOpenAI):
    def __init__(self,**kwargs):
        kwargs['model']=os.environ['AGENTCLOUD_REAL_MODEL']
        kwargs['base_url']=os.environ['AGENTCLOUD_REAL_MODEL_URL']
        kwargs['api_key']=os.environ.get('AGENTCLOUD_REAL_MODEL_KEY','local-model')
        kwargs.setdefault('max_tokens',4096)
        kwargs.setdefault('timeout',300)
        kwargs['use_responses_api']=False
        super().__init__(**kwargs)
