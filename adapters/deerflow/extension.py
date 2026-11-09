"""Logical/physical model and raw tool placements using DeerFlow extension contracts."""
from __future__ import annotations
import asyncio
from contextvars import ContextVar
import threading
import uuid
from langchain.agents.middleware import AgentMiddleware
from deerflow_extension_api.placement import AgentScope, MiddlewarePlacement, Placement
from adapters.deerflow.provider import TOOL_LOGICAL_ID, TOOL_SUBOP

MODEL_LOGICAL_ID=ContextVar('agentcloud_model_logical_id',default=None)


class TraceMiddleware(AgentMiddleware):
    def __init__(self,axis,events):
        self.axis,self.events=axis,events
        self.lock=threading.Lock()

    @property
    def name(self):
        return 'agentcloud_'+self.axis

    def record(self,event,**kwargs):
        with self.lock:
            self.events.append(dict(event=event,axis=self.axis,**kwargs))

class ModelTraceMiddleware(TraceMiddleware):
    def wrap_model_call(self,request,handler):
        if self.axis=='model_logical':
            logical=uuid.uuid4().hex
            token=MODEL_LOGICAL_ID.set(logical)
            self.record('logical_model_start',logical_request_id=logical)
            try:
                result=handler(request)
                self.record('logical_model_completed',logical_request_id=logical)
                return result
            finally:
                MODEL_LOGICAL_ID.reset(token)
        self.record('physical_model_attempt',logical_request_id=MODEL_LOGICAL_ID.get(),physical_attempt_id=uuid.uuid4().hex)
        return handler(request)

    async def awrap_model_call(self,request,handler):
        if self.axis=='model_logical':
            logical=uuid.uuid4().hex
            token=MODEL_LOGICAL_ID.set(logical)
            self.record('logical_model_start',logical_request_id=logical)
            try:
                result=await handler(request)
                self.record('logical_model_completed',logical_request_id=logical)
                return result
            finally:
                MODEL_LOGICAL_ID.reset(token)
        self.record('physical_model_attempt',logical_request_id=MODEL_LOGICAL_ID.get(),physical_attempt_id=uuid.uuid4().hex)
        return await handler(request)

class ToolTraceMiddleware(TraceMiddleware):
    def wrap_tool_call(self,request,handler):
        call=getattr(request,'tool_call',{})
        tool_id=call.get('id') or uuid.uuid4().hex
        token=TOOL_LOGICAL_ID.set(tool_id);subtoken=TOOL_SUBOP.set(0)
        self.record('tool_start',tool_op_id=tool_id,tool_name=call.get('name'))
        try:
            result=handler(request)
            self.record('tool_completed',tool_op_id=tool_id,tool_name=call.get('name'),
                output=getattr(result,'content',None))
            return result
        finally:
            TOOL_SUBOP.reset(subtoken);TOOL_LOGICAL_ID.reset(token)

    async def awrap_tool_call(self,request,handler):
        call=getattr(request,'tool_call',{})
        tool_id=call.get('id') or uuid.uuid4().hex
        token=TOOL_LOGICAL_ID.set(tool_id);subtoken=TOOL_SUBOP.set(0)
        self.record('tool_start',tool_op_id=tool_id,tool_name=call.get('name'))
        try:
            result=await handler(request)
            self.record('tool_completed',tool_op_id=tool_id,tool_name=call.get('name'),
                output=getattr(result,'content',None))
            return result
        finally:
            TOOL_SUBOP.reset(subtoken);TOOL_LOGICAL_ID.reset(token)


class AgentCloudExtension:
    def __init__(self):
        self.events=[]

    def contribute_middlewares(self,app_store,ctx):
        return [MiddlewarePlacement(ModelTraceMiddleware('model_logical',self.events),Placement.MODEL_LOGICAL,AgentScope.BOTH),
            MiddlewarePlacement(ModelTraceMiddleware('model_physical',self.events),Placement.MODEL_PHYSICAL,AgentScope.BOTH),
            MiddlewarePlacement(ToolTraceMiddleware('tool_raw',self.events),Placement.TOOL_RAW,AgentScope.BOTH)]

from deerflow_extension_api import extension

@extension(api='0.2.11',name='AgentCloud')
def install(registry,config):
    registry.middlewares(AgentCloudExtension())
