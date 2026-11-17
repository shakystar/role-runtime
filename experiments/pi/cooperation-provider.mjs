// Synthetic model; all RPC, history restoration and heartbeats use real pi.
import assert from "node:assert/strict";
import { Type, createAssistantMessageEventStream, getCurrentSystemPrompt } from "@earendil-works/pi-ai";
const text = m => typeof m.content === "string" ? m.content : m.content.filter(p => p.type === "text").map(p => p.text).join("");
export default function (pi) {
  pi.registerTool({name:"coop_probe",label:"Probe",description:"Fixture",parameters:Type.Object({}),async execute(){return {content:[{type:"text",text:"ok"}],details:{}}}});
  pi.registerProvider("cooperation", {
    api:"cooperation",apiKey:"synthetic-not-a-credential",baseUrl:"https://unused.invalid",
    models:[{id:"deterministic",name:"Cooperation fixture",reasoning:false,input:["text"],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:32768,maxTokens:2048}],
    streamSimple(model,context,options){
      const history=context.messages.filter(m=>m.role==="user").map(m=>JSON.parse(text(m)));
      const x=history.at(-1), task=x.task;
      for(const prior of history){assert.equal(prior.task.role,task.role);assert.equal(prior.task.project,task.project);}
      assert.ok(getCurrentSystemPrompt(context.messages).includes(`ROLE_CONTEXT_${task.role}_END`));
      const address=role=>({project:task.project,role,version:task.version,instance:task.instance});
      const index=task.role.split("-").at(-1), requester=address(`requester-${index}`), responder=address(`responder-${index}`);
      const correlation=`request-${index}`;
      let out;
      if(task.role.startsWith("requester-")){
        if(!x.inbox){out={checkpoint:"request-sent",send:[{id:`ask-${index}`,from:requester,to:responder,correlation,body:"please-answer"}],wait:{from:responder,correlation}};}
        else{assert.equal(x.checkpoint,"request-sent");assert.equal(x.inbox.body,"answer");assert.ok(history.length>=2);out={text:`resumed:${task.id}`};}
      } else {
        if(!x.inbox){out={checkpoint:"await-request",wait:{from:requester,correlation}};}
        else{assert.equal(x.inbox.body,"please-answer");assert.equal(x.checkpoint,"await-request");out={text:`answered:${task.id}`,send:[{id:`reply-${index}`,from:responder,to:requester,correlation,body:"answer"}]};}
      }
      const message={role:"assistant",api:model.api,provider:model.provider,model:model.id,content:[{type:"text",text:JSON.stringify(out)}],stopReason:"stop",timestamp:Date.now(),usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
      const stream=createAssistantMessageEventStream();
      setTimeout(()=>{if(options?.signal?.aborted){message.stopReason="aborted";message.content=[];}stream.end(message);},200);
      return stream;
    }
  });
}
