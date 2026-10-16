// Shared JS execution boundaries. The host supplies the existing collector transport; no storage or network here.
(function(root){
'use strict';
function create({emit,id,clock=()=>performance.now(),now=()=>Date.now(),schedule=setTimeout,cancel=clearTimeout}){
  const run=id(),metrics=new Map();let current=null,serial=0,lost=0,diagnostic=null,pending=0;
  if(!/^[a-f0-9]{32}$/.test(run))throw Error('invalid telemetry run');
  const code=name=>typeof name==='string'&&name.length>0&&name.length<=128&&/^[a-zA-Z0-9._:-]+$/.test(name);
  const publish=row=>{try{if(emit(row)!==false)return true;}catch(_){}lost++;return false;};
  const scope=()=>({trace:current?.trace||run+'.'+(++serial),span:run+'.'+(++serial),parent:current?.span||null});
  function invoke(name,fn,receiver,args,action=false){
    const metric=metrics.get(name);if(metric)metric.calls++;
    const selected=!action&&diagnostic&&diagnostic.remaining>0&&(diagnostic.selector==='*'||name===diagnostic.selector||name.startsWith(diagnostic.selector+'.'))&&clock()<diagnostic.until;
    if(selected)diagnostic.remaining--;
    const start=action||selected?clock():null,at=start===null?null:now(),previous=current;
    const context=action||selected?scope():current;
    if(context)current=context;
    if(action)publish({kind:'ui.start',operation:name,...context,started_ms:at,outcome:'unknown',error:'none'});
    let failed=false,unknown=false;
    try{const result=Reflect.apply(fn,receiver,args);unknown=result instanceof Promise;if(unknown){if(metric)metric.unknown++;lost++;}return result;}
    catch(error){failed=true;if(metric)metric.failed++;throw error;}
    finally{
      if(metric)metric.finished++;
      if(action||selected)publish({kind:action?'ui':'method',source:action?'action':'diagnostic',operation:name,...context,started_ms:at,duration_us:Math.max(0,Math.round((clock()-start)*1000)),outcome:failed?'failed':unknown?'unknown':'ok',error:failed?'javascript-error':unknown?'unowned-async':'none'});
      current=previous;
    }
  }
  function module(name,methods){
    if(!code(name))throw Error('invalid telemetry module');
    const wrapped={};
    for(const [key,descriptor] of Object.entries(Object.getOwnPropertyDescriptors(methods))){
      if(typeof descriptor.value!=='function'||descriptor.value.constructor.name==='AsyncFunction')throw Error('module methods must be functions');
      const operation='js.'+name+'.'+key;
      if(!code(operation))throw Error('invalid telemetry method');
      if(!metrics.has(operation)){
        if(metrics.size>=256)throw Error('telemetry method limit');
        metrics.set(operation,{calls:0,finished:0,failed:0,unknown:0});
      }
      Object.defineProperty(wrapped,key,{...descriptor,value:function(...args){return invoke(operation,descriptor.value,this,args);}});
    }
    return Object.freeze(wrapped);
  }
  function action(name,fn){
    if(!code('js.'+name))throw Error('invalid telemetry action');
    return function(...args){return invoke('js.'+name,fn,this,args,true);};
  }
  function effect(name,start,receive){
    if(!code('js.'+name)||typeof start!=='function'||typeof receive!=='function'||receive.constructor.name==='AsyncFunction'||pending>=64)throw Error('async effect unavailable');
    const previous=current,context=scope(),at=now(),began=clock();let waiting=true;
    pending++;publish({kind:'effect.start',operation:'js.'+name,...context,started_ms:at,outcome:'unknown',error:'none'});
    const timer=schedule(()=>{
      if(waiting){publish({kind:'effect.wait',operation:'js.'+name,...context,started_ms:at,outcome:'unknown',error:'completion-timeout'});deliver({ok:false,unknown:true});}
    },30000);
    let result;current=context;
    try{result=start();}catch(error){settle(false,error);return;}
    finally{current=previous;}
    // This owner consumes the result; the component receives one attributable completion.
    Promise.resolve(result).then(value=>settle(true,value),error=>settle(false,error));
    function deliver(result){
      const outer=current;current=context;
      try{invoke('js.'+name+'.result',receive,undefined,[result],true);}
      catch(_){/* callback failure is recorded; never reject an unowned Promise */}
      finally{current=outer;}
    }
    function settle(ok,value){
      if(!waiting)return;waiting=false;pending--;cancel(timer);
      publish({kind:'effect',operation:'js.'+name,...context,started_ms:at,duration_us:Math.max(0,Math.round((clock()-began)*1000)),outcome:ok?'ok':'failed',error:ok?'none':'javascript-error'});
      deliver({ok,value});
    }
  }
  function bind(callback){
    const context=current;
    return function(...args){const previous=current;current=context;try{return Reflect.apply(callback,this,args);}finally{current=previous;}};
  }
  function diagnose(selector,milliseconds,limit){
    if(selector!=='*'&&!code(selector)||!Number.isInteger(milliseconds)||milliseconds<1||milliseconds>60000||!Number.isInteger(limit)||limit<1||limit>256)throw Error('invalid diagnostic selection');
    selector=selector==='*'?selector:selector.replace(/\.+$/,'');
    if(!selector)throw Error('invalid diagnostic selection');
    diagnostic={selector,until:clock()+milliseconds,remaining:limit};
    publish({kind:'diagnostic',operation:'js.collector.diagnostic',source:selector,started_ms:now(),duration_us:milliseconds*1000,counts:{calls:limit},outcome:'ok',error:'none'});
  }
  function flush(){
    for(const [operation,counts] of metrics){
      if(counts.calls&&publish({kind:'method.aggregate',operation,started_ms:now(),counts:{...counts},outcome:counts.failed?'failed':counts.unknown?'unknown':'ok',error:counts.failed?'javascript-error':counts.unknown?'unowned-async':'none'}))for(const key of Object.keys(counts))counts[key]=0;
    }
  }
  return Object.freeze({module,action,effect,bind,diagnose,flush,context:()=>current&&({...current}),health:()=>({lost,pending,methods:metrics.size,diagnostic:diagnostic?{selector:diagnostic.selector,remaining:diagnostic.remaining,active:clock()<diagnostic.until&&diagnostic.remaining>0}:null})});
}
function shared(){
  if(!root.__coreqExecution){
    root.__coreqExecution=create({emit:row=>root.__coreqExecutionEmit?root.__coreqExecutionEmit(row):false,
      id:()=>Array.from(root.crypto.getRandomValues(new Uint8Array(16)),b=>b.toString(16).padStart(2,'0')).join('')});
  }
  return root.__coreqExecution;
}
function connect(emit){if(typeof emit!=='function')throw Error('invalid telemetry transport');root.__coreqExecutionEmit=emit;return shared();}
const api={create,shared,connect};if(typeof module==='object'&&module.exports)module.exports=api;else root.CoreqExecution=api;
})(typeof globalThis==='object'?globalThis:this);
