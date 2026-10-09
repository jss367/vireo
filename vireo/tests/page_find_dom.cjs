// Minimal DOM model: tests text/routing logic, not browser layout or native delivery.
const fs=require('fs');
const vm=require('vm');
const assert=require('node:assert/strict');
const root=process.argv[2] || require('node:path').resolve(__dirname, '../static');
function setup(text='Needle needle') {
 let doc,observer;
 class N {
  constructor(tag,type=1,value=''){this.tagName=tag.toUpperCase();this.nodeType=type;this.nodeValue=value;this.children=[];this.parentNode=null;this.hidden=false;this.attributes={};this.listeners={};this.style={};this.namespaceURI='http://www.w3.org/1999/xhtml';this.value='';this.classes=new Set();this.classList={add:x=>this.classes.add(x),remove:x=>this.classes.delete(x)};}
  get childNodes(){return this.children;}
  get parentElement(){return this.parentNode?.nodeType===1?this.parentNode:null;}
  get isConnected(){let n=this;while(n.parentNode)n=n.parentNode;return n===doc.body;}
  get textContent(){return this.nodeType===3?this.nodeValue:this.children.map(n=>n.textContent).join('');}
  set textContent(v){this.children.forEach(n=>n.parentNode=null);this.children=[];this.appendChild(new N('',3,String(v)));}
  appendChild(n){if(n.nodeType===11){for(const c of [...n.children])this.appendChild(c);}else{n.parentNode=this;this.children.push(n);}return n;}
  replaceChild(n,old){const i=this.children.indexOf(old);assert(i>=0);const replacements=n.nodeType===11?[...n.children]:[n];replacements.forEach(c=>c.parentNode=this);this.children.splice(i,1,...replacements);old.parentNode=null;}
  normalize(){for(let i=this.children.length-1;i>0;i--){const a=this.children[i-1],b=this.children[i];if(a.nodeType===3&&b.nodeType===3){a.nodeValue+=b.nodeValue;this.children.splice(i,1);b.parentNode=null;}}}
  setAttribute(k,v){this.attributes[k]=v;if(k==='class')this.classes=new Set(v.split(' '));}
  getAttribute(k){return this.attributes[k]??null;}
  contains(target){return target===this||this.children.some(c=>c.contains(target));}
  closest(){for(let n=this;n;n=n.parentElement){if(n.id==='pageFindPanel'||['SCRIPT','STYLE','NOSCRIPT','TEXTAREA','INPUT','SELECT','TITLE','DESC'].includes(n.tagName)||n.attributes.contenteditable!==undefined)return n;}return null;}
  getClientRects(){return this.hidden||this.style.display==='none'?[]:[{}];}
  focus(){doc.activeElement=this;}
  select(){this.selected=true;}
  scrollIntoView(){this.scrolled=true;}
  addEventListener(name,fn){(this.listeners[name]??=[]).push(fn);}
  fire(name,e={}){for(const fn of this.listeners[name]??[])fn(e);}
 }
 const elements={};
 doc={body:new N('body'),listeners:[],activeElement:null,filter:null,getElementById:id=>elements[id],querySelector:()=>doc.filter,createTextNode:s=>new N('',3,s),createDocumentFragment:()=>new N('',11),createElement:t=>new N(t),createElementNS:(ns,t)=>{const e=new N(t);e.namespaceURI=ns;return e;},addEventListener:(name,fn)=>{if(name==='keydown')doc.listeners.push(fn);},createTreeWalker:(body,type,filter)=>{const nodes=[];function visit(n){if(n.nodeType===3&&filter.acceptNode(n)===1)nodes.push(n);n.children.forEach(visit);}visit(body);let i=-1;return{currentNode:null,nextNode(){this.currentNode=nodes[++i];return !!this.currentNode;}};}};
 for(const id of ['pageFindPanel','pageFindInput','pageFindStatus','pageFindPrevious','pageFindNext','pageFindClose']){elements[id]=new N(id==='pageFindInput'?'input':'div');elements[id].id=id;}
 doc.body.appendChild(elements.pageFindPanel);for(const id of Object.keys(elements).slice(1))elements.pageFindPanel.appendChild(elements[id]);elements.pageFindPanel.hidden=true;
 const previous=new N('button');doc.body.appendChild(previous);doc.activeElement=previous;
 const paragraph=new N('p');paragraph.textContent=text;doc.body.appendChild(paragraph);
 const timers=new Map();let token=0;
 class KeyboardEvent {constructor(type,init){Object.assign(this,init);this.type=type;}preventDefault(){}stopPropagation(){}}
 class Observer{constructor(fn){this.fn=fn;observer=this;}observe(){this.active=true;}disconnect(){this.active=false;}}
 const ctx={KeyboardEvent,document:doc,MutationObserver:Observer,NodeFilter:{SHOW_TEXT:4,FILTER_ACCEPT:1,FILTER_REJECT:2},setTimeout:fn=>{timers.set(++token,fn);return token;},clearTimeout:t=>timers.delete(t),console};ctx.window=ctx;ctx.getComputedStyle=e=>({display:e.style.display??(['B','SPAN','EM','STRONG','I','MARK','BR','TSPAN'].includes(e.tagName)?'inline':'block'),visibility:e.style.visibility??'visible'});
 vm.createContext(ctx);for(const f of ['keymap.js','vireo-page-find.js'])vm.runInContext(fs.readFileSync(root+'/'+f,'utf8'),ctx);
 function key(mod={}){const e={key:'f',ctrlKey:true,metaKey:false,altKey:false,shiftKey:false,preventDefault(){this.prevented=true;},stopPropagation(){},stopImmediatePropagation(){this.stopped=true;},...mod};for(const fn of doc.listeners){fn(e);if(e.stopped)break;}return e;}
 function change(q){elements.pageFindInput.value=q;elements.pageFindInput.fire('input');}
 function flush(){for(const [id,fn]of [...timers]){timers.delete(id);fn();}}
 function marks(){const result=[];function visit(n){if(n.classes.has('page-find-mark'))result.push(n);n.children.forEach(visit);}visit(doc.body);return result;}
 return{ctx,doc,elements,paragraph,previous,key,change,flush,marks,mutation:()=>{if(observer.active)observer.fn([{target:paragraph}]);}};
}
let passed=0;function test(name,fn){try{fn();console.log('PASS '+name);passed++;}catch(e){console.log('FAIL '+name+': '+e.message);process.exitCode=1;}}
test('Ctrl/Cmd F opens one panel and navigates wraps',()=>{const a=setup();a.key();a.key({ctrlKey:false,metaKey:true});assert(!a.elements.pageFindPanel.hidden);a.change('needle');assert.equal(a.marks().length,2);assert.equal(a.elements.pageFindStatus.textContent,'1 of 2');a.elements.pageFindInput.fire('keydown',{key:'Enter',preventDefault(){}});assert.equal(a.elements.pageFindStatus.textContent,'2 of 2');a.elements.pageFindInput.fire('keydown',{key:'Enter',shiftKey:true,preventDefault(){}});assert.equal(a.elements.pageFindStatus.textContent,'1 of 2');a.key({key:'Escape',ctrlKey:false});assert(a.elements.pageFindPanel.hidden);assert.equal(a.marks().length,0);assert.equal(a.paragraph.textContent,'Needle needle');assert.equal(a.doc.activeElement,a.previous);});
test('Repeated opens and empty/no-match input clean marks',()=>{const a=setup();for(let i=0;i<3;i++){a.ctx.VireoPageFind.open();a.change('needle');a.ctx.VireoPageFind.close();assert.equal(a.marks().length,0);}a.ctx.VireoPageFind.open();a.change('needle');a.change('absent');assert.equal(a.marks().length,0);assert(a.elements.pageFindNext.disabled);a.change(' ');assert.equal(a.marks().length,0);});
test('Async content updates and close cancel pending refresh',()=>{const a=setup();a.ctx.VireoPageFind.open();a.change('needle');a.paragraph.textContent='Needle';a.mutation();a.flush();assert.equal(a.marks().length,1);a.paragraph.textContent='Needle needle';a.mutation();a.ctx.VireoPageFind.close();a.flush();assert.equal(a.marks().length,0);});
test('Browse and Settings retain their own search',()=>{const a=setup();a.doc.filter={getClientRects:()=>[{}],focus(){this.focused=true;},select(){this.selected=true;}};a.key();assert(a.doc.filter.focused&&a.doc.filter.selected);assert(a.elements.pageFindPanel.hidden);let count=0;a.ctx.openSettingsFind=()=>count++;a.key();assert.equal(count,1);assert(a.elements.pageFindPanel.hidden);});
test('Shortcut capture receives Ctrl F while Keymap paused',()=>{const a=setup();let captured=false;a.ctx.Keymap.pauseDispatch();a.doc.listeners.push(()=>captured=true);a.key();assert(captured,'new find listener swallowed shortcut editor capture');assert(a.elements.pageFindPanel.hidden);});

test('Unicode lowercase expansion preserves following match offsets',()=>{const a=setup('İ Needle needle');a.ctx.VireoPageFind.open();a.change('needle');assert.deepEqual(a.marks().map(m=>m.textContent),['Needle','needle']);assert.equal(a.paragraph.textContent,'İ Needle needle');});

test('Single native Find command completes actual shortcut recorder without keydown',()=>{
 const a=setup();
 a.doc.removeEventListener=(name,fn)=>{a.doc.listeners=a.doc.listeners.filter(f=>f!==fn);};
 const template=fs.readFileSync(root+'/../templates/shortcuts.html','utf8');
 const capture=template.slice(template.indexOf('function startCapture('),template.indexOf('function resetShortcut('));
 let saves=0,renders=0;
 Object.assign(a.ctx,{capturingBtn:null,currentShortcuts:{global:{test:''}},isBarePageShortcut:()=>false,findConflict:()=>null,renderShortcutsEditor:()=>renders++,saveShortcuts:()=>saves++});
 vm.runInContext(capture,a.ctx);
 const blurs=[];const btn={classList:{add(){},remove(){}},style:{},addEventListener(name,fn){if(name==='blur')blurs.push(fn);},removeEventListener(name,fn){const i=blurs.indexOf(fn);if(i>=0)blurs.splice(i,1);}};
 a.ctx.startCapture(btn,'global','test');
 assert(a.ctx.Keymap.isDispatchPaused());
 a.ctx.handleNativeMenuCommand('find');
 assert.equal(a.ctx.currentShortcuts.global.test,'ctrl+f');
 assert.equal(saves,1);assert.equal(renders,1);assert.equal(a.ctx.capturingBtn,null);
 assert(!a.ctx.Keymap.isDispatchPaused());assert(a.elements.pageFindPanel.hidden);
 // Normal Find resumes after capture, and a canceled recorder is not retained.
 a.ctx.handleNativeMenuCommand('find');assert(!a.elements.pageFindPanel.hidden);
 a.ctx.VireoPageFind.close();a.ctx.startCapture(btn,'global','test');for(const fn of [...blurs])fn();
 assert.equal(a.ctx.Keymap.captureNativeShortcut('ctrl+f'),false);assert.equal(saves,1);
});

test('Contiguous query crosses inline markup and counts one logical match',()=>{
 const a=setup('Click ');const bold=a.doc.createElement('b');bold.textContent='Scan for duplicate files';a.paragraph.appendChild(bold);a.paragraph.appendChild(a.doc.createTextNode(' to find duplicates'));
 a.ctx.VireoPageFind.open();a.change('Click Scan');
 assert.equal(a.marks().map(m=>m.textContent).join(''),'Click Scan');
 assert.equal(a.elements.pageFindStatus.textContent,'1 of 1');
 a.change('files to find');assert.equal(a.marks().map(m=>m.textContent).join(''),'files to find');assert.equal(a.elements.pageFindStatus.textContent,'1 of 1');
 a.ctx.VireoPageFind.close();assert.equal(a.paragraph.textContent,'Click Scan for duplicate files to find duplicates');assert.equal(bold.textContent,'Scan for duplicate files');
});

test('Repeated split phrases navigate by logical match and restore markup',()=>{
 const a=setup('Click ');const b=a.doc.createElement('b');b.textContent='Scan';a.paragraph.appendChild(b);a.paragraph.appendChild(a.doc.createTextNode(' then Click '));const em=a.doc.createElement('em');em.textContent='Scan';a.paragraph.appendChild(em);
 a.ctx.VireoPageFind.open();a.change('Click Scan');assert.equal(a.elements.pageFindStatus.textContent,'1 of 2');assert.equal(a.marks().filter(m=>m.classes.has('active')).length,2);
 a.elements.pageFindInput.fire('keydown',{key:'Enter',preventDefault(){}});assert.equal(a.elements.pageFindStatus.textContent,'2 of 2');assert.equal(a.marks().filter(m=>m.classes.has('active')).length,2);
 a.change('Click');a.change('Click Scan');a.ctx.VireoPageFind.close();assert.equal(a.paragraph.textContent,'Click Scan then Click Scan');assert.equal(b.textContent,'Scan');assert.equal(em.textContent,'Scan');assert.equal(a.marks().length,0);
});
test('Separate blocks and explicit BR never form a phrase',()=>{
 const a=setup('Click ');const next=a.doc.createElement('p');next.textContent='Scan';a.doc.body.appendChild(next);a.ctx.VireoPageFind.open();a.change('Click Scan');assert.equal(a.marks().length,0);
 a.paragraph.appendChild(a.doc.createElement('br'));a.paragraph.appendChild(a.doc.createTextNode('Scan'));a.change('Click Scan');assert.equal(a.marks().length,0);
});
test('Standalone whitespace text between inline elements joins phrase',()=>{
 const a=setup('');const b=a.doc.createElement('b');b.textContent='Click';a.paragraph.appendChild(b);a.paragraph.appendChild(a.doc.createTextNode(' '));const em=a.doc.createElement('em');em.textContent='Scan';a.paragraph.appendChild(em);a.ctx.VireoPageFind.open();a.change('Click Scan');assert.equal(a.elements.pageFindStatus.textContent,'1 of 1');assert.equal(a.marks().map(m=>m.textContent).join(''),'Click Scan');
});

test('Hidden inline content prevents a phrase and stays untouched',()=>{
 const a=setup('Click ');const hidden=a.doc.createElement('span');hidden.hidden=true;hidden.textContent='Secret';a.paragraph.appendChild(hidden);a.paragraph.appendChild(a.doc.createTextNode('Scan'));a.ctx.VireoPageFind.open();a.change('Click Scan');assert.equal(a.marks().length,0);a.change('Secret');assert.equal(a.marks().length,0);assert.equal(hidden.textContent,'Secret');
});
test('SVG label matches use tspan and restore chart text',()=>{
 const a=setup('');const label=a.doc.createElementNS('http://www.w3.org/2000/svg','text');label.textContent='Needle';a.doc.body.appendChild(label);a.ctx.VireoPageFind.open();a.change('needle');assert.equal(a.marks().length,1);assert.equal(a.marks()[0].tagName,'TSPAN');a.ctx.VireoPageFind.close();assert.equal(label.textContent,'Needle');
});
console.log(passed+' mock-DOM scenarios passed');
