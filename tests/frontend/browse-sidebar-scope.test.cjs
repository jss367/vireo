// Selected definitions only: invented DOM, inert transport, no app or browser imports.
const fs=require('fs'),vm=require('vm'),path=require('path'),assert=require('node:assert/strict');
const {test}=require('node:test');
const root=path.resolve(__dirname,'../..');
const {parse}=require('acorn');
function source(file){
  return fs.readFileSync(root+'/vireo/static/browse/'+file,'utf8');
}
function walk(n,fn){
  if(!n||typeof n!=='object')return;
  fn(n);
  Object.values(n).forEach(v=>Array.isArray(v)?v.forEach(x=>walk(x,fn)):walk(v,fn));
}
// The browse files are ES modules: find each function whether or not it is
// exported. Their page state is `state.<field>`; the context is its own
// `state` (see page()), so tests read and set fields on the context.
function declarations(s){
  return parse(s,{
    ecmaVersion:'latest',sourceType:'module'
  }).body.map(n=>n.type==='ExportNamedDeclaration'&&n.declaration?n.declaration:n);
}
function functions(c,file,names){
  const s=source(file),body=declarations(s);
  for(const name of names){
    const n=body.find(n=>n.type==='FunctionDeclaration'&&n.id.name===name);
    assert(n,name);
    vm.runInContext(s.slice(n.start,n.end),c);
  }
}
function node(data={
},active=false){
  let yes=active;
  return {
    dataset:data,style:{
    },classList:{
      toggle:(_n,b)=>{
        yes=b;
      },contains:()=>yes
    },scrollIntoView:()=>{
    }
  };
}
function page(){
  const elements=new Map(),rows={
    folderTree:[],keywordTree:[],collectionList:[]
  },requests=[],rules=[];
  function get(id){
    if(!elements.has(id)){
      const e={
        value:'newest',style:{
        },scrollTop:0
      };
      Object.defineProperty(e,'innerHTML',{
        set(html){
          e.html=html;
          if(!(id in rows))return;
          rows[id]=[];
          for(const match of html.matchAll(/<div\b([^>]*)>/g)){
            const a=match[1];
            if(!/class="tree-item(?:\s|\")/.test(a))continue;
            const ds={
            };
            for(const d of a.matchAll(/data-(folder-id|collection-id|keyword)="([^"]*)"/g))ds[d[1].replace(/-([a-z])/g,(_m,l)=>l.toUpperCase())]=d[2];
            rows[id].push(node(ds,/class="[^"]*\bactive\b/.test(a)));
          }
        }
      });
      elements.set(id,e);
    }return elements.get(id);
  }
  const no=()=>{
  };
  const c=vm.createContext({
    document:{
      getElementById:get,querySelectorAll:sel=>{
        const key=sel.split(' ')[0].slice(1);
        assert(key in rows,sel);
        return rows[key];
      }
    },URLSearchParams,
    activeFolderId:null,activeKeyword:null,activeCollectionId:null,openedCollectionId:null,dashboardCollectionScope:false,browseScopeGen:0,browseFilterInitPromise:null,timelineMode:false,collectionsById:{
    },collectionEditId:null,
    clearOfflineCollectionState:no,loadCalendarData:no,loadSummary:no,browseFilterReloadOptions:()=>({
    }),
    collectionAcceptsManualPhotos:()=>true,escapeHtml:s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;'),escapeAttr:s=>String(s).replaceAll('"','&quot;'),_rememberKeywordNames:no,treeTypeIcons:{
    },folderLocalStatuses:()=>({
      statuses:{
      },synthetic:[]
    }),pendingArchiveFolderStatuses:()=>({
    }),folderLocalStatusMarkup:()=>'',Element:function(){
    },
    getBrowseRules:()=>rules.length?rules:null,browseStacksEnabled:()=>false,hasCollectionAvailabilityScope:()=>false,browseLimits:{maxFocusCandidates:200},loadEpoch:0,
    searchLaneHeaders:()=>({
    }),bindBrowseMarkup:no,
    // The navbar's Missing Originals snapshot (shared.js): none taken yet.
    missingFoldersSnapshotVersion:()=>undefined,_reconcileMissingFoldersInitSnapshot:()=>false,console
  });
  c.window=c;
  c.state=c;
  functions(c,'sidebar-collections.js',['collectionCountMarkup','renderCollectionList','filterByCollection','refreshActiveCollectionAfterMembershipChange','highlightedCollectionId','syncSidebarHighlights']);
  functions(c,'sidebar-folders.js',['filterByFolder','renderFolderTree']);
  functions(c,'sidebar-keywords.js',['filterByKeyword','renderKeywordTree']);
  functions(c,'loading.js',['buildBrowsePageRequest','browseFilterReloadOptions','onBrowseFilterChange','settleSidebarScopeForReload']);
  const boot=source('boot.js');
  const bootstrap=declarations(boot).find(n=>n.type==='FunctionDeclaration'&&n.id.name==='bootstrapBrowse');
  let onChange;
  walk(bootstrap,n=>{
    if(n.type==='Property'&&n.key.name==='onChange'&&!onChange)onChange=n.value;
  });
  assert(onChange);
  vm.runInContext('var testOnChange='+boot.slice(onChange.start,onChange.end),c);
  c.resetAndLoad=opts=>{
    c.settleSidebarScopeForReload(opts&&opts.preserveCollection);
    requests.push('reload');
  };
  c.reloadBrowseResults=()=>c.resetAndLoad({
  });
  c.VireoFilter={
    isReady:()=>true,loadExpression:newRules=>{
      rules.splice(0,rules.length,...newRules);
      c.testOnChange({
        reason:'expressionLoaded'
      });
    },addRule:(field,op,value)=>{
      const i=rules.findIndex(r=>r.field===field&&r.value===value);
      if(i<0)rules.push({
        field,op,value
      });
      else rules.splice(i,1);
      c.testOnChange({
        reason:'ruleChanged'
      });
    },removeField:(field,info)=>{
      const found=rules.some(r=>r.field===field);
      for(let i=rules.length-1;
      i>=0;
      i--)if(rules[i].field===field)rules.splice(i,1);
      if(found)c.testOnChange(info);
      return found;
    },getVisual:()=>null
  };
  const folders=[{
    id:1,name:'one',path:'/invented/one',photo_count:2
  },{
    id:2,name:'two',path:'/invented/two',photo_count:3
  }],keywords=[{
    id:1,name:'Hawk',type:'species'
  },{
    id:2,name:'Robin',type:'species'
  }],collections=[{
    id:11,name:'All Photos',rules:[],photo_count:5
  },{
    id:12,name:'Manual',rules:[{
      field:'rating',op:'gte',value:3
    }],photo_count:2
  }];
  function render(){
    c.renderFolderTree(folders);
    c.renderKeywordTree(keywords);
    c.renderCollectionList(collections);
  }render();
  return {
    c,rows,get,requests,rules,render,folders,keywords,collections,active:()=>Object.entries(rows).flatMap(([tree,rs])=>rs.filter(r=>r.classList.contains('active')).map(r=>[tree,r.dataset]))
  };
}
test('folder → collection → chip edit → keyword paints actual scope exclusively',async()=>{
  const p=page();
  p.c.filterByFolder(1);
  assert.equal(p.active().length,1);
  await p.c.filterByCollection(11);
  assert.equal(p.c.activeFolderId,null);
  assert.deepEqual(p.active(),[['collectionList',{
    collectionId:'11'
  }]]);
  p.c.testOnChange({
    reason:'ruleChanged'
  });
  assert.equal(p.c.openedCollectionId,null);
  assert.equal(p.active().length,0);
  await p.c.filterByCollection(11);
  await p.c.filterByKeyword('Hawk');
  assert.deepEqual(p.active(),[['keywordTree',{
    keyword:'Hawk'
  }]]);
  assert.equal(p.c.openedCollectionId,null);
});
test('repeat folder keyword and collection choices leave no stale tree highlights',async()=>{
  const p=page();
  p.c.filterByFolder(1);
  p.c.filterByFolder(1);
  assert.equal(p.active().length,0);
  await p.c.filterByKeyword('Hawk');
  await p.c.filterByKeyword('Hawk');
  assert.equal(p.active().length,0);
  await p.c.filterByCollection(12);
  await p.c.filterByCollection(12);
  assert.deepEqual(p.active(),[['collectionList',{
    collectionId:'12'
  }]]);
  p.c.filterByFolder(2);
  assert.deepEqual(p.active(),[['folderTree',{
    folderId:'2'
  }]]);
});
test('Dashboard scopes remain composable and query body retains collection + folder + rules',()=>{
  const p=page();
  p.c.activeCollectionId=12;
  p.c.dashboardCollectionScope=true;
  p.c.activeFolderId=1;
  p.rules.push({
    field:'flag',value:'flagged'
  });
  p.c.settleSidebarScopeForReload(false);
  assert.equal(p.active().length,2);
  const req=p.c.buildBrowsePageRequest(1,50);
  assert.equal(req.url,'/api/photos/query');
  const body=JSON.parse(req.options.body);
  assert.equal(body.collection_id,12);
  assert.equal(body.folder_id,1);
  assert.equal(body.rules.length,1);
  p.c.testOnChange({
    reason:'ruleChanged'
  });
  assert.equal(p.c.activeCollectionId,12);
  assert.equal(p.active().length,2);
});
test('reload settling preserves explicitly requested legacy scope and drops unpreserved scope',()=>{
  const p=page();
  p.c.activeCollectionId=11;
  p.c.settleSidebarScopeForReload(true);
  assert.equal(p.c.activeCollectionId,11);
  assert.equal(p.active().length,1);
  p.c.settleSidebarScopeForReload(false);
  assert.equal(p.c.activeCollectionId,null);
  assert.equal(p.active().length,0);
});
test('list refreshes paint opened and Dashboard collections without repaint side effects',async()=>{
  const p=page();
  await p.c.filterByCollection(12);
  p.render();
  assert.deepEqual(p.active(),[['collectionList',{
    collectionId:'12'
  }]]);
  p.c.openedCollectionId=null;
  p.c.activeCollectionId=11;
  p.c.dashboardCollectionScope=true;
  p.render();
  assert.deepEqual(p.active(),[['collectionList',{
    collectionId:'11'
  }]]);
});
test('photo deep-link executes with invented init data and repaints all trees',async()=>{
  const p=page(),c=p.c;
  const no=()=>{
  };
  let reads=0,scrolled=false;
  Object.assign(c,{
    folderHealthRefreshSeq:0,_cfgPromise:Promise.resolve({
    }),applyBrowseConfig:no,claimBrowseWindow:()=>()=>true,updatePreviousPhotosButton:no,selectedPhotos:new Set(),closeDetail:no,perPage:50,loadCollectionCounts:no,setBrowseTotals:data=>{
      c.totalPhotos=data.total;
      c.totalUnderlyingPhotos=data.total;
    },loadedWindowOffset:()=>0,renderGrid:no,hydrateColorLabelsForRenderedPage:no,getGridCard:()=>({
      style:{
      },scrollIntoView:()=>{
        scrolled=true;
      }
    }),preferredScrollBehavior:()=> 'auto',setTimeout:fn=>fn(),rearmInfiniteScrollObserver:no,requestAnimationFrame:no,ensureViewportHydrated:no,safeFetch:async url=>{
      reads++;
      return url.startsWith('/api/photos/')?{
        id:999,folder_id:2
      }:{
        folders:p.folders,keywords:p.keywords,collections:p.collections,photos:[{
          id:999
        }],total:1,focus_page:1
      };
    }
  });
  functions(c,'boot.js',['_runPhotoDeepLink']);
  await c._runPhotoDeepLink(999);
  assert.equal(reads,2);
  assert.equal(c.activeFolderId,2);
  assert.equal(c.activeKeyword,null);
  assert.equal(c.activeCollectionId,null);
  assert.equal(scrolled,true);
  assert.deepEqual(p.active(),[['folderTree',{
    folderId:'2'
  }]]);
  assert.equal(c.loading,false);
});
test('deep-link-installed callback drops opened collection after chip edits and keyword selection',async()=>{
  for(const edit of ['chip','keyword']){
    const p=page(),c=p.c;
    const no=()=>{
    };
    let installed;
    Object.assign(c,{
      folderHealthRefreshSeq:0,_cfgPromise:Promise.resolve({
      }),applyBrowseConfig:no,claimBrowseWindow:()=>()=>true,updatePreviousPhotosButton:no,selectedPhotos:new Set(),closeDetail:no,perPage:50,loadCollectionCounts:no,setBrowseTotals:data=>{
        c.totalPhotos=data.total;
        c.totalUnderlyingPhotos=data.total;
      },loadedWindowOffset:()=>0,renderGrid:no,hydrateColorLabelsForRenderedPage:no,getGridCard:()=>({
        style:{
        },scrollIntoView:no
      }),preferredScrollBehavior:()=> 'auto',setTimeout:fn=>fn(),rearmInfiniteScrollObserver:no,requestAnimationFrame:no,ensureViewportHydrated:no,safeFetch:async url=>url.startsWith('/api/photos/')?{
        id:999,folder_id:2
      }:{
        folders:p.folders,keywords:p.keywords,collections:p.collections,photos:[{
          id:999
        }],total:1,focus_page:1
      }
    });
    let ready=false;
    c.VireoFilter.isReady=()=>ready;
    c.VireoFilter.init=opts=>{
      installed=opts.onChange;
      ready=true;
      return Promise.resolve();
    };
    c.VireoFilter.setResultTotal=no;
    c.VireoFilter.hasFilters=()=>false;
    functions(c,'boot.js',['_runPhotoDeepLink']);
    await c._runPhotoDeepLink(999);
    assert.equal(typeof installed,'function');
    c.testOnChange=installed;
    await c.filterByCollection(12);
    assert.equal(c.openedCollectionId,12);
    installed({
      reason:'expressionRefreshed'
    });
    assert.equal(c.openedCollectionId,12);
    installed({
      reason:'expressionLoaded'
    });
    assert.equal(c.openedCollectionId,12);
    if(edit==='chip')installed({
      reason:'ruleChanged'
    });
    else await c.filterByKeyword('Hawk');
    assert.equal(c.openedCollectionId,null,'deep-link callback must drop the saved collection handle');
    assert.equal(p.rows.collectionList.some(r=>r.classList.contains('active')),false);
    c.MUTATION_PREDICTION='prediction';
    c.sortSelectRanksOnPredictionConfidence=()=>false;
    c.safeFetch=()=>{
      throw Error('Edited scope must not refetch the saved collection');
    };
    c.filterByCollection=()=>{
      throw Error('Edited scope must not reopen the saved collection');
    };
    c.refreshActiveCollectionAfterMembershipChange(['keyword']);
    c.activeCollectionId=12;
    c.dashboardCollectionScope=true;
    c.openedCollectionId=null;
    installed({
      reason:'ruleChanged'
    });
    assert.equal(c.activeCollectionId,12);
    assert.equal(p.rows.collectionList.find(r=>r.dataset.collectionId==='12').classList.contains('active'),true);

  }
});
