import { useState, useEffect, useCallback, useRef } from "react";

const C = {
  bg:"#06080c",s1:"#0b0e15",s2:"#10141d",s3:"#181d29",s4:"#212839",
  border:"rgba(255,255,255,.06)",border2:"rgba(255,255,255,.12)",
  text:"#e2e6f0",muted:"#6b7394",dim:"#3b4264",
  green:"#00d68f",greenDim:"rgba(0,214,143,.1)",
  red:"#ff4d6a",redDim:"rgba(255,77,106,.1)",
  amber:"#ffb547",amberDim:"rgba(255,181,71,.1)",
  blue:"#4da6ff",blueDim:"rgba(77,166,255,.1)",
  purple:"#a78bfa",purpleDim:"rgba(167,139,250,.1)",
};
const mono = "'Menlo','SF Mono','Cascadia Mono',monospace";

function fmt(n){return Number(n||0).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtK(n){if(n>=1e6)return(n/1e6).toFixed(1)+"M";if(n>=1e3)return(n/1e3).toFixed(1)+"K";return(n||0).toFixed(0)}
function timeStr(h){return h==null?"∞":h<1?"<1h":h<24?Math.round(h)+"h":Math.round(h/24)+"d"}
function pctStr(n){return((n||0)*100).toFixed(1)+"%"}

const Btn=({children,primary,danger,active,sm,onClick,style:s})=>{
  const base={fontFamily:mono,fontSize:sm?10:11,padding:sm?"3px 8px":"5px 12px",borderRadius:6,border:`1px solid ${C.border}`,background:C.s3,color:C.muted,cursor:"pointer",whiteSpace:"nowrap",transition:"0.15s",...s};
  if(primary)Object.assign(base,{background:C.green,borderColor:C.green,color:"#000",fontWeight:600});
  if(danger)Object.assign(base,{background:C.redDim,borderColor:`${C.red}30`,color:C.red});
  if(active)Object.assign(base,{borderColor:C.green,color:C.green,background:C.greenDim});
  return <button style={base} onClick={onClick}>{children}</button>;
};

const Tag=({children,color=C.blue})=>(
  <span style={{fontFamily:mono,fontSize:10,padding:"1px 6px",borderRadius:4,border:`1px solid ${color}20`,background:`${color}10`,color,fontWeight:500,whiteSpace:"nowrap"}}>{children}</span>
);

const Card=({title,icon,children,right,style:s})=>(
  <div style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:10,padding:14,...s}}>
    {title&&<div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12}}>
      <div style={{fontFamily:mono,fontSize:11,color:C.muted,textTransform:"uppercase",letterSpacing:".07em"}}>{icon} {title}</div>
      {right&&<div style={{display:"flex",gap:5}}>{right}</div>}
    </div>}
    {children}
  </div>
);

const Input=({label,...p})=>(
  <div style={{display:"flex",flexDirection:"column",gap:3}}>
    {label&&<div style={{fontSize:10,color:C.dim,textTransform:"uppercase",letterSpacing:".06em"}}>{label}</div>}
    <input {...p} style={{background:C.s1,border:`1px solid ${C.border}`,borderRadius:6,color:C.text,fontFamily:mono,fontSize:12,padding:"6px 10px",outline:"none",width:"100%",...p.style}}/>
  </div>
);

const Sel=({label,children,...p})=>(
  <div style={{display:"flex",flexDirection:"column",gap:3}}>
    {label&&<div style={{fontSize:10,color:C.dim,textTransform:"uppercase",letterSpacing:".06em"}}>{label}</div>}
    <select {...p} style={{background:C.s1,border:`1px solid ${C.border}`,borderRadius:6,color:C.text,fontFamily:mono,fontSize:12,padding:"6px 10px",outline:"none",width:"100%",...p.style}}>{children}</select>
  </div>
);

const Empty=({emoji,title,desc})=>(
  <div style={{textAlign:"center",padding:"32px 16px",color:C.muted}}>
    <div style={{fontSize:28,marginBottom:4}}>{emoji}</div>
    <div style={{fontSize:13,fontWeight:500,color:C.text,marginBottom:3}}>{title}</div>
    <div style={{fontSize:11}}>{desc}</div>
  </div>
);

const Metric=({label,value,sub,color})=>(
  <div style={{background:C.s1,border:`1px solid ${C.border}`,borderRadius:10,padding:"11px 13px",flex:"1 1 140px",minWidth:130}}>
    <div style={{fontSize:9,color:C.dim,textTransform:"uppercase",letterSpacing:".07em",marginBottom:3}}>{label}</div>
    <div style={{fontFamily:mono,fontSize:18,fontWeight:700,color:color||C.text,lineHeight:1.1}}>{value}</div>
    {sub&&<div style={{fontSize:10,color:C.muted,marginTop:2}}>{sub}</div>}
  </div>
);

const Spinner=()=><span style={{fontFamily:mono,color:C.amber,fontSize:11}}>⟳ Loading<span className="loading-dots"></span></span>;

// ═══════════════════════════════════════════════════════════════════════════
export default function App(){
  const [page,setPage]=useState("dashboard");
  const [apiUrl,setApiUrl]=useState(()=>{try{return localStorage.getItem("pt_api")||""}catch{return""}});
  const [connected,setConnected]=useState(false);
  const [loading,setLoading]=useState({});
  const [mode,setMode]=useState("paper");
  const [portfolio,setPortfolio]=useState({total_balance:0,available_balance:0,total_position_value:0,daily_pnl:0,daily_pnl_pct:0,total_trades_today:0,wins_today:0,losses_today:0,positions:[],exposure_pct:0});
  const [markets,setMarkets]=useState({});
  const [marketCat,setMarketCat]=useState("all");
  const [timeFilter,setTimeFilter]=useState("upcoming");
  const [whales,setWhales]=useState([]);
  const [whaleTier,setWhaleTier]=useState("all");
  const [logs,setLogs]=useState([]);
  const [testOrders,setTestOrders]=useState([]);
  const [toMid,setToMid]=useState("");
  const [toQ,setToQ]=useState("");
  const [toOut,setToOut]=useState("Yes");
  const [toSize,setToSize]=useState(50);
  const [toPrice,setToPrice]=useState(0.5);
  const [agents,setAgents]=useState({politics:false,sports:false,esports:false,crypto:false});
  const [autoOn,setAutoOn]=useState(false);
  const [showApi,setShowApi]=useState(false);
  const [apiInput,setApiInput]=useState(apiUrl);
  const wsRef=useRef(null);
  const pollRef=useRef(null);

  const addLog=useCallback((lv,mod,msg)=>{
    setLogs(p=>[{time:new Date().toISOString(),level:lv,module:mod,message:msg},...p].slice(0,500));
  },[]);

  const api=useCallback(async(path,opts={})=>{
    if(!apiUrl)return null;
    try{
      const r=await fetch(apiUrl+path,{...opts,headers:{"Content-Type":"application/json",...(opts.headers||{})}});
      if(!r.ok)throw new Error(`HTTP ${r.status}`);
      return await r.json();
    }catch(e){return null;}
  },[apiUrl]);

  // Connect to API
  const doConnect=useCallback(async(url)=>{
    const u=url.replace(/\/$/,"");
    setApiUrl(u);
    try{localStorage.setItem("pt_api",u)}catch{}
    try{
      const r=await fetch(u+"/health");const d=await r.json();
      if(d?.status==="ok"){setConnected(true);addLog("INFO","api","Connected to "+u);setShowApi(false);return true;}
    }catch{}
    setConnected(false);addLog("ERROR","api","Connection failed: "+u);return false;
  },[addLog]);

  // Poll data
  const pollData=useCallback(async()=>{
    if(!apiUrl||!connected)return;
    const[st,pf]=await Promise.all([api("/api/status"),api("/api/portfolio")]);
    if(st)setMode(st.mode||"paper");
    if(pf)setPortfolio(pf);
  },[api,apiUrl,connected]);

  // Initial connect + polling
  useEffect(()=>{
    if(apiUrl)doConnect(apiUrl);
  },[]);// eslint-disable-line

  useEffect(()=>{
    if(!connected)return;
    pollData();
    const iv=setInterval(pollData,10000);
    return()=>clearInterval(iv);
  },[connected,pollData]);

  // WebSocket
  useEffect(()=>{
    if(!apiUrl||!connected)return;
    let ws;let retries=0;
    const connect=()=>{
      try{
        ws=new WebSocket(apiUrl.replace("http","ws")+"/ws/live");
        ws.onopen=()=>{retries=0;};
        ws.onmessage=(e)=>{try{const d=JSON.parse(e.data);
          if(d.type==="portfolio")setPortfolio(p=>({...p,total_balance:d.balance||p.total_balance,daily_pnl:d.daily_pnl??p.daily_pnl,daily_pnl_pct:d.daily_pnl_pct??p.daily_pnl_pct}));
          if(d.type==="logs"&&d.entries)d.entries.forEach(l=>addLog(l.level,l.module,l.message));
        }catch{}};
        ws.onclose=()=>{if(retries<8){retries++;setTimeout(connect,Math.min(retries*2000,20000));}};
        ws.onerror=()=>ws.close();
        wsRef.current=ws;
      }catch{}
    };
    connect();
    return()=>{try{ws?.close()}catch{}};
  },[apiUrl,connected,addLog]);

  // ── Actions ──
  const scanMarkets=useCallback(async()=>{
    setLoading(p=>({...p,scan:true}));
    addLog("INFO","scanner",`Scanning ${marketCat} / ${timeFilter}...`);
    const d=await api(`/api/markets/scan?category=${marketCat}&time_filter=${timeFilter}&refresh=true`);
    if(d){setMarkets(d);const t=Object.values(d).reduce((s,a)=>s+(Array.isArray(a)?a.length:0),0);addLog("INFO","scanner",`Found ${t} markets`);}
    else addLog("WARN","scanner","Scan failed — check API connection");
    setLoading(p=>({...p,scan:false}));
  },[api,marketCat,timeFilter,addLog]);

  const loadWhales=useCallback(async()=>{
    setLoading(p=>({...p,whales:true}));
    const d=await api("/api/whales?limit=30");
    if(d&&Array.isArray(d))setWhales(d);
    else addLog("WARN","whale","No whale data returned");
    setLoading(p=>({...p,whales:false}));
  },[api,addLog]);

  const loadTestOrders=useCallback(async()=>{
    const d=await api("/api/test-orders");if(d)setTestOrders(d);
  },[api]);

  const placeTestOrder=useCallback(async()=>{
    if(!toMid){addLog("WARN","test","Select a market first");return;}
    const d=await api("/api/test-orders",{method:"POST",body:JSON.stringify({market_id:toMid,market_question:toQ,outcome:toOut,size_usd:toSize,price:toPrice,strategy:"manual_test"})});
    if(d){addLog("INFO","test",`Filled ${toOut}@${d.fill_price} slip=${d.slippage_pct}%`);loadTestOrders();}
    else addLog("ERROR","test","Order failed");
  },[api,toMid,toQ,toOut,toSize,toPrice,addLog,loadTestOrders]);

  const loadLogs=useCallback(async()=>{
    const d=await api("/api/logs?limit=200");
    if(d&&Array.isArray(d))setLogs(prev=>{const ids=new Set(prev.map(l=>l.time+l.message));const nw=d.filter(l=>!ids.has(l.time+l.message));return[...nw,...prev].slice(0,500);});
  },[api]);

  // On page change, load data
  useEffect(()=>{
    if(!connected)return;
    if(page==="markets"&&Object.keys(markets).length===0)scanMarkets();
    if(page==="whales"&&whales.length===0)loadWhales();
    if(page==="test-orders")loadTestOrders();
    if(page==="logs")loadLogs();
  },[page,connected]);// eslint-disable-line

  // Flat market list
  const allMarkets=(()=>{
    let arr=[];
    const cats=marketCat==="all"?Object.keys(markets):[marketCat];
    cats.forEach(c=>{if(Array.isArray(markets[c]))markets[c].forEach(m=>arr.push({...m,_cat:c}));});
    return arr;
  })();

  const filteredWhales=whaleTier==="all"?whales:whales.filter(w=>w.whale_tier===whaleTier);

  const tabs=[
    {id:"dashboard",icon:"📊",label:"Dashboard"},
    {id:"markets",icon:"🔍",label:"Markets"},
    {id:"test-orders",icon:"🧪",label:"Test Orders"},
    {id:"whales",icon:"🐳",label:"Whales"},
    {id:"agents",icon:"🤖",label:"Agents"},
    {id:"logs",icon:"📋",label:"Logs"},
    {id:"settings",icon:"⚙️",label:"Settings"},
  ];

  const catIcons={politics:"🏛",sports:"⚽",esports:"🎮",crypto:"₿",other:"📊"};

  return(
    <div style={{fontFamily:"system-ui,-apple-system,sans-serif",background:C.bg,color:C.text,height:"100vh",display:"flex",flexDirection:"column",overflow:"hidden",fontSize:13}}>
      {/* HEADER */}
      <div style={{height:46,display:"flex",alignItems:"center",gap:8,padding:"0 12px",background:C.s1,borderBottom:`1px solid ${C.border}`,flexShrink:0}}>
        <div style={{fontFamily:mono,fontSize:13,fontWeight:700,display:"flex",alignItems:"center",gap:5}}>
          <div style={{width:20,height:20,background:`linear-gradient(135deg,${C.green},${C.blue})`,borderRadius:5,display:"flex",alignItems:"center",justifyContent:"center",fontSize:10,fontWeight:800,color:"#000"}}>P</div>
          <span style={{color:C.green}}>Poly</span><span>Trader</span>
        </div>
        <div style={{flex:1}}/>
        <div onClick={()=>setShowApi(true)} style={{display:"flex",alignItems:"center",gap:4,background:C.s2,border:`1px solid ${C.border}`,borderRadius:16,padding:"2px 8px 2px 5px",cursor:"pointer",fontSize:10,fontFamily:mono,color:C.muted}}>
          <div style={{width:6,height:6,borderRadius:3,background:connected?C.green:C.red,boxShadow:connected?`0 0 5px ${C.green}40`:"none"}}/>
          {connected?apiUrl.replace(/https?:\/\//,"").slice(0,22):"Click to connect"}
        </div>
        <Tag color={mode==="live"?C.red:C.amber}>{mode.toUpperCase()}</Tag>
      </div>

      {/* TABS */}
      <div style={{display:"flex",background:C.s1,borderBottom:`1px solid ${C.border}`,overflowX:"auto",flexShrink:0}}>
        {tabs.map(t=>(
          <div key={t.id} onClick={()=>setPage(t.id)} style={{padding:"7px 12px",cursor:"pointer",fontSize:11,fontFamily:mono,color:page===t.id?C.green:C.muted,borderBottom:page===t.id?`2px solid ${C.green}`:"2px solid transparent",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:3,transition:"0.15s"}}>
            <span style={{fontSize:12}}>{t.icon}</span>{t.label}
          </div>
        ))}
      </div>

      {/* CONTENT */}
      <div style={{flex:1,overflow:"auto",padding:12}}>
        {/* DASHBOARD */}
        {page==="dashboard"&&<div style={{display:"flex",flexDirection:"column",gap:12}}>
          {!connected&&<div style={{background:C.amberDim,border:`1px solid ${C.amber}30`,borderRadius:8,padding:"10px 14px",fontSize:12,color:C.amber}}>⚠ Not connected to API. Click the connection pill in the header or go to Settings to configure.</div>}
          <div style={{display:"flex",flexWrap:"wrap",gap:8}}>
            <Metric label="Balance" value={`$${fmt(portfolio.total_balance)}`} sub={mode+" trading"}/>
            <Metric label="Today P&L" value={`${portfolio.daily_pnl>=0?"+":""}$${fmt(Math.abs(portfolio.daily_pnl))}`} color={portfolio.daily_pnl>=0?C.green:C.red} sub={pctStr(portfolio.daily_pnl_pct)}/>
            <Metric label="Positions" value={portfolio.positions?.length||0} sub={`${pctStr(portfolio.exposure_pct)} exposure`}/>
            <Metric label="Win Rate" value={portfolio.total_trades_today>0?pctStr(portfolio.wins_today/portfolio.total_trades_today):"—"} sub={`${portfolio.total_trades_today} trades today`}/>
          </div>
          <Card title="Recent Activity" icon="⚡" right={<Btn sm onClick={loadLogs}>Refresh</Btn>}>
            {logs.length===0?<Empty emoji="📭" title="No activity yet" desc={connected?"Waiting for events...":"Connect API to see activity"}/>:
            <div style={{maxHeight:240,overflow:"auto"}}>{logs.slice(0,12).map((l,i)=>(
              <div key={i} style={{display:"flex",gap:6,padding:"3px 0",fontSize:11,fontFamily:mono,borderBottom:`1px solid ${C.border}`}}>
                <span style={{color:C.dim,width:52,flexShrink:0}}>{new Date(l.time).toLocaleTimeString().slice(0,8)}</span>
                <span style={{width:34,flexShrink:0,fontWeight:600,fontSize:10,color:l.level==="ERROR"?C.red:l.level==="WARN"?C.amber:C.blue}}>{l.level}</span>
                <span style={{color:C.muted,wordBreak:"break-word"}}>{l.message}</span>
              </div>
            ))}</div>}
          </Card>
        </div>}

        {/* MARKETS */}
        {page==="markets"&&<div style={{display:"flex",flexDirection:"column",gap:12}}>
          <Card title="Market Scanner" icon="🔍" right={<>{loading.scan?<Spinner/>:<Btn sm primary onClick={scanMarkets}>⟳ Scan Now</Btn>}</>}>
            <div style={{display:"flex",gap:3,flexWrap:"wrap",marginBottom:8}}>
              {["all","politics","sports","esports","crypto"].map(c=>(
                <Btn key={c} sm active={marketCat===c} onClick={()=>{setMarketCat(c);}}>{
                  {all:"All",politics:"🏛 Politics",sports:"⚽ Sports",esports:"🎮 Esports",crypto:"₿ Crypto"}[c]
                }</Btn>
              ))}
            </div>
            <div style={{display:"flex",gap:3,flexWrap:"wrap",marginBottom:6}}>
              {[["today","🔴 Today"],["live","⏰ 48h"],["upcoming","📅 7 Days"],["all","🌐 All"]].map(([k,l])=>(
                <Btn key={k} sm active={timeFilter===k} onClick={()=>setTimeFilter(k)}>{l}</Btn>
              ))}
            </div>
            <div style={{fontSize:10,color:C.dim,fontFamily:mono}}>{allMarkets.length} markets loaded • Click a market to test order</div>
          </Card>
          {allMarkets.length===0?(
            loading.scan?<div style={{textAlign:"center",padding:40}}><Spinner/></div>:
            <Empty emoji="🔍" title="No markets loaded" desc={connected?"Click 'Scan Now' to fetch live markets from Polymarket":"Connect to API first (Settings → API Endpoint)"}/>
          ):(
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))",gap:8}}>
              {allMarkets.slice(0,60).map(m=>{
                const yp=(m.yes_price*100).toFixed(1);
                const ec=m.edge_score>=70?C.green:m.edge_score>=45?C.amber:C.muted;
                return(
                  <div key={m.id} onClick={()=>{setToMid(m.id);setToQ(m.question);setToPrice(m.yes_price);setPage("test-orders");}}
                    style={{background:C.s1,border:`1px solid ${C.border}`,borderRadius:8,padding:10,cursor:"pointer",transition:"border-color 0.15s"}}
                    onMouseOver={e=>e.currentTarget.style.borderColor=C.border2}
                    onMouseOut={e=>e.currentTarget.style.borderColor=C.border}>
                    <div style={{display:"flex",gap:4,alignItems:"center",marginBottom:5,flexWrap:"wrap"}}>
                      <Tag color={C.blue}>{catIcons[m._cat]||"📊"} {m._cat}</Tag>
                      {m.hours_left!=null&&m.hours_left<24&&<Tag color={C.red}>⏰ {timeStr(m.hours_left)}</Tag>}
                      {m.hours_left!=null&&m.hours_left>=24&&m.hours_left<72&&<Tag color={C.amber}>{timeStr(m.hours_left)}</Tag>}
                      <span style={{marginLeft:"auto",fontFamily:mono,fontSize:9,padding:"1px 5px",borderRadius:8,background:`${ec}15`,color:ec,fontWeight:600}}>{m.edge_score}</span>
                    </div>
                    <div style={{fontSize:11,fontWeight:500,lineHeight:1.35,marginBottom:6,display:"-webkit-box",WebkitLineClamp:2,WebkitBoxOrient:"vertical",overflow:"hidden",minHeight:30}}>{m.question}</div>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline"}}>
                      <span style={{fontFamily:mono,fontSize:15,fontWeight:700,color:C.green}}>YES {yp}¢</span>
                      <span style={{fontFamily:mono,fontSize:9,color:C.dim}}>${fmtK(m.liquidity)} liq</span>
                    </div>
                    <div style={{height:3,background:C.s3,borderRadius:2,marginTop:5,overflow:"hidden"}}>
                      <div style={{height:"100%",width:`${yp}%`,background:`linear-gradient(90deg,${C.green},${C.blue})`,borderRadius:2}}/>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>}

        {/* TEST ORDERS */}
        {page==="test-orders"&&<div style={{display:"flex",flexDirection:"column",gap:12}}>
          <Card title="Test Order" icon="🧪">
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
              <div style={{gridColumn:"1/-1"}}><Input label="Market ID" value={toMid} onChange={e=>setToMid(e.target.value)} placeholder="Select from scanner or paste ID"/></div>
              <div style={{gridColumn:"1/-1"}}><Input label="Market Question" value={toQ} onChange={e=>setToQ(e.target.value)} placeholder="Question text"/></div>
              <Sel label="Outcome" value={toOut} onChange={e=>setToOut(e.target.value)}><option>Yes</option><option>No</option></Sel>
              <Input label="Size (USD)" type="number" value={toSize} onChange={e=>setToSize(+e.target.value)} min={10}/>
              <Input label="Price" type="number" value={toPrice} onChange={e=>setToPrice(+e.target.value)} min={0.01} max={0.99} step={0.01}/>
              <Sel label="Strategy" defaultValue="manual_test"><option>manual_test</option><option>swing_event_driven</option><option>sports_safe_lock</option></Sel>
            </div>
            <div style={{marginTop:10}}><Btn primary onClick={placeTestOrder}>Place Test Order</Btn></div>
          </Card>
          <Card title="History" icon="📜">
            {testOrders.length===0?<Empty emoji="🧪" title="No test orders" desc="Place one above"/>:
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",minWidth:480}}>
              <thead><tr>{["Time","Market","Side","Req","Fill","Size","Slip"].map(h=>(
                <th key={h} style={{fontFamily:mono,fontSize:9,color:C.dim,padding:"5px 6px",textAlign:"left",borderBottom:`1px solid ${C.border}`,textTransform:"uppercase"}}>{h}</th>
              ))}</tr></thead>
              <tbody>{testOrders.slice(0,25).map((o,i)=>(
                <tr key={i}>{[
                  <td style={{fontFamily:mono,fontSize:9,color:C.dim}}>{new Date(o.created_at).toLocaleTimeString()}</td>,
                  <td style={{maxWidth:160,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",fontSize:11}}>{o.market_question?.slice(0,45)}</td>,
                  <td><Tag color={o.outcome==="Yes"?C.green:C.red}>{o.outcome}</Tag></td>,
                  <td style={{fontFamily:mono,fontSize:11}}>{o.requested_price?.toFixed(3)}</td>,
                  <td style={{fontFamily:mono,fontSize:11}}>{o.fill_price?.toFixed(3)}</td>,
                  <td style={{fontFamily:mono,fontSize:11}}>${fmt(o.size_usd)}</td>,
                  <td style={{fontFamily:mono,fontSize:11,color:o.slippage_pct>1?C.amber:C.muted}}>{o.slippage_pct}%</td>,
                ].map((c,j)=><td key={j} style={{padding:"5px 6px",borderBottom:`1px solid ${C.border}`,...(c.props?.style||{})}}>{c.props?.children||c}</td>)}</tr>
              ))}</tbody>
            </table></div>}
          </Card>
        </div>}

        {/* WHALES */}
        {page==="whales"&&<div style={{display:"flex",flexDirection:"column",gap:12}}>
          <Card title="Whale Tracker" icon="🐳" right={<>{loading.whales?<Spinner/>:<Btn sm onClick={loadWhales}>⟳ Refresh</Btn>}</>}>
            <div style={{display:"flex",gap:3,marginBottom:8,flexWrap:"wrap"}}>
              {["all","S","A","B"].map(t=>(
                <Btn key={t} sm active={whaleTier===t} onClick={()=>setWhaleTier(t)}>{
                  {all:"All Tiers",S:"🐳 S-Tier",A:"🦈 A-Tier",B:"🐟 B-Tier"}[t]
                }</Btn>
              ))}
            </div>
          </Card>
          {filteredWhales.length===0?(
            loading.whales?<div style={{textAlign:"center",padding:40}}><Spinner/></div>:
            <Empty emoji="🐳" title="No whale data" desc={connected?"Click Refresh to fetch from Polymarket leaderboard":"Connect API first"}/>
          ):(
            <div style={{display:"flex",flexDirection:"column",gap:6}}>
              {filteredWhales.map((w,i)=>{
                const te={S:"🐳",A:"🦈",B:"🐟"}[w.whale_tier]||"🐠";
                const bg={S:"linear-gradient(135deg,#FFD700,#FFA500)",A:`linear-gradient(135deg,${C.blue},${C.purple})`,B:C.s3}[w.whale_tier]||C.s3;
                return(
                  <div key={i} style={{background:C.s1,border:`1px solid ${C.border}`,borderRadius:8,padding:"9px 11px",display:"flex",gap:9,alignItems:"flex-start"}}>
                    <div style={{width:30,height:30,borderRadius:15,background:bg,display:"flex",alignItems:"center",justifyContent:"center",fontSize:14,flexShrink:0}}>{te}</div>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{display:"flex",alignItems:"center",gap:5,marginBottom:2,flexWrap:"wrap"}}>
                        <span style={{fontWeight:600,fontSize:12}}>{w.whale_name||"Unknown"}</span>
                        <Tag color={C.blue}>Tier {w.whale_tier}</Tag>
                        {w.whale_win_rate>0&&<span style={{fontFamily:mono,fontSize:9,color:C.muted}}>{(w.whale_win_rate*100).toFixed(0)}% WR</span>}
                        {w.volume_total>0&&<span style={{fontFamily:mono,fontSize:9,color:C.dim}}>Vol ${fmtK(w.volume_total)}</span>}
                      </div>
                      <div style={{fontSize:10,color:C.muted,display:"flex",gap:6,flexWrap:"wrap"}}>
                        {w.action&&w.action!=="—"&&<span>{w.action==="buy"?"🟢":"🔴"} {w.action.toUpperCase()}</span>}
                        {w.amount_usd>0&&<span style={{fontFamily:mono}}>${fmtK(w.amount_usd)}</span>}
                        {w.pnl_total!==undefined&&w.pnl_total!==0&&<span style={{fontFamily:mono,color:w.pnl_total>=0?C.green:C.red}}>PnL ${fmtK(Math.abs(w.pnl_total))}</span>}
                        {w.market_question&&<span style={{overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:180}}>{w.market_question}</span>}
                        <span style={{fontFamily:mono,fontSize:9,color:C.dim}}>{w.source||""}</span>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>}

        {/* AGENTS */}
        {page==="agents"&&<div style={{display:"flex",flexDirection:"column",gap:12}}>
          <Card title="Trading Agents" icon="🤖" right={
            <div style={{display:"flex",alignItems:"center",gap:6}}>
              <span style={{fontSize:10,color:C.muted}}>Master</span>
              <div onClick={()=>{setAutoOn(!autoOn);addLog(autoOn?"WARN":"INFO","auto",autoOn?"Automation OFF":"Automation ON")}} style={{width:34,height:18,borderRadius:9,background:autoOn?C.greenDim:C.s3,border:`1px solid ${autoOn?C.green+"40":C.border}`,position:"relative",cursor:"pointer"}}>
                <div style={{width:12,height:12,borderRadius:6,background:autoOn?C.green:C.muted,position:"absolute",top:2,left:autoOn?18:3,transition:"0.2s"}}/>
              </div>
            </div>
          }>
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(250px,1fr))",gap:8}}>
              {[
                {key:"politics",icon:"🏛",bg:`linear-gradient(135deg,${C.blue},#6366f1)`,name:"Politics",desc:"Elections, policy, government",wr:"90%+",edge:"Info lag",roi:"30-80%"},
                {key:"sports",icon:"⚽",bg:`linear-gradient(135deg,${C.green},#10b981)`,name:"Sports",desc:"Football, NBA, NFL live",wr:"85-95%",edge:"Data latency",roi:"100-500%"},
                {key:"esports",icon:"🎮",bg:`linear-gradient(135deg,${C.purple},#f472b6)`,name:"Esports",desc:"LoL, CS2, Valorant, Dota",wr:"95-100%",edge:"Packet delay",roi:"200-1000%"},
                {key:"crypto",icon:"₿",bg:`linear-gradient(135deg,${C.amber},#f59e0b)`,name:"Crypto",desc:"BTC/ETH price markets",wr:"60-70%",edge:"Momentum",roi:"20-60%"},
              ].map(a=>(
                <div key={a.key} style={{background:C.s1,border:`1px solid ${C.border}`,borderRadius:8,padding:12}}>
                  <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:6}}>
                    <div style={{width:28,height:28,borderRadius:7,background:a.bg,display:"flex",alignItems:"center",justifyContent:"center",fontSize:14}}>{a.icon}</div>
                    <div style={{flex:1}}><div style={{fontWeight:600,fontSize:12}}>{a.name}</div><div style={{fontSize:9,color:C.muted}}>{a.desc}</div></div>
                    <div onClick={()=>setAgents(p=>({...p,[a.key]:!p[a.key]}))} style={{width:34,height:18,borderRadius:9,background:agents[a.key]?C.greenDim:C.s3,border:`1px solid ${agents[a.key]?C.green+"40":C.border}`,position:"relative",cursor:"pointer"}}>
                      <div style={{width:12,height:12,borderRadius:6,background:agents[a.key]?C.green:C.muted,position:"absolute",top:2,left:agents[a.key]?18:3,transition:"0.2s"}}/>
                    </div>
                  </div>
                  <div style={{display:"flex",gap:10,fontSize:9,color:C.muted,fontFamily:mono}}>
                    <span>WR <b style={{color:C.text}}>{a.wr}</b></span>
                    <span>Edge <b style={{color:C.text}}>{a.edge}</b></span>
                    <span>ROI <b style={{color:C.green}}>{a.roi}</b></span>
                  </div>
                </div>
              ))}
            </div>
          </Card>
          <Btn primary onClick={async()=>{
            const aa=Object.entries(agents).filter(([,v])=>v).map(([k])=>k);
            await api("/api/automation",{method:"POST",body:JSON.stringify({enabled:autoOn,agents:aa,min_confidence:70,max_position_pct:0.05,paper_only:true})});
            addLog("INFO","auto","Config saved: "+aa.join(", "));
          }}>Save Automation Config</Btn>
        </div>}

        {/* LOGS */}
        {page==="logs"&&<Card title="Activity Logs" icon="📋" right={<><Btn sm onClick={loadLogs}>Refresh</Btn><Btn sm danger onClick={async()=>{await api("/api/logs",{method:"DELETE"});setLogs([])}}>Clear</Btn></>}>
          {logs.length===0?<Empty emoji="📋" title="No logs" desc="Events appear as the system processes them"/>:
          <div style={{maxHeight:"calc(100vh - 170px)",overflow:"auto"}}>{logs.map((l,i)=>(
            <div key={i} style={{display:"flex",gap:6,padding:"3px 0",fontSize:11,fontFamily:mono,borderBottom:`1px solid ${C.border}`}}>
              <span style={{color:C.dim,width:55,flexShrink:0}}>{new Date(l.time).toLocaleTimeString()}</span>
              <span style={{width:34,flexShrink:0,fontWeight:600,fontSize:10,color:l.level==="ERROR"?C.red:l.level==="WARN"?C.amber:C.blue}}>{l.level}</span>
              <span style={{color:C.blue,width:50,flexShrink:0,fontSize:9}}>{l.module}</span>
              <span style={{color:C.muted,wordBreak:"break-word"}}>{l.message}</span>
            </div>
          ))}</div>}
        </Card>}

        {/* SETTINGS */}
        {page==="settings"&&<div style={{display:"flex",flexDirection:"column",gap:12}}>
          <Card title="API Connection" icon="🔌">
            <div style={{display:"grid",gridTemplateColumns:"1fr",gap:8}}>
              <Input label="API Endpoint" value={apiInput} onChange={e=>setApiInput(e.target.value)} placeholder="http://your-server:8088"/>
            </div>
            <div style={{display:"flex",gap:6,marginTop:10,alignItems:"center"}}>
              <Btn primary onClick={()=>doConnect(apiInput)}>Connect</Btn>
              <span style={{fontFamily:mono,fontSize:10,color:connected?C.green:C.red}}>{connected?"✓ Connected":"✗ Disconnected"}</span>
            </div>
          </Card>
          <Card title="Data API Keys" icon="🔑">
            <p style={{fontSize:10,color:C.muted,marginBottom:8}}>These keys are saved to the server via /api/config and used by the trading system.</p>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
              <Input label="Polygon RPC (WS)" placeholder="wss://polygon-bor-rpc.publicnode.com" id="cfg_rpc"/>
              <Input label="API-Football Key" type="password" placeholder="RapidAPI key" id="cfg_football"/>
              <Input label="NewsAPI Key" type="password" placeholder="NewsAPI key" id="cfg_news"/>
              <Input label="OpenAI API Key" type="password" placeholder="sk-..." id="cfg_openai"/>
              <Input label="Odds API Key" type="password" placeholder="Odds API key" id="cfg_odds"/>
              <Input label="Anthropic API Key" type="password" placeholder="sk-ant-..." id="cfg_anthropic"/>
            </div>
            <Btn primary style={{marginTop:10}} onClick={async()=>{
              const cfg={};
              ["rpc","football","news","openai","odds","anthropic"].forEach(k=>{const el=document.getElementById("cfg_"+k);if(el?.value)cfg[k+"_key"]=el.value;});
              await api("/api/config",{method:"POST",body:JSON.stringify({config:cfg})});
              addLog("INFO","config","API keys saved");
            }}>Save API Keys</Btn>
          </Card>
          <Card title="Paper Trading" icon="💰">
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
              <Input label="Initial Balance ($)" type="number" defaultValue={10000} min={100} step={100} id="cfg_bal"/>
              <Input label="Fee Rate %" type="number" defaultValue={2} min={0} max={10} step={0.5}/>
            </div>
            <Btn style={{marginTop:8}} onClick={async()=>{
              const v=parseFloat(document.getElementById("cfg_bal")?.value)||10000;
              await api("/api/paper/balance",{method:"POST",body:JSON.stringify({balance:v})});
              addLog("INFO","paper","Balance set to $"+fmt(v));
              pollData();
            }}>Set Balance</Btn>
          </Card>
          <Card title="System" icon="ℹ️">
            <div style={{fontFamily:mono,fontSize:11,color:C.muted,lineHeight:2}}>
              <div>Version: <span style={{color:C.text}}>3.0.0</span></div>
              <div>API: <span style={{color:connected?C.green:C.red}}>{connected?apiUrl:"Not connected"}</span></div>
              <div>Mode: <span style={{color:C.text}}>{mode}</span></div>
            </div>
          </Card>
        </div>}
      </div>

      {/* API MODAL */}
      {showApi&&<div style={{position:"fixed",inset:0,background:"rgba(0,0,0,.7)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:500,padding:16}} onClick={()=>setShowApi(false)}>
        <div style={{background:C.s2,border:`1px solid ${C.border2}`,borderRadius:12,padding:18,width:"100%",maxWidth:380}} onClick={e=>e.stopPropagation()}>
          <div style={{fontFamily:mono,fontSize:13,fontWeight:600,marginBottom:12}}>🔌 Connect to API</div>
          <Input label="API Endpoint" value={apiInput} onChange={e=>setApiInput(e.target.value)} placeholder="http://localhost:8088"/>
          <div style={{display:"flex",gap:6,marginTop:12}}>
            <Btn primary onClick={()=>doConnect(apiInput)}>Connect</Btn>
            <Btn onClick={()=>setShowApi(false)}>Cancel</Btn>
          </div>
        </div>
      </div>}
    </div>
  );
}
