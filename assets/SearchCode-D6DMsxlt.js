const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["assets/CodeBox-C1GWGw1I.js","assets/CodeHighlighted-C-d1w7yz.js","assets/index-CYtsL9nI.js","assets/_commonjsHelpers-CE1G-McA.js","assets/utils-BFUSjNrH.js","assets/I18N-BC6F-L_M.js","assets/index-DXWlJITM.css","assets/download-KnhDYu5b.js","assets/CopyButton-CN9mgVOt.js","assets/index-BaMzboGy.js","assets/vuex.cjs-DmT4ZGNM.js","assets/CodeHighlighted-Dw1Dok_I.css","assets/CodeBox-D99ooD2D.css"])))=>i.map(i=>d[i]);
import{aE as U,aC as L,ao as v,ap as h,h as u,p as S,U as j,j as y,F as $,aG as H,i as g,y as R,t as J,d as w,cz as F,aK as x,aL as I}from"./index-CYtsL9nI.js";import{a as k}from"./BFormRadioGroup.vue_vue_type_script_setup_true_lang-DaMHx4U3-Dl85I3xs.js";import{a as G,_ as M}from"./BTabs.vue_vue_type_script_setup_true_lang-CU4B0ph2-DT7jevUO.js";import{v as N}from"./vuex.cjs-DmT4ZGNM.js";import"./_commonjsHelpers-CE1G-McA.js";import"./utils-BFUSjNrH.js";import"./I18N-BC6F-L_M.js";import"./ConditionalWrapper.vue_vue_type_script_lang-IX_NpHH--DFBoU-Sy.js";import"./useStateClass-BGbSLWFN-CHJenuVv.js";class c{constructor(e,t){this.catalogHref=e,this.searchLink=t}get label(){return U.global.t(`programming.${this.language}`)}get language(){throw new Error("Subclasses must implement language")}get outputFile(){throw new Error("Subclasses must implement outputFile")}get indent(){return 0}get template(){throw new Error("Subclasses must implement template")}get installDependencies(){return null}get method(){const e=this.searchLink?.method;return typeof e=="string"?e.toUpperCase():"GET"}getFiltersAsJson(e){return JSON.stringify(e,null,this.indent)}getCleanFilters(e){const t={};for(const[n,s]of Object.entries(e))n==="filters"||s===null||s===void 0||(t[n]=s);return t}formatFilters(e){return this.getFiltersAsJson(this.getCleanFilters(e))}normalizeFilters(e){const t=Object.assign({},e),n=t.filters;if(n&&typeof n.serialize=="function")return{filters:t,cqlSerialized:n.serialize(this.method)};if(t["filter-lang"]||t.filter!==void 0){const s=t["filter-lang"],a=t.filter;delete t["filter-lang"],delete t.filter;const o={};return s&&(o["filter-lang"]=s),a!==void 0&&(o.filter=a),t.filters={serialize:()=>o},{filters:t,cqlSerialized:o}}return{filters:t,cqlSerialized:null}}generate(e){const{filters:t,cqlSerialized:n}=this.normalizeFilters(e),s=this.method==="POST",a=L.addFiltersToLink(this.searchLink,t);let o,l;s?(o=this.searchUrl,l=JSON.stringify(a?.body??{},null,this.indent)):(o=a?.href??this.searchUrl,l="");const _={...this.getVariables(t,n),REQUEST_URL:o,REQUEST_BODY:l,IS_GET:!s,IS_POST:s};return this.renderTemplate(this.getTemplate(t,n),_)}getTemplate(){return this.template}getVariables(e){return{CATALOG_URL:this.catalogHref,SEARCH_URL:this.searchLink.getAbsoluteUrl(),SEARCH_METHOD:this.method,RESULT_ARRAY_KEY:this.resultArrayKey,FILTERS:this.formatFilters(e)}}get searchUrl(){return this.searchLink.getAbsoluteUrl()}get isCollectionSearch(){try{return new URL(this.searchUrl).pathname.replace(/\/+$/,"").endsWith("/collections")}catch{return!1}}get resultArrayKey(){return this.isCollectionSearch?"collections":"features"}get commentChars(){return"##"}renderTemplate(e,t){let n=this.processConditionals(e,t);for(const[s,a]of Object.entries(t))n=n.replaceAll(`__${s}__`,String(a??""));return n.replace(/\n{3,}/g,`

`).trim()}processConditionals(e,t){let n=e;const s=this.commentChars.replace(/[.*+?^${}()|[\]\\]/g,"\\$&"),a=new RegExp(`${s}\\s+if\\s+(\\w+)\\s+${s}\\n?((?:(?!${s}\\s+if\\s)[\\s\\S])*?)${s}\\s+endif\\s+${s}\\n?`),o=new RegExp(`${s}\\s+else\\s+${s}\\n?`);let l=0;for(;a.test(n)&&l++<100;)n=n.replace(a,(_,d,i)=>{const m=!!t[d],p=i.match(o);if(!p)return m?i:"";const f=p.index,O=f+p[0].length;return m?i.substring(0,f):i.substring(O)});return n}}const B=`from pystac_client import Client

url = "__CATALOG_URL__"
catalog = Client.open(url)
results = catalog.__SEARCH_FUNCTION__(__SEARCH_ARGS__)

for entry in results.__ITERATOR_NAME__():
    print(entry.id)
`;class C extends c{get language(){return"python"}get outputFile(){return"search.py"}get template(){return B}get indent(){return 4}get installDependencies(){return"pip install pystac-client"}formatFilters(e){return this.getFiltersAsJson(e)}getVariables(e,t){return{...super.getVariables(e,t),ITERATOR_NAME:this.isCollectionSearch?"collections":"items",SEARCH_FUNCTION:this.isCollectionSearch?"collection_search":"search",SEARCH_ARGS:this.formatPystacSearchArgs(e,t)}}commaSeparatedStrings(e){return e.map(t=>JSON.stringify(t)).join(", ")}scopedCollections(e){if(Array.isArray(e.collections)&&e.collections.length>0)return e.collections;try{const n=new URL(this.searchUrl).pathname.match(/\/collections\/([^/]+)\/items\/?$/);if(n)return[decodeURIComponent(n[1])]}catch{}return[]}formatPystacSearchArgs(e,t){const n=[];if(!this.isCollectionSearch){const s=this.scopedCollections(e);if(s.length>0){const a=this.commaSeparatedStrings(s);n.push(`collections=[${a}]`)}if(Array.isArray(e.ids)&&e.ids.length>0){const a=this.commaSeparatedStrings(e.ids);n.push(`ids=[${a}]`)}}if(Array.isArray(e.bbox)&&e.bbox.length>0&&n.push(`bbox=[${e.bbox.join(", ")}]`),e.datetime&&n.push(`datetime=${JSON.stringify(e.datetime)}`),Array.isArray(e.q)&&e.q.length>0){const s=this.commaSeparatedStrings(e.q);n.push(`q=[${s}]`)}return typeof e.limit=="number"&&n.push(`max_items=${e.limit}`),e.sortby&&n.push(`sortby=${JSON.stringify(e.sortby)}`),t?.filter!==void 0&&n.push(`filter=${JSON.stringify(t.filter)}`),t?.["filter-lang"]&&n.push(`filter_lang=${JSON.stringify(t["filter-lang"])}`),this.method!=="POST"&&n.push(`method=${JSON.stringify(this.method)}`),n.join(", ")}}const V=`const url = "__REQUEST_URL__";
/// if IS_POST ///
const response = await fetch(url, {
  method: "__SEARCH_METHOD__",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(__REQUEST_BODY__)
});
/// else ///
const response = await fetch(url);
/// endif ///
const data = await response.json();
for (const entry of data.__RESULT_ARRAY_KEY__ ?? []) {
  console.log(entry.id);
}
`;class P extends c{get language(){return"javascript"}get outputFile(){return"search.mjs"}get template(){return V}get indent(){return 2}get commentChars(){return"///"}}const E=`library(rstac)

catalog <- stac("__CATALOG_URL__")
query <- stac_search(catalog__FILTER_ARGS__)
__EXT_FILTER__
result <- __REQUEST_FUNCTION__(query)

entries <- result[["__RESULT_ARRAY_KEY__"]]
if (!is.null(entries) && length(entries) > 0) {
  for (entry in entries) {
    if (!is.null(entry$id)) {
      cat(entry$id, "\\n")
    }
  }
}
`,q=`library(httr)
library(jsonlite)

## if IS_POST ##
search_filters <- __FILTERS_OBJECT__
resp <- VERB("__SEARCH_METHOD__", "__SEARCH_URL__", body = search_filters, encode = "json")
## else ##
resp <- GET("__REQUEST_URL__")
## endif ##
result <- fromJSON(content(resp, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)

entries <- result[["__RESULT_ARRAY_KEY__"]]
if (!is.null(entries) && length(entries) > 0) {
  for (entry in entries) {
    if (!is.null(entry$id)) {
      cat(entry$id, "\\n")
    }
  }
}
`;class Y extends c{get language(){return"r"}get outputFile(){return"search.R"}get template(){return E}getTemplate(e,t){return this.isCollectionSearch||t?q:E}get indent(){return 4}get installDependencies(){return`Rscript -e "install.packages('rstac')"`}commaSeparatedStrings(e){return e.map(t=>JSON.stringify(t)).join(", ")}scopedCollections(e){if(Array.isArray(e.collections)&&e.collections.length>0)return e.collections;try{const n=new URL(this.searchUrl).pathname.match(/\/collections\/([^/]+)\/items\/?$/);if(n)return[decodeURIComponent(n[1])]}catch{}return[]}formatRstacArgs(e){const t=[],n=this.scopedCollections(e);if(n.length>0){const a=this.commaSeparatedStrings(n);t.push(`collections = c(${a})`)}if(e.ids&&e.ids.length>0){const a=this.commaSeparatedStrings(e.ids);t.push(`ids = c(${a})`)}if(e.bbox&&t.push(`bbox = c(${e.bbox.join(", ")})`),e.datetime&&t.push(`datetime = "${e.datetime}"`),e.limit&&t.push(`limit = ${e.limit}`),t.length===0)return"";const s=" ".repeat(this.indent);return`,
`+s+t.join(`,
`+s)}getVariables(e,t){const n=this.getCleanFilters(e);return t&&Object.assign(n,t),{...super.getVariables(e,t),FILTERS_OBJECT:this.formatJsonBody(n),FILTER_ARGS:this.formatRstacArgs(e),EXT_FILTER:this.formatExtFilter(t),REQUEST_FUNCTION:this.method==="GET"?"get_request":"post_request"}}formatJsonBody(e){return`jsonlite::fromJSON("${JSON.stringify(e).replaceAll("\\","\\\\").replaceAll('"','\\"')}", simplifyVector = FALSE)`}formatExtFilter(e){if(!e?.filter)return"";let t;e["filter-lang"]==="cql2-json"?t=`jsonlite::fromJSON("${JSON.stringify(e.filter).replaceAll("\\","\\\\").replaceAll('"','\\"')}", simplifyVector = FALSE)`:t=JSON.stringify(e.filter);const n=e["filter-lang"]?`, lang = "${e["filter-lang"]}"`:"";return`query <- ext_filter(query, expr = ${t}${n})`}}const D=`using System;
using System.Net.Http;
/// if IS_POST ///
using System.Text;
/// endif ///

var httpClient = new HttpClient();
/// if IS_POST ///
var url = "__SEARCH_URL__";
var json = """
__REQUEST_BODY__
""";
var content = new StringContent(json, Encoding.UTF8, "application/json");
var method = new HttpMethod("__SEARCH_METHOD__");
var response = await httpClient.SendAsync(new HttpRequestMessage(method, url)
{
    Content = content
});
/// else ///
var url = "__REQUEST_URL__";
var response = await httpClient.GetAsync(url);
/// endif ///
var responseBody = await response.Content.ReadAsStringAsync();

using var doc = System.Text.Json.JsonDocument.Parse(responseBody);
if (doc.RootElement.TryGetProperty("__RESULT_ARRAY_KEY__", out var entries))
{
    foreach (var entry in entries.EnumerateArray())
    {
        if (entry.TryGetProperty("id", out var id))
        {
            Console.WriteLine(id.GetString());
        }
    }
}
`;class Q extends c{get language(){return"csharp"}get outputFile(){return"Program.cs"}get indent(){return 4}get template(){return D}get commentChars(){return"///"}}const K=`import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class StacSearch {
    public static void main(String[] args) throws Exception {
/// if IS_POST ///
        String url = "__SEARCH_URL__";
        String json = """
__REQUEST_BODY__
""";
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .method("__SEARCH_METHOD__", HttpRequest.BodyPublishers.ofString(json))
            .build();
/// else ///
        String url = "__REQUEST_URL__";
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET()
            .build();
/// endif ///

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JsonObject result = JsonParser.parseString(response.body()).getAsJsonObject();
        JsonArray entries = result.getAsJsonArray("__RESULT_ARRAY_KEY__");
        if (entries != null) {
            for (JsonElement entry : entries) {
                JsonElement id = entry.getAsJsonObject().get("id");
                if (id != null) {
                    System.out.println(id.getAsString());
                }
            }
        }
    }
}
`;class z extends c{get language(){return"java"}get outputFile(){return"StacSearch.java"}get template(){return K}get indent(){return 4}get installDependencies(){return"curl -sLO https://repo1.maven.org/maven2/com/google/code/gson/gson/2.13.2/gson-2.13.2.jar # Linux/MacOS only"}get commentChars(){return"///"}}const T=`use stac::api::Search;
use serde_json::json;
use stac_io::api;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let params = json!(__SEARCH_PARAMS__);
    let search: Search = serde_json::from_value(params)?;
    let max_items = search.limit.and_then(|value| usize::try_from(value).ok());
    let items = api::search("__CATALOG_URL__", search, max_items).await?;

    for item in items.items {
        if let Some(id) = item.get("id").and_then(|value| value.as_str()) {
            println!("{}", id);
        }
    }

    Ok(())
}
`,W=`use reqwest::Client;
/// if IS_POST ///
use serde_json::{json, Value};
/// else ///
use serde_json::Value;
/// endif ///
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
/// if IS_POST ///
    let search_url = "__SEARCH_URL__";
    let filters = json!(__REQUEST_BODY__);
    let body = serde_json::to_vec(&filters)?;
    let response = client
        .request(reqwest::Method::from_bytes("__SEARCH_METHOD__".as_bytes())?, search_url)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;
/// else ///
    let response = client.get("__REQUEST_URL__").send().await?;
/// endif ///
    let payload: Value = serde_json::from_str(&response.text().await?)?;
    if let Some(entries) = payload.get("__RESULT_ARRAY_KEY__").and_then(|value| value.as_array()) {
        for entry in entries {
            if let Some(id) = entry.get("id").and_then(|value| value.as_str()) {
                println!("{}", id);
            }
        }
    }

    Ok(())
}
`;class X extends c{get language(){return"rust"}get outputFile(){return"main.rs"}get template(){return T}getTemplate(e,t){return this.method==="POST"||this.isCollectionSearch||t?W:T}get indent(){return 4}get installDependencies(){return"cargo add serde_json stac stac-io reqwest && cargo add tokio@1 --features full"}get commentChars(){return"///"}getVariables(e,t){const n={};for(const[s,a]of Object.entries(e))s==="filters"||s==="filter"||s==="filter-lang"||a!=null&&(n[s]=a);return{...super.getVariables(e,t),SEARCH_PARAMS:this.formatJson(n)}}formatJson(e){const t=" ".repeat(this.indent);return JSON.stringify(e,null,this.indent).split(`
`).map((n,s)=>s===0?n:t+n).join(`
`)}formatFilters(e){return this.formatJson(this.getCleanFilters(e))}}const Z=[Q,z,P,C,Y,X],ee=C,b="codeLanguage",A="codeMethod",te=w({name:"SearchCode",components:{BTabs:M,BTab:G,CodeBox:x(()=>I(()=>import("./CodeBox-C1GWGw1I.js"),__vite__mapDeps([0,1,2,3,4,5,6,7,8,9,10,11,12])))},props:{searchLinks:{type:Object,required:!0},filters:{type:Object,default:()=>({})}},data(){return{storage:new F,selectedTab:null,selectedMethod:null}},computed:{...N.mapState(["catalogUrl"]),availableMethods(){return Object.keys(this.searchLinks)},activeSearchLink(){return this.searchLinks[this.selectedMethod]||Object.values(this.searchLinks)[0]},generatorInstances(){return Z.map(r=>new r(this.catalogUrl,this.activeSearchLink))},defaultLanguage(){const r=new ee(this.catalogUrl,this.activeSearchLink);return this.generatorInstances.find(t=>t.language===r.language)?.language}},watch:{selectedTab(r){this.saveSelectedTab(r)},selectedMethod(r){this.storage.set(A,r)}},created(){this.loadSelectedMethod(),this.loadSelectedTab()},methods:{loadSelectedMethod(){const r=this.storage.get(A);this.availableMethods.includes(r)?this.selectedMethod=r:this.selectedMethod=this.availableMethods[0]},loadSelectedTab(){const r=this.storage.get(b);if(this.generatorInstances.some(e=>e.language===r)){this.selectedTab=r;return}this.defaultLanguage&&(this.selectedTab=this.defaultLanguage)},saveSelectedTab(r){this.defaultLanguage!==r&&this.storage.set(b,r)}}}),ne={class:"search-code"};function se(r,e,t,n,s,a){const o=h("CodeBox"),l=h("b-tab"),_=h("b-tabs"),d=k;return u(),S("div",ne,[j(_,{modelValue:r.selectedTab,"onUpdate:modelValue":e[0]||(e[0]=i=>r.selectedTab=i)},{default:y(()=>[(u(!0),S($,null,H(r.generatorInstances,i=>(u(),g(l,{key:i.language,title:i.label,id:i.language},{default:y(()=>[r.selectedTab===i.language?(u(),g(o,{key:0,generator:i,filters:r.filters},null,8,["generator","filters"])):R("",!0)]),_:2},1032,["title","id"]))),128))]),_:1},8,["modelValue"]),e[2]||(e[2]=J()),r.availableMethods.length>1?(u(),g(d,{key:0,modelValue:r.selectedMethod,"onUpdate:modelValue":e[1]||(e[1]=i=>r.selectedMethod=i),options:r.availableMethods,"button-variant":"outline-primary",size:"sm",buttons:"",class:"mt-2"},null,8,["modelValue","options"])):R("",!0)])}const pe=v(te,[["render",se],["__scopeId","data-v-680bea14"]]);export{pe as default};
//# sourceMappingURL=SearchCode-D6DMsxlt.js.map
