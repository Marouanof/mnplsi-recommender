const DEFAULT_API_BASE = (() => {
  if (window.location.protocol === 'file:') return 'http://localhost:5000';
  return `${window.location.protocol}//${window.location.hostname}:5000`;
})();

const API = (window.__API_BASE__ || DEFAULT_API_BASE);

let allUsers = [];
const $ = id => document.getElementById(id);

let loadingTimer = null;
let loadingRequestId = 0;

function setStatus(message, type){
  const badge = $('statusBadge');
  if(!badge) return;

  badge.className = 'status-indicator ' + (type || 'existing');
  badge.textContent = message || '';
}

async function readJsonOrText(res){
  const ct = (res.headers.get('content-type') || '').toLowerCase();
  if(ct.includes('application/json')) return await res.json();
  const text = await res.text();
  try{ return JSON.parse(text); }catch{ return { message: text }; }
}

async function apiFetch(path){
  const url = `${API}${path}`;
  const res = await fetch(url);
  const data = await readJsonOrText(res);
  if(!res.ok){
    const msg = data?.error || data?.message || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

async function loadUsers(){
  try{
    const data = await apiFetch(`/users/list`);
    allUsers = data.users || [];
    console.info('Utilisateurs chargés:', allUsers.length);
  }catch(e){
    allUsers = ['A3SGXH7AUHU8GW','A1D87F6ZCVE5NK','ABXLMWJIXXAIN'];
  }
}

function setLoading(active){
  const ld = $('loading');
  if(ld) ld.hidden = !active;
}

function startLoading(){
  loadingRequestId += 1;
  const requestId = loadingRequestId;
  if(loadingTimer) clearTimeout(loadingTimer);
  setLoading(false);
  loadingTimer = setTimeout(() => {
    if(requestId !== loadingRequestId) return;
    setLoading(true);
  }, 250);
  return requestId;
}

function stopLoading(requestId){
  if(typeof requestId === 'number' && requestId !== loadingRequestId) return;
  if(loadingTimer) clearTimeout(loadingTimer);
  loadingTimer = null;
  setLoading(false);
}

async function renderTop(){
  const rid = startLoading();
  try{
    setStatus('', 'existing');
    const resultCard = $('resultCard');
    if(resultCard) resultCard.hidden = false;
    const data = await apiFetch(`/recommendations/top`);
    const ids = (data.top_products||[]).join(',');
    if(!ids) throw new Error('Aucun top');
    const details = await apiFetch(`/products/details?ids=${encodeURIComponent(ids)}`);
    const list = $('topList');
    list.hidden = false;
    list.innerHTML = (details.products||[]).map(p=>`
      <li>
        <div class="product-info">
          <strong>📦 ${p.product_id}</strong>
          <span class="meta">⭐ ${p.avg_score}/5 · 📝 ${p.nb_reviews} avis</span>
          ${p.summary !== 'N/A' ? `<span class="product-summary">💬 ${p.summary}</span>` : ''}
        </div>
      </li>
    `).join('');
  }catch(e){
    setStatus(`❌ Top produits: ${e.message} (API: ${API})`, 'new');
  }finally{stopLoading(rid)}
}

async function fetchRecommendations(userId){
  if(!userId){alert('Entrez un ID utilisateur');return}
  const rid = startLoading();
  const resultCard = $('resultCard');
  if(resultCard) resultCard.hidden = false;
  const list = $('recommendationsList');
  if(list) list.innerHTML = '';
  const ui = $('userInfo');
  if(ui) ui.innerHTML = '';
  try{
    setStatus('', 'existing');
    const data = await apiFetch(`/recommendations/user/${encodeURIComponent(userId)}`);

    $('displayUserId').textContent = userId;
    const ui = $('userInfo'); ui.innerHTML='';
    if(data.profile_name && data.profile_name!=='Inconnu'){
      ui.innerHTML = `
        <p>👤 <strong>${data.profile_name}</strong> | ⭐ ${data.avg_score}/5 | 📝 ${data.nb_reviews} avis | 👍 ${data.helpfulness_rate} utiles</p>
      `;
    }

    const badge = $('statusBadge');
    badge.className = 'status-indicator ' + (data.status==='new_user' ? 'new' : 'existing');
    badge.textContent = data.status==='new_user' ? '🆕 Nouvel Utilisateur' : '✅ Utilisateur Existant';

    const list = $('recommendationsList');
    list.innerHTML = '';
    if((data.recommendations||[]).length>0){
      const ids = data.recommendations.join(',');
      const details = await apiFetch(`/products/details?ids=${encodeURIComponent(ids)}`);
      list.innerHTML = (details.products||[]).map((p,i)=>`
        <li>
          <div class="product-info">
            <strong>#${i+1} 📦 ${p.product_id}</strong>
            <span class="meta">⭐ ${p.avg_score}/5 · 📝 ${p.nb_reviews} avis</span>
            ${p.summary !== 'N/A' ? `<span class="product-summary">💬 ${p.summary}</span>` : ''}
          </div>
        </li>
      `).join('');
    }else{
      list.innerHTML = '<li>Aucune recommandation disponible</li>';
    }

    resultCard.hidden = false;
  }catch(e){
    $('recommendationsList').innerHTML = `<li>❌ Erreur API: ${e.message} (API: ${API})</li>`;
    setStatus(`❌ Recommandations: ${e.message}`, 'new');
    resultCard.hidden = false;
  }finally{stopLoading(rid)}
}

// UI bindings
window.addEventListener('load', async ()=>{
  await loadUsers();
  $('getRecommendations').addEventListener('click', ()=>fetchRecommendations($('userId').value.trim()));
  $('randomUser').addEventListener('click', ()=>{
    if(allUsers.length){
      const u = allUsers[Math.floor(Math.random()*allUsers.length)];
      $('userId').value = u;
      fetchRecommendations(u);
    }
  });
  $('getTop').addEventListener('click', renderTop);

  $('resultCard').hidden = true;
});