const messagesEl = document.getElementById('messages');
const inputEl = document.getElementById('input');
const sendBtn = document.getElementById('send');
const apiUrlEl = document.getElementById('apiUrl');
const saveUrlBtn = document.getElementById('saveUrl');

let conversationId = null;

function appendMessage(role, content) {
  const wrap = document.createElement('div');
  wrap.className = `msg ${role}`;
  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  bubble.textContent = content;
  wrap.appendChild(bubble);
  messagesEl.appendChild(wrap);
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function loadApiUrl() {
  chrome.storage.sync.get(['healthinsight_api_url'], (data) => {
    apiUrlEl.value = data.healthinsight_api_url || 'http://localhost:8787';
  });
}

function saveApiUrl() {
  const url = apiUrlEl.value.trim();
  if (!/^https?:\/\//.test(url)) {
    alert('Enter a valid http(s) URL');
    return;
  }
  chrome.storage.sync.set({ healthinsight_api_url: url }, () => {
    appendMessage('assistant', 'Saved service URL.');
  });
}

async function sendMessage() {
  const text = inputEl.value.trim();
  if (!text) return;
  inputEl.value = '';
  appendMessage('user', text);

  const { healthinsight_api_url } = await new Promise(resolve => {
    chrome.storage.sync.get(['healthinsight_api_url'], resolve);
  });
  const base = healthinsight_api_url || 'http://localhost:8787';

  try {
    const resp = await fetch(`${base}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, conversation_id: conversationId })
    });
    if (!resp.ok) {
      appendMessage('assistant', `Error: ${resp.status}`);
      return;
    }
    const data = await resp.json();
    conversationId = data.conversation_id;
    appendMessage('assistant', data.reply);
  } catch (e) {
    appendMessage('assistant', `Failed to reach service: ${e}`);
  }
}

sendBtn.addEventListener('click', sendMessage);
inputEl.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') sendMessage();
});
saveUrlBtn.addEventListener('click', saveApiUrl);

loadApiUrl();
appendMessage('assistant', 'HEALTHINSIGHT ready. Ask me anything.');


