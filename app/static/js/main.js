/* ============================================================
   BOAMP × Cohesity — main.js
   Filtres live, watchlist toggle, copie référence, interactions
   ============================================================ */

'use strict';

// ─── Theme toggle ─────────────────────────────────────────────

/**
 * Appelée depuis la page Profil : bascule vers le thème demandé
 * uniquement si ce n'est pas déjà le thème actif.
 */
function pickTheme(theme) {
  const current = document.documentElement.getAttribute('data-theme') || 'light';
  if (current !== theme) toggleTheme();
}
window.pickTheme = pickTheme;

async function toggleTheme() {
  const html = document.documentElement;
  const current = html.getAttribute('data-theme') || 'light';
  const next = current === 'dark' ? 'light' : 'dark';

  // Basculement immédiat (pas d'attente serveur)
  html.setAttribute('data-theme', next);

  // Mise à jour du libellé + icône dans le dropdown
  const btn = document.getElementById('themeToggle');
  if (btn) {
    const icon = btn.querySelector('i');
    if (icon) icon.className = next === 'light' ? 'fa-solid fa-moon' : 'fa-solid fa-sun';
    const textNode = [...btn.childNodes].find(n => n.nodeType === Node.TEXT_NODE);
    if (textNode) textNode.textContent = next === 'light' ? ' Mode sombre' : ' Mode clair';
  }

  // Mise à jour du picker dans la page Profil (si présente)
  document.querySelectorAll('.theme-choice').forEach(b => b.classList.remove('active'));
  const activePicker = document.getElementById(next === 'dark' ? 'pickDark' : 'pickLight');
  if (activePicker) activePicker.classList.add('active');

  // Persistance côté serveur (best-effort)
  try {
    const csrf = document.querySelector('meta[name="csrf-token"]')?.content
              || document.querySelector('input[name="csrf_token"]')?.value
              || '';
    await fetch('/set-theme', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf },
      credentials: 'same-origin',
      body: JSON.stringify({ theme: next }),
    });
  } catch (_) {
    // Silencieux : le thème est quand même appliqué localement
  }
}
window.toggleTheme = toggleTheme;

// ─── User menu dropdown ───────────────────────────────────────
(function () {
  const btn = document.getElementById('userMenuBtn');
  const dropdown = document.getElementById('userDropdown');
  if (!btn || !dropdown) return;

  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    dropdown.classList.toggle('open');
  });

  document.addEventListener('click', () => {
    dropdown.classList.remove('open');
  });
})();


// ─── Toggle password visibility ───────────────────────────────
function togglePassword(fieldId, btn) {
  const field = document.getElementById(fieldId);
  if (!field) return;
  const isText = field.type === 'text';
  field.type = isText ? 'password' : 'text';
  const icon = btn.querySelector('i');
  if (icon) {
    icon.classList.toggle('fa-eye', isText);
    icon.classList.toggle('fa-eye-slash', !isText);
  }
}
window.togglePassword = togglePassword;


// ─── Copy reference to clipboard ─────────────────────────────
function copyRef(text, btn) {
  if (!text) return;
  navigator.clipboard.writeText(text).then(() => {
    const icon = btn ? btn.querySelector('i') : null;
    if (icon) {
      const original = icon.className;
      icon.className = 'fa-solid fa-check';
      btn.style.color = 'var(--accent-green)';
      setTimeout(() => {
        icon.className = original;
        btn.style.color = '';
      }, 1500);
    }
  }).catch(() => {
    // Fallback pour les navigateurs sans clipboard API
    const el = document.createElement('textarea');
    el.value = text;
    el.style.position = 'fixed';
    el.style.opacity = '0';
    document.body.appendChild(el);
    el.select();
    try { document.execCommand('copy'); } catch (e) { /* noop */ }
    document.body.removeChild(el);
  });
}
window.copyRef = copyRef;


// ─── Watchlist toggle (AJAX) ──────────────────────────────────
async function toggleWatchlist(btn) {
  const idweb = btn.dataset.idweb;
  if (!idweb) return;

  // Récupérer le token CSRF depuis le meta tag ou un champ hidden
  const csrfMeta = document.querySelector('meta[name="csrf-token"]');
  const csrfField = document.querySelector('input[name="csrf_token"]');
  const csrf = csrfMeta ? csrfMeta.content : (csrfField ? csrfField.value : '');

  btn.disabled = true;
  try {
    const resp = await fetch(`/watchlist/toggle/${encodeURIComponent(idweb)}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrf,
      },
      credentials: 'same-origin',
    });

    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    const icon = btn.querySelector('i');
    if (data.status === 'added') {
      btn.classList.add('wl-active');
      if (icon) { icon.className = 'fa-solid fa-star'; }
      btn.title = 'Retirer de la watchlist';
      showToast('Ajouté à votre watchlist', 'success');
    } else {
      btn.classList.remove('wl-active');
      if (icon) { icon.className = 'fa-regular fa-star'; }
      btn.title = 'Ajouter à la watchlist';
      showToast('Retiré de la watchlist', 'info');
    }
  } catch (err) {
    showToast('Erreur lors de la mise à jour de la watchlist', 'danger');
  } finally {
    btn.disabled = false;
  }
}
window.toggleWatchlist = toggleWatchlist;


// ─── Timeline panels (expand/collapse) ───────────────────────
function togglePanel(panelId) {
  const panel = document.getElementById(panelId);
  if (!panel) return;
  panel.classList.toggle('open');

  // Toggle chevron
  const header = panel.previousElementSibling;
  if (header) {
    const toggle = header.querySelector('.tl-v-toggle');
    if (toggle) toggle.classList.toggle('open');
  }
}
window.togglePanel = togglePanel;

// Ouvrir le premier panel par défaut + le panel attribution s'il existe
document.addEventListener('DOMContentLoaded', () => {
  ['panel-initial', 'panel-attribution'].forEach(id => {
    const panel = document.getElementById(id);
    if (panel) {
      panel.classList.add('open');
      const header = panel.previousElementSibling;
      if (header) {
        const toggle = header.querySelector('.tl-v-toggle');
        if (toggle) toggle.classList.add('open');
      }
    }
  });
});


// ─── Live search (debounced) ──────────────────────────────────
(function () {
  const searchInput = document.getElementById('searchInput');
  if (!searchInput) return;

  let debounceTimer;
  const form = searchInput.closest('form');

  searchInput.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      if (form) form.submit();
    }, 500);
  });

  searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      clearTimeout(debounceTimer);
      if (form) form.submit();
    }
  });
})();


// ─── Alert frequency toggle ───────────────────────────────────
(function () {
  const alertToggle = document.querySelector('input[name="alert_enabled"]');
  const freqGroup = document.getElementById('alertFreqGroup');
  if (!alertToggle || !freqGroup) return;

  function syncFreq() {
    freqGroup.style.opacity = alertToggle.checked ? '1' : '0.4';
    freqGroup.style.pointerEvents = alertToggle.checked ? '' : 'none';
  }

  alertToggle.addEventListener('change', syncFreq);
  syncFreq();
})();


// ─── Password strength indicator ─────────────────────────────
(function () {
  const pwdField = document.getElementById('password');
  const strengthEl = document.getElementById('pwdStrength');
  if (!pwdField || !strengthEl) return;

  pwdField.addEventListener('input', () => {
    const val = pwdField.value;
    let score = 0;
    if (val.length >= 8) score++;
    if (val.length >= 12) score++;
    if (/[A-Z]/.test(val)) score++;
    if (/[0-9]/.test(val)) score++;
    if (/[^A-Za-z0-9]/.test(val)) score++;

    const levels = ['', 'Très faible', 'Faible', 'Moyen', 'Fort', 'Très fort'];
    const colors = ['', '#EF4444', '#F59E0B', '#F59E0B', '#10B981', '#10B981'];

    if (val.length === 0) {
      strengthEl.textContent = '';
      strengthEl.style.color = '';
    } else {
      strengthEl.textContent = levels[score] || levels[1];
      strengthEl.style.color = colors[score] || colors[1];
      strengthEl.style.fontSize = '12px';
      strengthEl.style.marginTop = '4px';
    }
  });
})();


// ─── Toggle filtre expirés ────────────────────────────────────
function toggleExpire(btn) {
  const input = document.getElementById('expireInput');
  if (!input) return;
  const current = input.value;
  const next = current === 'avec' ? 'sans' : 'avec';
  input.value = next;

  // Mettre à jour le libellé et le style du bouton
  const strong = btn.querySelector('strong');
  if (strong) strong.textContent = next;
  if (next === 'sans') {
    btn.classList.add('expire-btn-off');
    btn.classList.remove('expire-btn-on');
    btn.title = 'Afficher les dossiers expirés';
  } else {
    btn.classList.add('expire-btn-on');
    btn.classList.remove('expire-btn-off');
    btn.title = 'Masquer les dossiers expirés';
  }

  // Soumettre le formulaire
  const form = btn.closest('form');
  if (form) form.submit();
}
window.toggleExpire = toggleExpire;


// ─── Toggle filtre période (Tous / Actifs) ────────────────────
function togglePeriode(btn) {
  const input = document.getElementById('periodeInput');
  if (!input) return;
  const next = input.value === 'tous' ? 'actifs' : 'tous';
  input.value = next;

  const strong = btn.querySelector('strong');
  if (strong) strong.textContent = next === 'actifs' ? 'Actifs' : 'Tous';
  if (next === 'actifs') {
    btn.classList.add('periode-btn-actifs');
    btn.classList.remove('periode-btn-tous');
    btn.title = 'Afficher tous les dossiers';
  } else {
    btn.classList.add('periode-btn-tous');
    btn.classList.remove('periode-btn-actifs');
    btn.title = 'Afficher uniquement les dossiers des 90 derniers jours';
  }

  const form = btn.closest('form');
  if (form) form.submit();
}
window.togglePeriode = togglePeriode;


// ─── Toggle filtre attribués ──────────────────────────────────
function toggleAttribue(btn) {
  const input = document.getElementById('attribueInput');
  if (!input) return;
  const next = input.value === 'avec' ? 'sans' : 'avec';
  input.value = next;

  const strong = btn.querySelector('strong');
  if (strong) strong.textContent = next;
  if (next === 'sans') {
    btn.classList.add('attribue-btn-off');
    btn.classList.remove('attribue-btn-on');
    btn.title = 'Afficher les dossiers attribués';
  } else {
    btn.classList.add('attribue-btn-on');
    btn.classList.remove('attribue-btn-off');
    btn.title = 'Masquer les dossiers attribués';
  }

  const form = btn.closest('form');
  if (form) form.submit();
}
window.toggleAttribue = toggleAttribue;


// ─── Share dossier ────────────────────────────────────────────
async function shareDossier(idweb, btn) {
  if (!idweb) return;
  const icon = btn ? btn.querySelector('i') : null;
  if (icon) icon.className = 'fa-solid fa-spinner fa-spin';
  if (btn) btn.disabled = true;

  try {
    const resp = await fetch(`/dossier/${encodeURIComponent(idweb)}/share`, {
      method: 'POST',
      credentials: 'same-origin',
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    _showShareModal(data.url);
  } catch (err) {
    showToast('Impossible de générer le lien de partage', 'danger');
  } finally {
    if (icon) icon.className = 'fa-solid fa-share-nodes';
    if (btn) btn.disabled = false;
  }
}
window.shareDossier = shareDossier;

function _showShareModal(url) {
  // Supprimer un éventuel modal existant
  document.getElementById('shareModal')?.remove();

  const modal = document.createElement('div');
  modal.id = 'shareModal';
  modal.className = 'share-modal-overlay';
  modal.innerHTML = `
    <div class="share-modal">
      <div class="share-modal-header">
        <i class="fa-solid fa-share-nodes"></i>
        <span>Partager ce dossier</span>
        <button class="share-modal-close" onclick="document.getElementById('shareModal').remove()">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>
      <div class="share-modal-body">
        <p>Ce lien donne accès à une vue en lecture seule du dossier, sans connexion requise. Il est valable <strong>90 jours</strong>.</p>
        <div class="share-url-row">
          <input type="text" class="share-url-input" value="${url}" readonly onclick="this.select()" />
          <button class="btn btn-primary btn-sm share-copy-btn" onclick="_copyShareUrl(this, '${url}')">
            <i class="fa-regular fa-copy"></i> Copier
          </button>
        </div>
      </div>
    </div>
  `;

  // Fermer en cliquant sur l'overlay
  modal.addEventListener('click', (e) => {
    if (e.target === modal) modal.remove();
  });

  document.body.appendChild(modal);
  // Focus sur l'input pour select facile
  setTimeout(() => modal.querySelector('.share-url-input')?.select(), 50);
}

function _copyShareUrl(btn, url) {
  navigator.clipboard.writeText(url).then(() => {
    const orig = btn.innerHTML;
    btn.innerHTML = '<i class="fa-solid fa-check"></i> Copié !';
    btn.style.background = 'var(--accent-green)';
    setTimeout(() => { btn.innerHTML = orig; btn.style.background = ''; }, 2000);
  }).catch(() => {
    // fallback
    const el = document.createElement('textarea');
    el.value = url;
    el.style.position = 'fixed'; el.style.opacity = '0';
    document.body.appendChild(el); el.select();
    try { document.execCommand('copy'); } catch (_) {}
    document.body.removeChild(el);
  });
}
window._copyShareUrl = _copyShareUrl;


// ─── Toast notifications ──────────────────────────────────────
function showToast(message, type = 'info') {
  const container = document.querySelector('.flash-container') || createToastContainer();

  const toast = document.createElement('div');
  toast.className = `flash flash-${type}`;

  const icons = { success: 'fa-circle-check', danger: 'fa-circle-xmark', warning: 'fa-triangle-exclamation', info: 'fa-circle-info' };
  const iconClass = icons[type] || icons.info;

  toast.innerHTML = `
    <i class="fa-solid ${iconClass}"></i>
    ${message}
    <button class="flash-close" onclick="this.parentElement.remove()"><i class="fa-solid fa-xmark"></i></button>
  `;

  container.appendChild(toast);
  setTimeout(() => { toast.remove(); }, 4000);
}
window.showToast = showToast;

function createToastContainer() {
  const c = document.createElement('div');
  c.className = 'flash-container';
  document.body.appendChild(c);
  return c;
}


// ─── Auto-dismiss flash messages ─────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.flash').forEach((flash) => {
    setTimeout(() => {
      flash.style.transition = 'opacity 0.5s';
      flash.style.opacity = '0';
      setTimeout(() => flash.remove(), 500);
    }, 5000);
  });
});


// ─── CSRF token dans les requêtes AJAX fetch ──────────────────
(function () {
  const originalFetch = window.fetch;
  window.fetch = function (url, options = {}) {
    if (typeof url === 'string' && url.startsWith('/')) {
      const csrfField = document.querySelector('input[name="csrf_token"]');
      if (csrfField) {
        options.headers = options.headers || {};
        if (!options.headers['X-CSRFToken']) {
          options.headers['X-CSRFToken'] = csrfField.value;
        }
      }
    }
    return originalFetch(url, options);
  };
})();


// ─── Loading overlay ──────────────────────────────────────────
function showLoading(msg) {
  const overlay = document.getElementById('loadingOverlay');
  const label   = document.getElementById('loadingMsg');
  if (!overlay) return;
  if (label && msg) label.textContent = msg;
  overlay.style.display = 'flex';
}
window.showLoading = showLoading;

// Bouton "Refresh" du dashboard (form= attribute)
document.addEventListener('click', function (e) {
  const btn = e.target.closest('button[form="refreshForm"]');
  if (!btn) return;
  showLoading('Récupération des données BOAMP / TED…');
});
