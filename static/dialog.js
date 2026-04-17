/* MakoletChain — Custom Dialog System
   Replaces browser-native confirm(), alert(), prompt()
   with dark-themed, RTL-aware, mobile-responsive dialogs.
*/
(function () {
  // Inject CSS once
  const style = document.createElement('style');
  style.textContent = `
    .mk-backdrop {
      position: fixed; inset: 0; z-index: 10000;
      background: rgba(0,0,0,0.65);
      backdrop-filter: blur(4px); -webkit-backdrop-filter: blur(4px);
      display: flex; align-items: center; justify-content: center;
      opacity: 0; transition: opacity 0.2s ease;
      padding: 1rem;
    }
    .mk-backdrop.mk-visible { opacity: 1; }

    .mk-box {
      background: #1e293b; border: 1px solid #475569;
      border-radius: 16px; max-width: 400px; width: 100%;
      padding: 2rem 1.5rem 1.5rem; text-align: center;
      transform: scale(0.9); transition: transform 0.2s ease;
      direction: rtl;
    }
    .mk-backdrop.mk-visible .mk-box { transform: scale(1); }

    .mk-icon {
      width: 56px; height: 56px; border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      margin: 0 auto 1rem; font-size: 1.6rem;
    }
    .mk-icon-danger  { background: rgba(239,68,68,0.15); color: #f87171; }
    .mk-icon-warning { background: rgba(245,158,11,0.15); color: #fbbf24; }
    .mk-icon-info    { background: rgba(99,102,241,0.15); color: #818cf8; }
    .mk-icon-success { background: rgba(34,197,94,0.15); color: #4ade80; }

    .mk-title {
      font-size: 1.15rem; font-weight: 700; color: #fff;
      margin: 0 0 0.5rem;
    }
    .mk-message {
      font-size: 0.95rem; color: #cbd5e1; line-height: 1.5;
      margin: 0 0 1.25rem; white-space: pre-line;
    }

    .mk-input {
      width: 100%; box-sizing: border-box;
      background: #0f172a; border: 1px solid #475569;
      border-radius: 8px; padding: 0.7rem 0.9rem;
      color: #e2e8f0; font-size: 16px; /* prevent iOS zoom */
      font-family: inherit; direction: rtl;
      outline: none; margin-bottom: 1.25rem;
    }
    .mk-input:focus { border-color: #6366f1; }

    .mk-buttons {
      display: flex; flex-direction: row-reverse; gap: 0.6rem;
    }
    .mk-btn {
      flex: 1; min-height: 44px; border: none; border-radius: 10px;
      font-size: 0.95rem; font-weight: 600; cursor: pointer;
      font-family: inherit; transition: filter 0.15s;
    }
    .mk-btn:hover { filter: brightness(1.15); }
    .mk-btn:active { filter: brightness(0.95); }

    .mk-btn-primary  { background: #6366f1; color: #fff; }
    .mk-btn-danger   { background: #dc2626; color: #fff; }
    .mk-btn-success  { background: #16a34a; color: #fff; }
    .mk-btn-warning  { background: #d97706; color: #fff; }
    .mk-btn-cancel   { background: #334155; color: #cbd5e1; }

    @media (max-width: 768px) {
      .mk-box { max-width: 100%; }
      .mk-buttons { flex-direction: column-reverse; }
      .mk-btn { width: 100%; }
    }
  `;
  document.head.appendChild(style);

  const ICONS = {
    danger:  '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
    warning: '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    info:    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>',
    success: '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>'
  };

  function buildDialog({ type, title, message, buttons, input }) {
    const backdrop = document.createElement('div');
    backdrop.className = 'mk-backdrop';

    const primaryClass = type === 'danger' ? 'mk-btn-danger'
                       : type === 'success' ? 'mk-btn-success'
                       : type === 'warning' ? 'mk-btn-warning'
                       : 'mk-btn-primary';

    let inputHTML = '';
    if (input) {
      inputHTML = `<input class="mk-input" placeholder="${input.placeholder || ''}" value="${input.defaultValue || ''}">`;
    }

    backdrop.innerHTML = `
      <div class="mk-box">
        <div class="mk-icon mk-icon-${type}">${ICONS[type] || ICONS.info}</div>
        <div class="mk-title">${title}</div>
        <div class="mk-message">${message}</div>
        ${inputHTML}
        <div class="mk-buttons">
          ${buttons.map(b =>
            `<button class="mk-btn ${b.primary ? primaryClass : 'mk-btn-cancel'}" data-action="${b.action}">${b.text}</button>`
          ).join('')}
        </div>
      </div>
    `;

    document.body.appendChild(backdrop);
    // Force reflow then animate in
    backdrop.offsetHeight;
    backdrop.classList.add('mk-visible');

    return backdrop;
  }

  function removeDialog(backdrop) {
    backdrop.classList.remove('mk-visible');
    setTimeout(() => backdrop.remove(), 200);
  }

  // ── mkConfirm ──
  window.mkConfirm = function ({ type = 'info', title, message, confirmText = '\u05D0\u05D9\u05E9\u05D5\u05E8', cancelText = '\u05D1\u05D9\u05D8\u05D5\u05DC' } = {}) {
    return new Promise(resolve => {
      const backdrop = buildDialog({
        type, title, message,
        buttons: [
          { text: confirmText, primary: true, action: 'confirm' },
          { text: cancelText, primary: false, action: 'cancel' }
        ]
      });

      function finish(val) {
        removeDialog(backdrop);
        resolve(val);
      }

      backdrop.querySelectorAll('.mk-btn').forEach(btn => {
        btn.addEventListener('click', () => finish(btn.dataset.action === 'confirm'));
      });
      backdrop.addEventListener('click', e => { if (e.target === backdrop) finish(false); });
      document.addEventListener('keydown', function handler(e) {
        if (e.key === 'Escape') { document.removeEventListener('keydown', handler); finish(false); }
      });
    });
  };

  // ── mkAlert ──
  window.mkAlert = function ({ type = 'info', title, message, confirmText = '\u05D4\u05D1\u05E0\u05EA\u05D9' } = {}) {
    return new Promise(resolve => {
      const backdrop = buildDialog({
        type, title, message,
        buttons: [
          { text: confirmText, primary: true, action: 'ok' }
        ]
      });

      function finish() {
        removeDialog(backdrop);
        resolve();
      }

      backdrop.querySelector('.mk-btn').addEventListener('click', finish);
      backdrop.addEventListener('click', e => { if (e.target === backdrop) finish(); });
      document.addEventListener('keydown', function handler(e) {
        if (e.key === 'Escape') { document.removeEventListener('keydown', handler); finish(); }
      });
    });
  };

  // ── mkPrompt ──
  window.mkPrompt = function ({ type = 'info', title, message, placeholder = '', defaultValue = '', confirmText = '\u05D0\u05D9\u05E9\u05D5\u05E8', cancelText = '\u05D1\u05D9\u05D8\u05D5\u05DC' } = {}) {
    return new Promise(resolve => {
      const backdrop = buildDialog({
        type, title, message,
        input: { placeholder, defaultValue },
        buttons: [
          { text: confirmText, primary: true, action: 'confirm' },
          { text: cancelText, primary: false, action: 'cancel' }
        ]
      });

      const input = backdrop.querySelector('.mk-input');
      if (input) { input.focus(); input.select(); }

      function finish(val) {
        removeDialog(backdrop);
        resolve(val);
      }

      backdrop.querySelectorAll('.mk-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          finish(btn.dataset.action === 'confirm' ? (input ? input.value : '') : null);
        });
      });
      if (input) {
        input.addEventListener('keydown', e => {
          if (e.key === 'Enter') finish(input.value);
        });
      }
      backdrop.addEventListener('click', e => { if (e.target === backdrop) finish(null); });
      document.addEventListener('keydown', function handler(e) {
        if (e.key === 'Escape') { document.removeEventListener('keydown', handler); finish(null); }
      });
    });
  };
})();
