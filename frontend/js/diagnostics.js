/**
 * diagnostics.js — Connection test UI for ONTAP S3 diagnostics.
 */

window.DiagView = (() => {
  const runBtn    = document.getElementById('runTestBtn');
  const results   = document.getElementById('diagResults');

  const STATUS_ICONS = {
    ok:      `<svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"/></svg>`,
    failed:  `<svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clip-rule="evenodd"/></svg>`,
    skipped: `<svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM7 9a1 1 0 000 2h6a1 1 0 000-2H7z" clip-rule="evenodd"/></svg>`,
  };

  function _esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function _renderLoading() {
    results.innerHTML = `
      <div class="diag-summary" style="border-color:var(--clr-warning);color:var(--clr-warning);background:color-mix(in srgb,var(--clr-warning) 10%,transparent)">
        <div class="spinner" style="width:22px;height:22px;border-width:2px;flex-shrink:0"></div>
        Running connection tests…
      </div>`;
  }

  function _renderResults(data) {
    const isOk = data.overall === 'ok';

    let html = `
      <div class="diag-summary ${data.overall}">
        ${STATUS_ICONS[isOk ? 'ok' : 'failed']}
        ${isOk ? 'All connection tests passed — ONTAP S3 is reachable.' : 'Connection test failed — see step details below.'}
      </div>

      <div class="diag-config">
        <div class="diag-config-title">Configuration Used</div>
        <div class="diag-config-grid">
          ${_cfgRow('Endpoint',         data.endpoint)}
          ${_cfgRow('Region',           data.region)}
          ${_cfgRow('Addressing Style', data.addressing_style)}
          ${_cfgRow('TLS Verify',       String(data.tls_verify))}
          ${_cfgRow('CA Bundle',        data.ca_bundle || 'OS trust store')}
        </div>
      </div>

      <div class="diag-steps">
        ${data.steps.map(step => _renderStep(step)).join('')}
      </div>`;

    results.innerHTML = html;

    // Toggle step detail panels
    results.querySelectorAll('.diag-step__header').forEach(header => {
      header.addEventListener('click', () => {
        const step = header.closest('.diag-step');
        step.classList.toggle('open');
      });
    });
  }

  function _cfgRow(label, value) {
    return `<div class="meta-row">
      <span class="meta-row__key">${label}</span>
      <span class="meta-row__value">${_esc(value || '—')}</span>
    </div>`;
  }

  function _renderStep(step) {
    const hasDetail = step.detail || (typeof step.detail === 'object' && step.detail !== null);
    const detailText = hasDetail
      ? (typeof step.detail === 'object' ? JSON.stringify(step.detail, null, 2) : String(step.detail))
      : '';

    return `
      <div class="diag-step ${step.status}" role="article" aria-label="Step ${step.step}: ${_esc(step.name)}">
        <div class="diag-step__header" role="button" tabindex="0"
             aria-expanded="false" aria-controls="step-body-${step.step}">
          <div class="diag-step__num">${step.step}</div>
          <div style="flex:1">
            <div class="diag-step__name">${_esc(step.name)}</div>
            <div class="diag-step__message">${_esc(step.message)}</div>
          </div>
          <div class="diag-step__timing">${step.duration_ms ? step.duration_ms + ' ms' : ''}</div>
        </div>
        ${hasDetail ? `
        <div class="diag-step__body" id="step-body-${step.step}">
          <pre class="diag-step__detail">${_esc(detailText)}</pre>
        </div>` : ''}
      </div>`;
  }

  async function runTest() {
    runBtn.disabled = true;
    _renderLoading();
    try {
      const data = await API.testConnection();
      _renderResults(data);
      if (data.overall === 'ok') {
        Toast.success('Connection test passed!');
      } else {
        Toast.error('Connection test failed — check step details.');
      }
    } catch (err) {
      results.innerHTML = ``;
      renderError(err, results);
      Toast.error('Could not complete connection test.');
    } finally {
      runBtn.disabled = false;
    }
  }

  runBtn.addEventListener('click', runTest);

  // Keyboard on step headers
  document.addEventListener('keydown', e => {
    if (e.target.matches('.diag-step__header') && (e.key === 'Enter' || e.key === ' ')) {
      e.target.click();
    }
  });

  return { runTest };
})();
