(function () {
  'use strict';
  const panel = document.getElementById('pendingArchives');
  if (!panel) return;
  const list = document.getElementById('pendingArchiveItems');
  const error = document.getElementById('pendingArchiveError');
  let signature = '';
  let loading = false;
  let sending = false;
  let actionError = '';

  async function refresh() {
    if (loading || sending || document.hidden) return;
    loading = true;
    try {
      const response = await fetch('/api/import/pending-archives');
      if (!response.ok) throw new Error('Could not check photos waiting for NAS transfer.');
      const data = await response.json();
      error.textContent = actionError;
      const next = JSON.stringify(data.items || []);
      if (next === signature) return;
      signature = next;
      list.replaceChildren();
      panel.hidden = !data.items.length;
      data.items.forEach(item => {
        const row = document.createElement('div');
        row.style.cssText = 'margin-top:10px;font-size:12px;overflow-wrap:anywhere;';
        const title = document.createElement('strong');
        title.textContent = item.name;
        row.appendChild(title);
        const destination = document.createElement('div');
        destination.textContent = 'Destination: ' + item.destination;
        row.appendChild(destination);
        if (item.collection_id) {
          const review = document.createElement('a');
          review.href = '/browse?collection_id=' + encodeURIComponent(item.collection_id);
          review.textContent = 'Review photos';
          review.style.marginRight = '10px';
          row.appendChild(review);
        }
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn';
        button.textContent = item.state === 'sending' ? 'Sending to NAS…' : 'Send to NAS';
        button.disabled = item.state !== 'ready';
        button.addEventListener('click', async () => {
          if (sending) return;
          sending = true;
          button.disabled = true;
          error.textContent = '';
          actionError = '';
          try {
            const response = await fetch('/api/import/pending-archives/' + encodeURIComponent(item.id) + '/send', {method: 'POST'});
            const result = await response.json();
            if (!response.ok) throw new Error(result.error || 'Could not start the NAS transfer.');
          } catch (e) {
            actionError = e.message;
            error.textContent = actionError;
          } finally {
            sending = false;
            signature = '';
            refresh();
          }
        });
        row.appendChild(button);
        if (item.error || item.state === 'waiting') {
          const note = document.createElement('div');
          note.textContent = item.error || 'Waiting for running jobs to finish.';
          row.appendChild(note);
        }
        list.appendChild(row);
      });
    } catch (e) {
      if (!panel.hidden) error.textContent = e.message;
    } finally {
      loading = false;
    }
  }
  refresh();
  setInterval(refresh, 5000);
  window.addEventListener('focus', refresh);
  document.addEventListener('visibilitychange', refresh);
})();
