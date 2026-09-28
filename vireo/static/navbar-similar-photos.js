function findSimilar(photoId) {
  var alreadyOpen = !!window._similarEscToken;
  if (alreadyOpen) Keymap.popEsc(window._similarEscToken);
  window._similarEscToken = Keymap.pushEsc(function() { closeSimilar(); });

  var overlay = document.getElementById('similarOverlay');
  var content = document.getElementById('similarContent');
  overlay.classList.add('active');
  if (!alreadyOpen) Keymap.lockBodyScroll();
  content.innerHTML = '<p style="color:var(--text-dim);">Searching for similar photos...</p>';

  safeFetch('/api/photos/' + photoId + '/similar?limit=40', {}, { toast: false })
    .then(function(data) {
      if (data.error) {
        content.innerHTML = '<p style="color:var(--warning);">' + escapeHtml(data.error) + '</p>';
        return;
      }
      // Filenames are user-controlled: build cards with DOM methods and
      // closure-bound click handlers instead of interpolating into
      // HTML/onclick strings — a quote or angle bracket in a filename broke
      // the inline handler (and could inject markup).
      content.textContent = '';
      var summary = document.createElement('p');
      summary.style.cssText = 'font-size:12px;color:var(--text-dim);margin-bottom:12px;';
      summary.textContent = 'Compared against ' + data.total_compared.toLocaleString() + ' photos with embeddings';
      content.appendChild(summary);

      if (data.similar.length === 0) {
        var none = document.createElement('p');
        none.style.color = 'var(--text-dim)';
        none.textContent = 'No similar photos found.';
        content.appendChild(none);
      } else {
        var grid = document.createElement('div');
        grid.className = 'similar-grid';
        var similarPhotoList = data.similar.map(function(item) {
          return item.photo;
        });
        data.similar.forEach(function(item) {
          var p = item.photo;
          var pct = Math.round(item.similarity * 100);
          var card = document.createElement('div');
          card.className = 'similar-card';
          card.addEventListener('click', function() {
            closeSimilar();
            var lightboxOptions = typeof window.getLightboxOpenOptions === 'function'
              ? window.getLightboxOpenOptions(p.id)
              : undefined;
            openLightbox(p.id, p.filename || '', similarPhotoList, lightboxOptions);
          });
          var img = document.createElement('img');
          img.src = window.vireoThumbnailUrl
            ? window.vireoThumbnailUrl(p)
            : '/thumbnails/' + p.id + '.jpg';
          img.loading = 'lazy';
          img.alt = '';
          var info = document.createElement('div');
          info.className = 'similar-card-info';
          var name = document.createElement('div');
          name.className = 'similar-card-name';
          name.title = p.filename || '';
          name.textContent = p.filename || '';
          var score = document.createElement('div');
          score.className = 'similar-card-score';
          score.textContent = pct + '% similar';
          info.append(name, score);
          card.append(img, info);
          grid.appendChild(card);
        });
        content.appendChild(grid);
      }
    })
    .catch(function(e) {
      content.innerHTML = '<p style="color:var(--danger);">Failed: ' + escapeHtml(e.message) + '</p>';
    });
}

function closeSimilar() {
  var wasOpen = !!window._similarEscToken;
  if (wasOpen) { Keymap.popEsc(window._similarEscToken); window._similarEscToken = null; }
  document.getElementById('similarOverlay').classList.remove('active');
  if (wasOpen) Keymap.unlockBodyScroll();
}
