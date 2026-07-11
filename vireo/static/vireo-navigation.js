/* Compatibility-preserving navigation groups for secondary workflows. */
(function(global) {
  'use strict';

  var Vireo = global.Vireo = global.Vireo || {};
  var GROUPS = [
    {
      id: 'review', label: 'Review modes',
      pages: [
        ['review', 'Review Queue', '/review'],
        ['pipeline_review', 'Process Review', '/pipeline/review'],
        ['pipeline_rapid_review', 'Rapid Review', '/pipeline/rapid-review'],
        ['cull', 'Cull', '/cull'],
        ['misses', 'Misses', '/misses'],
        ['compare', 'Compare', '/compare'],
        ['variants', 'Variants', '/variants'],
      ],
    },
    {
      id: 'library', label: 'Library',
      pages: [
        ['highlights', 'Highlights', '/highlights'],
        ['life_list', 'Life List', '/life-list'],
        ['map', 'Map', '/map'],
        ['keywords', 'Keywords', '/keywords'],
        ['duplicates', 'Duplicates', '/duplicates'],
        ['dashboard', 'Dashboard', '/dashboard'],
      ],
    },
    {
      id: 'tools', label: 'Tools',
      pages: [
        ['jobs', 'Jobs', '/jobs'],
        ['move', 'Move', '/move'],
        ['storage', 'Storage', '/storage'],
        ['audit', 'Audit', '/audit'],
        ['lightroom', 'Lightroom', '/lightroom'],
        ['logs', 'Logs', '/logs'],
        ['shortcuts', 'Shortcuts', '/shortcuts'],
      ],
    },
    {
      id: 'application', label: 'App',
      pages: [
        ['workspace', 'Workspace', '/workspace'],
        ['settings', 'Settings', '/settings'],
      ],
    },
  ];

  function currentId() {
    var path = global.location.pathname;
    if (path.indexOf('/pipeline/rapid-review') === 0) return 'pipeline_rapid_review';
    if (path.indexOf('/pipeline/review') === 0) return 'pipeline_review';
    if (path === '/life-list') return 'life_list';
    return (path.split('/')[1] || 'browse').replace(/-/g, '_');
  }

  function closeMenus(except) {
    document.querySelectorAll('.nav-section-menu.open').forEach(function(menu) {
      if (menu !== except) menu.classList.remove('open');
    });
    document.querySelectorAll('.nav-section-button[aria-expanded="true"]').forEach(function(btn) {
      if (!except || btn.nextElementSibling !== except) btn.setAttribute('aria-expanded', 'false');
    });
  }

  function render() {
    var host = document.getElementById('navSectionMenus');
    if (!host || host.childElementCount) return;
    var active = currentId();

    GROUPS.forEach(function(group) {
      var wrap = document.createElement('span');
      wrap.className = 'nav-section';
      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'nav-section-button';
      button.textContent = group.label + ' ▾';
      button.setAttribute('aria-haspopup', 'menu');
      button.setAttribute('aria-expanded', 'false');
      button.dataset.section = group.id;

      var menu = document.createElement('span');
      menu.className = 'nav-section-menu';
      menu.setAttribute('role', 'menu');
      group.pages.forEach(function(page) {
        var link = document.createElement('a');
        link.href = page[2];
        link.textContent = page[1];
        link.dataset.navId = page[0];
        link.setAttribute('role', 'menuitem');
        if (page[0] === active) {
          link.classList.add('active');
          button.classList.add('active');
        }
        menu.appendChild(link);
      });

      button.addEventListener('click', function(event) {
        event.stopPropagation();
        var opening = !menu.classList.contains('open');
        closeMenus(menu);
        menu.classList.toggle('open', opening);
        button.setAttribute('aria-expanded', opening ? 'true' : 'false');
        if (opening) {
          var first = menu.querySelector('a');
          if (first) first.focus();
        }
      });
      menu.addEventListener('keydown', function(event) {
        var links = Array.from(menu.querySelectorAll('a'));
        var index = links.indexOf(document.activeElement);
        if (event.key === 'ArrowDown') index = Math.min(links.length - 1, index + 1);
        else if (event.key === 'ArrowUp') index = Math.max(0, index - 1);
        else if (event.key === 'Escape') {
          closeMenus();
          button.focus();
          return;
        } else return;
        event.preventDefault();
        links[index].focus();
      });
      wrap.appendChild(button);
      wrap.appendChild(menu);
      host.appendChild(wrap);
    });
  }

  document.addEventListener('click', function() { closeMenus(); });
  document.addEventListener('DOMContentLoaded', render);
  Vireo.navigation = {groups: GROUPS, render: render, closeMenus: closeMenus};
})(window);
