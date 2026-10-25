"""Reach into a module page from a browser test.

Module pages (``frontend-module-pages.json``) keep their state and functions
in ES modules, not globals, so ``page.evaluate("selection.size")`` has nothing
to read. ``import()`` of a module URL the page already loaded returns the
page's own instance, so a test can import the modules it needs and read or
call their exports. The page never publishes them; the test does.

Two ways in:

- ``import_page_modules(page, directory, names)`` imports the named modules
  once, now, into ``window.pageModules.<name>`` (the Misses tests).
- ``install_page_module_exposure(page, directory, state_name)`` does it for
  every module of the page on every navigation to it, and names the page's
  ``state`` object ``window.<state_name>``. The ``page`` fixture installs it
  for Browse, so Browse tests read ``pageModules.selectionState.selection.focusedPhotoId`` and call
  ``pageModules.grid.renderGrid()`` (the module's file name without
  ``.js``, camelCased: ``pageModules.selectionPanel`` for
  ``selection-panel.js``).
"""
import json
from pathlib import Path

STATIC = Path(__file__).resolve().parents[2] / "vireo" / "static"


def import_page_modules(page, directory, names):
    """Import ``/static/<directory>/<name>.js`` for each name and publish the
    module namespaces to the test as ``window.pageModules.<name>``."""
    page.evaluate(
        """async ([directory, names]) => {
          window.pageModules = window.pageModules || {};
          for (const name of names) {
            window.pageModules[name] = await import(`/static/${directory}/${name}.js`);
          }
        }""",
        [directory, names],
    )


def page_module_names(directory):
    """Every module file of a module page, without ``.js``."""
    return sorted(path.stem for path in (STATIC / directory).glob("*.js"))


_EXPOSE = """async ([directory, names, stateName]) => {
  if (document.readyState === 'loading') {
    await new Promise(resolve => document.addEventListener('DOMContentLoaded', resolve, {once: true}));
  }
  if (!document.querySelector(`script[type="module"][src="/static/${directory}/boot.js"]`)) return false;
  window.pageModules = window.pageModules || {};
  for (const name of names) {
    const key = name.replace(/-(\w)/g, (_, letter) => letter.toUpperCase());
    window.pageModules[key] = await import(`/static/${directory}/${name}.js`);
  }
  if (stateName) window[stateName] = window.pageModules.state.state;
  return true;
}"""


def expose_page_modules(page, directory, state_name=None):
    """Publish every module of the page at ``directory`` (if the page loaded
    it) to the test, and its state object as ``window.<state_name>``."""
    return page.evaluate(_EXPOSE, [directory, page_module_names(directory), state_name])


def install_page_module_exposure(page, directory, state_name=None):
    """``expose_page_modules`` on every navigation: an init script runs it
    once each document has parsed (links and keyboard navigation included),
    and ``page.goto`` / ``page.reload`` wait for it (``page.after_navigation``,
    see navbar_modules.py)."""
    args = json.dumps([directory, page_module_names(directory), state_name])
    page.add_init_script(f"({_EXPOSE})({args}).catch(() => {{}});")
    page.after_navigation.append(lambda p: expose_page_modules(p, directory, state_name))
    return page
