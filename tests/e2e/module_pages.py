"""Reach into a module page from a browser test.

Module pages (``frontend-module-pages.json``) keep their state and functions
in ES modules, not globals, so ``page.evaluate("selection.size")`` has nothing
to read. ``import()`` of a module URL the page already loaded returns the
page's own instance, so a test can import the modules it needs and read or
call their exports. The page never publishes them; the test does.
"""


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
