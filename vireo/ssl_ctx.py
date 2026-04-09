"""Shared SSL context using certifi's CA bundle.

On macOS, Python's default certificate store is often empty unless the user
runs ``Install Certificates.command``.  Using certifi's bundled Mozilla CA
certificates ensures HTTPS works out of the box on all platforms.
"""

import ssl

import certifi

ssl_ctx = ssl.create_default_context(cafile=certifi.where())
