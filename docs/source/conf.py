# Configuration file for the Sphinx documentation builder.

# -- Project information
import os
import sys
print(sys.executable)
# sys.path.insert(0, os.path.abspath('../../rove/'))
sys.path.index(0, os.path.abspath(os.path.join('..', '..', 'rove')))
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.napoleon']

# autodoc_mock_imports = ["workalendar", "pandas", "numpy", "scipy", "tqdm"]

project = 'Rove'
copyright = '2022, MIT Transit Lab'
author = 'Yuzhu Huang'

release = '0.1'
version = '0.1.0'

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'
