"""
Shared page template for the Review maps published from maap_tools/maps (https://pahbs.github.io/maap_tools/maps/).

Every Review map notebook builds its folium map, then calls save_review_map() with a header from
build_header_html(), so all maps share one look: title + logos, a dataset table with one status
badge per row, a usage note, references, and a contact/last-updated footer.

Logo images live in maps/logos/ and are referenced relative to the saved html (maps/*.html).
"""
import json
import os
from datetime import datetime, timezone

import folium
from folium import MacroElement
from jinja2 import Template

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MAPS_DIR = os.path.join(REPO_DIR, 'maps')
CAVM_TREELINE_GEOJSON = os.path.join(REPO_DIR, 'data', 'cavm_treeline.geojson')

# key: (src relative to maps/, alt text, link)
LOGOS = {
    'nasa':     ('logos/nasa.png',     'NASA',                                     'https://www.nasa.gov'),
    'above':    ('logos/above.png',    'NASA Arctic-Boreal Vulnerability Experiment (ABoVE)', 'https://above.nasa.gov'),
    'umd':      ('logos/umd.png',      'University of Maryland',                   'https://umd.edu'),
    'edgehill': ('logos/edgehill.png', 'Edge Hill University',                     'https://www.edgehill.ac.uk'),
    'utaustin': ('logos/utaustin.png', 'The University of Texas at Austin',        'https://www.utexas.edu'),
    'abr':      ('logos/abr.png',      'ABR, Inc. Environmental Research & Services', 'https://www.abrinc.com'),
    'nasa_dsg': ('logos/nasa_dsg.svg', 'NASA Goddard Data Science Group',          'https://science.gsfc.nasa.gov/cisto/dsg/'),
}

# Contributors table entries: (name, affiliation)
UMD_GEOG = 'Dept. Geographical Sciences, U. Maryland-College Park'
PEOPLE = {
    'montesano':      ('Paul Montesano', 'NASA Goddard Space Flight Center / ESSIC, U. Maryland-College Park'),
    'duncanson':      ('Laura Duncanson', UMD_GEOG),
    'zarringhalam':   ('Ali Zarringhalam', UMD_GEOG),
    'minor':          ('David Minor', UMD_GEOG),
    'thomas':         ('Nathan Thomas', 'Edge Hill University'),
    'guenther':       ('Eric Guenther', 'U. Texas at Austin'),
    'neuenschwander': ('Amy Neuenschwander', 'U. Texas at Austin'),
}

# Opening views for set_extent(): (center [lat, lon], zoom)
EXTENTS = {
    'na_boreal': ([60, -115], 4),     # North American boreal
    'alaska':    ([64, -152], 5),
}

WIP = 'Work in progress'
COMPLETE = 'Complete'
# status: (background, text color)
STATUS_STYLES = {
    WIP:      ('#fef3c7', '#92400e'),   # yellow
    COMPLETE: ('#dcfce7', '#166534'),   # green
}

CONTACT = 'paul.m.montesano@nasa.gov'

# References used by more than one map: (text, doi or None)
REF_DUNCANSON = ('Duncanson et al. (2026). <em>Remote Sensing of Environment</em>.', '10.1016/j.rse.2026.115406')
REF_MONTESANO_TTE = ('Montesano et al. (2020). <em>Environmental Research Letters</em>.', '10.1088/1748-9326/abb2c7')
REF_CAVM = ('Walker et al. (2005). The Circumpolar Arctic vegetation map. <em>Journal of Vegetation Science</em>.',
            '10.1111/j.1654-1103.2005.tb02365.x')
REF_MONTESANO_INPREP = ('Montesano et al. (in prep.).', None)
REF_GAMI = ('Besnard et al. (2024). Global Age Mapping Integration (GAMI) v2.0. <em>GFZ Data Services</em>.',
            '10.5880/GFZ.1.4.2023.006')

FULLWIDTH_CSS = '''
<style>
  body { margin: 0; padding: 0; background-color: #ffffff; }
  .folium-map, div[id^="map_"] {
    width: 100% !important;
    height: 60vh !important;
    max-width: 100% !important;
  }
</style>
'''


def status_badge(status):
    bg, fg = STATUS_STYLES[status]
    return (f'<span style="display: inline-block; white-space: nowrap; background-color: {bg}; color: {fg}; '
            f'padding: 2px 8px; border-radius: 9999px; font-weight: 500; font-size: 11px;">{status}</span>')


def logos_html(logos):
    imgs = []
    for key in logos:
        src, alt, href = LOGOS[key]
        imgs.append(f'<a href="{href}" target="_blank" rel="noopener noreferrer" title="{alt}">'
                    f'<img src="{src}" alt="{alt}" style="height: 44px; width: auto; display: block;"></a>')
    return '\n      '.join(imgs)


SMALL_HEADING = ('font-weight: 600; color: #4b5563; margin-bottom: 4px; text-transform: uppercase; '
                 'font-size: 10.5px; letter-spacing: 0.05em;')


def contributors_html(contributors):
    '''Compact contributors list, same type size as the references and shown beside them.'''
    rows = '\n'.join(f'        <tr><td style="padding: 0 12px 0 0; vertical-align: top; color: #4b5563; white-space: nowrap;">'
                     f'{PEOPLE[k][0]}</td><td style="padding: 0; vertical-align: top;">{PEOPLE[k][1]}</td></tr>'
                     for k in contributors)
    return f'''
    <div style="flex: 1 1 280px; min-width: 0;">
      <div style="{SMALL_HEADING}">Contributors</div>
      <table style="border-collapse: collapse; text-align: left; font-size: inherit; line-height: inherit; color: inherit;">
{rows}
      </table>
    </div>'''


def build_header_html(title, subtitle, rows, note, refs, logos=('nasa',), contributors=('montesano',), contact=CONTACT):
    '''
    Page header for a Review map.
    rows         : list of dicts with keys 'layer', 'resolution', 'description', 'citation', 'status' (HTML allowed);
                   status is WIP or COMPLETE
    refs         : list of (text, doi) tuples; doi may be None
    logos        : keys of LOGOS, shown left to right
    contributors : keys of PEOPLE, listed in order
    '''
    td = 'padding: 10px 12px; vertical-align: top;'
    body_rows = '\n'.join(f'''        <tr style="border-bottom: 1px solid #f3f4f6;">
          <td style="{td} font-weight: 600; color: #111827;">{r['layer']}</td>
          <td style="{td} color: #4b5563;">{r['resolution']}</td>
          <td style="{td} color: #4b5563;">{r['description']}</td>
          <td style="{td} color: #6b7280;">{r['citation']}</td>
          <td style="{td}">{status_badge(r['status'])}</td>
        </tr>''' for r in rows)
    ref_items = '\n'.join(
        f'      <li>[{i}] {text} ' + (
            f'<a href="https://doi.org/{doi}" target="_blank" rel="noopener noreferrer" '
            f'style="color: #2563eb; text-decoration: none;">doi:{doi}</a>' if doi else
            '<span style="color: #9ca3af; font-style: italic;">DOI pending</span>') + '</li>'
        for i, (text, doi) in enumerate(refs, 1))
    refs_div = f'''
    <div style="flex: 2 1 440px; min-width: 0;">
      <div style="{SMALL_HEADING}">References &amp; Circumpolar Context</div>
      <ul style="margin: 0; padding-left: 18px;">
{ref_items}
      </ul>
    </div>''' if refs else ''
    # References and contributors side by side (they wrap to stacked on narrow screens)
    refs_contributors = f'''
  <section style="display: flex; flex-wrap: wrap; gap: 8px 32px; font-size: 11.5px; line-height: 1.5; color: #6b7280;
                  border-top: 1px solid #f3f4f6; padding-top: 10px; margin-bottom: 14px;">{refs_div}{contributors_html(contributors)}
  </section>'''
    updated = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    return f'''
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            max-width: 1400px; margin: 0 auto; padding: 24px 32px; background-color: #ffffff;
            border-radius: 8px; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05); color: #1a202c;">

  <header style="display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 18px;">
    <div style="flex: 1 1 480px; min-width: 0;">
      <h1 style="margin: 0 0 4px 0; font-size: 24px; font-weight: 700; color: #111827; letter-spacing: -0.02em;">{title}</h1>
      <h3 style="margin: 0; font-size: 14.5px; font-weight: 500; color: #4b5563;">{subtitle}</h3>
    </div>
    <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 18px;">
      {logos_html(logos)}
    </div>
  </header>

  <section style="margin-bottom: 16px; overflow-x: auto;">
    <table style="width: 100%; border-collapse: collapse; font-size: 13.5px; text-align: left;">
      <thead>
        <tr style="border-bottom: 2px solid #e5e7eb; color: #374151; font-weight: 600;">
          <th style="padding: 8px 12px; width: 24%;">Dataset / Layer</th>
          <th style="padding: 8px 12px; width: 11%;">Resolution</th>
          <th style="padding: 8px 12px; width: 36%;">Description &amp; Display</th>
          <th style="padding: 8px 12px; width: 18%;">Citation</th>
          <th style="padding: 8px 12px; width: 11%;">Status</th>
        </tr>
      </thead>
      <tbody>
{body_rows}
      </tbody>
    </table>
  </section>

  <p style="margin: 0 0 16px 0; padding: 9px 12px; background-color: #f8fafc; border-left: 3px solid #3b82f6; border-radius: 0 4px 4px 0; font-size: 13px; color: #475569;">
    {note}
  </p>
{refs_contributors}

  <footer style="display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px;
                 padding-top: 10px; border-top: 1px solid #e5e7eb; font-size: 12px; color: #6b7280;">
    <span><strong style="color: #374151;">Contact:</strong>
      <a href="mailto:{contact}" style="color: #2563eb; text-decoration: none;">{contact}</a></span>
    <span><strong style="color: #374151;">Last updated:</strong> {updated}</span>
  </footer>
</div>
'''


class FloatingLegend(MacroElement):
    '''Fixed html legend drawn inside the map container.'''
    def __init__(self, html):
        super().__init__()
        self._name = 'FloatingLegend'
        self._html = html
        self._template = Template("""
            {% macro script(this, kwargs) %}
            (function() {
                var wrapper = document.createElement('div');
                wrapper.innerHTML = {{ this._html|tojson }};
                var legend = wrapper.firstElementChild;
                {{ this._parent.get_name() }}.getContainer().appendChild(legend);
            })();
            {% endmacro %}
        """)


TREELINE_STYLE = {'color': '#111827', 'weight': 1.5, 'opacity': 0.9, 'dashArray': '6 4', 'fill': False}
TREELINE_LEGEND_ROW = (
    '<div style="display:flex;align-items:center;margin:6px 0 2px 0;">'
    '  <span style="width:18px;border-top:2px dashed #111827;display:inline-block;margin-right:6px;"></span>'
    '  <span style="font-size:11px;">CAVM treeline (context)</span>'
    '</div>'
)


def cavm_treeline_layer(name='CAVM treeline (context)', show=True):
    '''Dashed northern treeline from the Circumpolar Arctic Vegetation Map (Walker et al. 2005), for context.'''
    with open(CAVM_TREELINE_GEOJSON) as f:
        data = json.load(f)
    return folium.GeoJson(data, name=name, show=show, style_function=lambda x: TREELINE_STYLE,
                          control=True, smooth_factor=1)


def drop_layer(m, layer_name):
    '''Remove a layer (e.g. 'Boreal tiles', always added by MAP_REGISTERED_DPS_RESULTS) from a folium map.'''
    for k, child in list(m._children.items()):
        if getattr(child, 'layer_name', None) == layer_name:
            del m._children[k]


def set_extent(m, extent):
    '''Open the map at a named view in EXTENTS (e.g. 'na_boreal', 'alaska').'''
    m.location, m.options['zoom'] = EXTENTS[extent]


def point_url(tile_layer_dict, bidx=1):
    '''Titiler pixel-query url ({lon},{lat} placeholders) for a mosaic tile layer dict.'''
    return tile_layer_dict['layer'].tiles.split('/tiles/')[0] + '/point/{lon},{lat}' + f'?bidx={bidx}'


def save_review_map(m, out_html, header_html):
    '''Save the folium map and insert the page css + header at the top of <body>.'''
    m.save(out_html)
    with open(out_html, 'r') as f:
        html = f.read()
    html = html.replace('<body>', f'<body>\n{FULLWIDTH_CSS}\n{header_html}', 1)
    with open(out_html, 'w') as f:
        f.write(html)
    print(f"Saved {out_html} — push to GitHub to view at:")
    print(f"  https://pahbs.github.io/maap_tools/maps/{os.path.basename(out_html)}")
