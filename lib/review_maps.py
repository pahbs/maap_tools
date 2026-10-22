"""
Shared page template for the Review maps published from maap_tools/maps (https://pahbs.github.io/maap_tools/maps/).

Every Review map notebook builds its folium map, then calls save_review_map() with a header from
build_header_html(), so all maps share one look: title + logos, a dataset table with one status
badge per row, a usage note, references, and a contact/last-updated footer.

Logo images live in maps/logos/ and are referenced relative to the saved html (maps/*.html).

save_review_map() also writes each map's overlay tile layers (XYZ urls) to maps/tile_layers.csv, published at
https://pahbs.github.io/maap_tools/maps/tile_layers.csv for use outside the Review maps (e.g. mobile mapping apps).
Call use_mosaic_registry(maplib_folium) before building tile layers so those urls stay the same across reruns.
"""
import csv
import hashlib
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import folium
from folium import MacroElement
from jinja2 import Template

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MAPS_DIR = os.path.join(REPO_DIR, 'maps')
PAGES_URL = 'https://pahbs.github.io/maap_tools/maps'
CAVM_TREELINE_GEOJSON = os.path.join(REPO_DIR, 'data', 'cavm_treeline.geojson')

# Overlay tile layers of every saved Review map, one row per layer (see update_tile_layers_csv)
TILE_LAYERS_CSV = os.path.join(MAPS_DIR, 'tile_layers.csv')
TILE_LAYERS_FIELDS = ['map', 'layer_name', 'xyz_url', 'min_zoom', 'max_zoom', 'west', 'south', 'east', 'north',
                      'source', 'bidx', 'rescale', 'colormap', 'tiler', 'mosaic_id', 'map_url', 'updated_utc']
# TiTiler mosaic registrations, keyed by mosaic json (see use_mosaic_registry)
MOSAIC_REGISTRY_JSON = os.path.join(REPO_DIR, 'data', 'mosaic_registry.json')

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


def _load_mosaic_registry():
    if not os.path.exists(MOSAIC_REGISTRY_JSON):
        return {}
    with open(MOSAIC_REGISTRY_JSON) as f:
        return json.load(f)


def _get_tilejson(mosaic_links):
    '''TileJSON of a registered mosaic, or None if TiTiler no longer serves that registration.'''
    href = next((l['href'] for l in mosaic_links.get('links', []) if l.get('rel') == 'tilejson'), None)
    if href is None:
        return None
    try:
        with urllib.request.urlopen(href, timeout=60) as r:
            return json.load(r)
    except Exception:
        return None


def use_mosaic_registry(maplib_folium):
    '''
    Make maplib_folium reuse TiTiler mosaic registrations across reruns, so tile urls (in the published maps and in
    tile_layers.csv) stay the same. A mosaic json is registered again only when its contents change or TiTiler no
    longer serves the old registration. Registrations are kept in data/mosaic_registry.json (commit it with the maps).
    Call once, after importing maplib_folium and before building tile layers.
    '''
    register = getattr(maplib_folium.register_mosaic_json_titiler, '__wrapped__',
                       maplib_folium.register_mosaic_json_titiler)

    def register_or_reuse(mosiac_json_fn, titiler_endpoint, print_info=False):
        if isinstance(mosiac_json_fn, list):
            return register(mosiac_json_fn, titiler_endpoint, print_info=print_info)
        with maplib_folium.S3Backend(mosiac_json_fn) as mosaic:
            mosaic_def = mosaic.mosaic_def.model_dump(exclude_none=True)
        digest = hashlib.sha1(json.dumps(mosaic_def, sort_keys=True, default=str).encode()).hexdigest()

        registry = _load_mosaic_registry()
        entry = registry.get(mosiac_json_fn)
        if entry and entry['digest'] == digest and entry['titiler_endpoint'] == titiler_endpoint:
            if _get_tilejson(entry['mosaic_links']) is not None:
                print(f"Reusing mosaic {entry['mosaic_id']} (registered {entry['registered_utc']}): {mosiac_json_fn}")
                return entry['mosaic_links']

        mosaic_links = register(mosiac_json_fn, titiler_endpoint, print_info=print_info)
        tilejson = _get_tilejson(mosaic_links)
        if tilejson is None:
            return mosaic_links   # registration failed: leave the registry alone, maplib_folium reports the error
        registry[mosiac_json_fn] = {
            'mosaic_id': re.search(r'/mosaics/([^/]+)/', tilejson['tiles'][0]).group(1),
            'mosaic_links': mosaic_links,
            'digest': digest,
            'titiler_endpoint': titiler_endpoint,
            'minzoom': tilejson.get('minzoom'),
            'maxzoom': tilejson.get('maxzoom'),
            'bounds': tilejson.get('bounds'),
            'registered_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
        }
        with open(MOSAIC_REGISTRY_JSON, 'w') as f:
            json.dump(registry, f, indent=1, sort_keys=True)
        return mosaic_links

    register_or_reuse.__wrapped__ = register
    maplib_folium.register_mosaic_json_titiler = register_or_reuse


def _overlay_tile_layers(element):
    '''Overlay (non-basemap) folium TileLayers in a map, in the order they were added.'''
    for child in element._children.values():
        if isinstance(child, folium.TileLayer) and child.overlay:
            yield child
        yield from _overlay_tile_layers(child)


def tile_layer_rows(m, map_name):
    '''One tile_layers.csv row per overlay tile layer in folium map m.'''
    by_mosaic_id = {v['mosaic_id']: (k, v) for k, v in _load_mosaic_registry().items()}
    updated = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    rows = []
    for layer in _overlay_tile_layers(m):
        url = urllib.parse.urlsplit(layer.tiles)
        params = dict(urllib.parse.parse_qsl(url.query))
        mosaic_id = re.search(r'/mosaics/([^/]+)/tiles/', url.path)
        mosaic_id = mosaic_id.group(1) if mosaic_id else ''
        source, entry = by_mosaic_id.get(mosaic_id, ('', {}))
        collection = re.search(r'/collections/([^/]+)/', url.path)
        if collection:
            source = f'STAC collection {collection.group(1)}'
        # folium option keys are camelCase or snake_case depending on the folium version
        opts = layer.options
        min_zoom = opts.get('minZoom', opts.get('min_zoom')) or entry.get('minzoom') or 0
        max_zoom = entry.get('maxzoom') or opts.get('maxZoom', opts.get('max_zoom'))
        bounds = entry.get('bounds')
        if not bounds and opts.get('bounds'):
            (s, w), (n, e) = opts['bounds']
            bounds = [w, s, e, n]
        rows.append({
            'map': map_name,
            'layer_name': layer.layer_name,
            'xyz_url': layer.tiles,
            'min_zoom': min_zoom,
            'max_zoom': max_zoom,
            **dict(zip(['west', 'south', 'east', 'north'], [round(b, 4) for b in bounds] if bounds else [''] * 4)),
            'source': source,
            'bidx': params.get('bidx') or params.get('assets', '').partition('bidx=')[2],
            'rescale': params.get('rescale', ''),
            'colormap': params.get('colormap_name') or ('custom' if 'colormap' in params else ''),
            'tiler': url.netloc,
            'mosaic_id': mosaic_id,
            'map_url': f'{PAGES_URL}/{map_name}',
            'updated_utc': updated,
        })
    return rows


def update_tile_layers_csv(m, out_html):
    '''Replace this map's rows in maps/tile_layers.csv with its current overlay tile layers; other maps' rows are kept.'''
    map_name = os.path.basename(out_html)
    rows = []
    if os.path.exists(TILE_LAYERS_CSV):
        with open(TILE_LAYERS_CSV, newline='') as f:
            rows = [r for r in csv.DictReader(f) if r['map'] != map_name]
    new_rows = tile_layer_rows(m, map_name)
    rows = sorted(rows + new_rows, key=lambda r: r['map'])   # stable: layer order kept within each map
    with open(TILE_LAYERS_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=TILE_LAYERS_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(new_rows)} tile layers for {map_name} to {TILE_LAYERS_CSV}")
    print(f"  {PAGES_URL}/{os.path.basename(TILE_LAYERS_CSV)}")


def save_review_map(m, out_html, header_html):
    '''Save the folium map, insert the page css + header at the top of <body>, and update maps/tile_layers.csv.'''
    m.save(out_html)
    with open(out_html, 'r') as f:
        html = f.read()
    html = html.replace('<body>', f'<body>\n{FULLWIDTH_CSS}\n{header_html}', 1)
    with open(out_html, 'w') as f:
        f.write(html)
    print(f"Saved {out_html} — push to GitHub to view at:")
    print(f"  {PAGES_URL}/{os.path.basename(out_html)}")
    update_tile_layers_csv(m, out_html)
