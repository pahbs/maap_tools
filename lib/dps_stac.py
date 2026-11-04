"""
Map layers from collections indexed in the MAAP DPS STAC (https://dps-stac-browser.maap-project.org).

A DPS STAC collection needs no mosaic json: give tile_layer() the collection (its id, or its url from the STAC browser)
and it returns the same dict as maplib_folium.make_tiles_layer_dict(), ready for a map's list of tile layers:

    import dps_stac

    URL = 'https://dps-stac-browser.maap-project.org/catalogs/user-omshinde23/collections/omshinde23__publish_prithvi_agb__main'
    dps_stac.describe(URL)                                  # what the collection has: renders, bands, extent
    AGB = dps_stac.tile_layer(URL, render='agbd', name='Prithvi AGBD 2020 [Mg/ha]')
    query = dps_stac.query_layer(AGB, section='Prithvi AGBD 2020', name='mean', uncertainty_band=2)   # for add_pixel_query

If the collection has no `renders`, say how to draw it:

    dps_stac.tile_layer(URL, name='...', asset='cog', band=1, rescale=(0, 30), colormap_name='inferno')

Display settings come from the collection, and these are handled for you (see tile_layer):
- the band goes inside `assets` (`assets=cog|bidx=1`); on this tiler a separate `bidx` is ignored
- bands stored with a scale factor are unscaled, so rescale limits and pixel queries are in real units
- items whose footprint wraps the globe at the 180 deg meridian would otherwise blank tiles at their latitudes
"""
import functools
import json
import re
import urllib.parse
import urllib.request

from folium import TileLayer

DPS_STAC_API = 'https://dps-stac.maap-project.org'
DPS_STAC_BROWSER = 'https://dps-stac-browser.maap-project.org'
DPS_STAC_TITILER = 'https://titiler-dps-stac.maap-project.org'

# Below this, tiles of a 30 m circumboreal collection (~5,000 items) take over 20 s or fail (API gateway limit: 30 s)
DEFAULT_MIN_ZOOM = 7


def collection_id(collection):
    '''Collection id from an id, or from any STAC browser or API url of the collection.'''
    match = re.search(r'/collections/([^/?#]+)', collection)
    return urllib.parse.unquote(match.group(1)) if match else collection


def _quoted(collection):
    return urllib.parse.quote(collection_id(collection), safe='')


def _get_json(url):
    with urllib.request.urlopen(url, timeout=120) as r:
        return json.load(r)


def _item_bboxes(collection):
    '''Bounding boxes of every item in the collection.'''
    bboxes = []
    url = f'{DPS_STAC_API}/collections/{_quoted(collection)}/items?limit=1000&fields=id,bbox'
    while url:
        page = _get_json(url)
        bboxes += [item['bbox'] for item in page['features']]
        url = next((link['href'] for link in page['links'] if link['rel'] == 'next'), None)
    return bboxes


def _is_data_asset(asset):
    return 'data' in asset.get('roles', []) or asset.get('type', '').startswith('image/tiff')


@functools.lru_cache(maxsize=None)
def collection_info(collection):
    '''
    What tile_layer() needs to know about a collection (cached for the session; restart the kernel to pick up changes).
    Returns a dict: id, title, bounds [w, s, e, n], renders, assets {name: [band dicts]}, n_items, n_wrapping_items.
    '''
    cid = collection_id(collection)
    coll = _get_json(f'{DPS_STAC_API}/collections/{_quoted(cid)}')
    features = _get_json(f'{DPS_STAC_API}/collections/{_quoted(cid)}/items?limit=1')['features']
    if not features:
        raise ValueError(f'Collection {cid} has no items')
    bboxes = _item_bboxes(cid)
    return {
        'id': cid,
        'title': coll.get('title', cid),
        'bounds': coll['extent']['spatial']['bbox'][0],
        'renders': coll.get('renders', {}),
        'assets': {name: asset.get('bands', asset.get('raster:bands', []))
                   for name, asset in features[0]['assets'].items() if _is_data_asset(asset)},
        'n_items': len(bboxes),
        # bbox west > east: the item crosses 180 deg. Its polygon then runs the long way round the globe, unless it is
        # split into a MultiPolygon, and the tiler takes it for full coverage of every tile at those latitudes
        'n_wrapping_items': sum(bbox[0] > bbox[2] for bbox in bboxes),
    }


def _is_scaled(band):
    return band.get('scale', 1) not in (None, 1) or band.get('offset', 0) not in (None, 0)


def describe(collection):
    '''Print what a collection offers, to help choose the arguments of tile_layer().'''
    info = collection_info(collection)
    w, s, e, n = info['bounds']
    print(f"{info['title']}\n  id: {info['id']}\n  items: {info['n_items']}, extent: {w:.1f}, {s:.1f}, {e:.1f}, {n:.1f}")
    for name, bands in info['assets'].items():
        print(f'  asset {name!r}:')
        for i, band in enumerate(bands, 1):
            scale = f", scale {band['scale']}" if _is_scaled(band) else ''
            print(f"    band {i}: {band.get('name', '')} [{band.get('unit', '')}]{scale}")
    if info['renders']:
        print('  renders:')
        for key, render in info['renders'].items():
            print(f"    {key!r}: {render.get('title', '')}, bands {render.get('bidx')}, rescale {render.get('rescale')}, "
                  f"colormap {render.get('colormap_name')}")
    else:
        print('  renders: none (give asset, band, rescale and colormap_name to tile_layer)')
    if info['n_wrapping_items']:
        print(f"  note: {info['n_wrapping_items']} items wrap the globe at 180 deg (tile_layer works around this)")


def user_collections(user):
    '''Ids and titles of a MAAP user's collections in the DPS STAC.'''
    collections = _get_json(f'{DPS_STAC_API}/catalogs/user-{user}/collections')['collections']
    return {c['id']: c.get('title', '') for c in collections}


def _resolve_render(info, render):
    '''Render dict for `render`, or {} if the collection has none.'''
    renders = info['renders']
    if render is not None:
        if render not in renders:
            raise ValueError(f"{info['id']} has no render {render!r}; it has: {', '.join(renders) or 'none'}")
        return renders[render]
    if len(renders) == 1:
        return next(iter(renders.values()))
    if renders:
        raise ValueError(f"Choose a render for {info['id']}: {', '.join(renders)}")
    return {}


def tile_layer(collection, render=None, name=None, asset=None, band=None, rescale=None, colormap_name=None, cmap=None,
               unscale=None, min_zoom=DEFAULT_MIN_ZOOM, show=False, show_cbar=False, caption=None, **tiler_params):
    '''
    Tile layer dict for a DPS STAC collection, like maplib_folium.make_tiles_layer_dict().

    collection    : collection id, or its url from the STAC browser
    render        : key of the collection's `renders`; asset, band, rescale and colormap_name below override it
    name          : layer name in the map (default: render title or collection title)
    asset, band   : asset name and band number (from 1)
    rescale       : (min, max) in real units
    colormap_name : titiler colormap
    cmap          : colormap for the colorbar, if it differs from colormap_name (e.g. with a custom `colormap`)
    unscale       : apply the band scale and offset; default True if the band has one
    min_zoom      : tiles are not requested below this zoom
    show          : layer on at opening
    show_cbar     : add a colorbar
    caption       : colorbar caption (default: name)
    tiler_params  : anything else for the tiler, e.g. colormap=json.dumps(...)
    '''
    info = collection_info(collection)
    spec = _resolve_render(info, render)

    asset = asset or next(iter(spec.get('assets', [])), None)
    if asset is None:
        if len(info['assets']) != 1:
            raise ValueError(f"Choose an asset for {info['id']}: {', '.join(info['assets'])}")
        asset = next(iter(info['assets']))
    band = band or next(iter(spec.get('bidx', [])), 1)
    rescale = rescale or next(iter(spec.get('rescale', [])), None)
    colormap_name = colormap_name or spec.get('colormap_name')
    if rescale is None or (colormap_name is None and 'colormap' not in tiler_params):
        raise ValueError(f"{info['id']} has no render to use: give rescale and colormap_name")

    bands = info['assets'].get(asset, [])
    band_info = bands[band - 1] if band <= len(bands) else {}
    if unscale is None:
        unscale = _is_scaled(band_info)

    params = {'assets': f'{asset}|bidx={band}', 'rescale': ','.join(str(v) for v in rescale)}
    if colormap_name:
        params['colormap_name'] = colormap_name
    if unscale:
        params['unscale'] = 'true'
    if info['n_wrapping_items']:
        params.update({'exitwhenfull': 'false', 'skipcovered': 'false'})
    params.update(tiler_params)

    name = name or spec.get('title') or info['title']
    west, south, east, north = info['bounds']
    if info['n_wrapping_items']:
        west, east = -180, 180   # the collection extent can stop short of 180 deg, which would clip those items
    layer = TileLayer(
        tiles=f"{DPS_STAC_TITILER}/collections/{_quoted(info['id'])}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?"
              + urllib.parse.urlencode(params),
        name=name, attr='NASA MAAP', overlay=True, opacity=1, show=show,
        min_zoom=min_zoom, bounds=[[south, west], [north, east]],
    )
    return {
        'layer': layer,
        'cmap': cmap or colormap_name,
        'min_val': rescale[0],
        'max_val': rescale[1],
        'caption': caption or name,
        'show_cbar': show_cbar,
        # for point_url() and query_layer()
        'collection': info['id'],
        'asset': asset,
        'band': band,
        'unscale': unscale,
        'units': band_info.get('unit', ''),
    }


def point_url(tile_layer_dict, bands=None):
    '''Titiler pixel-query url ({lon},{lat} placeholders) for a tile_layer() dict; bands: one number or a list.'''
    bands = bands or tile_layer_dict['band']
    bidx = ','.join(str(b) for b in bands) if isinstance(bands, (list, tuple)) else bands
    asset = urllib.parse.quote(f"{tile_layer_dict['asset']}|bidx={bidx}", safe='=,')
    url = (f"{DPS_STAC_TITILER}/collections/{urllib.parse.quote(tile_layer_dict['collection'], safe='')}"
           f"/point/{{lon}},{{lat}}?assets={asset}")
    return url + ('&unscale=true' if tile_layer_dict['unscale'] else '')


def query_layer(tile_layer_dict, section=None, name=None, units=None, uncertainty_band=None):
    '''
    One entry for maplib_folium.add_pixel_query(). uncertainty_band: band to show as ± in the popup.
    '''
    bands = [tile_layer_dict['band'], uncertainty_band] if uncertainty_band else tile_layer_dict['band']
    query = {
        'section': section or tile_layer_dict['caption'],
        'name': name or 'value',
        'point_url': point_url(tile_layer_dict, bands),
        'units': tile_layer_dict['units'] if units is None else units,
    }
    if uncertainty_band:
        query['uncertainty_bidx'] = uncertainty_band
    return query
