"""iNaturalist API client for Vireo."""

import requests

INAT_API = "https://api.inaturalist.org/v1"
INAT_RAILS = "https://www.inaturalist.org"


class InatAuthError(Exception):
    """Raised when iNat token is invalid or expired."""
    pass


class InatApiError(Exception):
    """Raised when iNat API returns a non-auth error."""
    pass


def _headers(token):
    return {"Authorization": f"Bearer {token}"}


def validate_token(token):
    """Validate iNat API token. Returns user dict on success, None on failure."""
    resp = requests.get(f"{INAT_RAILS}/users/edit.json", headers=_headers(token))
    if resp.status_code == 200:
        return resp.json()
    return None


def create_observation(token, taxon_name=None, observed_on=None,
                       latitude=None, longitude=None,
                       description=None, geoprivacy="open"):
    """Create an iNaturalist observation. Returns observation dict."""
    obs = {}
    if taxon_name:
        obs["species_guess"] = taxon_name
    if observed_on:
        obs["observed_on_string"] = observed_on
    if latitude is not None and longitude is not None:
        obs["latitude"] = latitude
        obs["longitude"] = longitude
    if description:
        obs["description"] = description
    if geoprivacy and geoprivacy != "open":
        obs["geoprivacy"] = geoprivacy

    resp = requests.post(
        f"{INAT_RAILS}/observations.json",
        json={"observation": obs},
        headers=_headers(token),
    )
    if resp.status_code == 401:
        raise InatAuthError("iNaturalist token is invalid or expired. Please refresh it in Settings.")
    if resp.status_code not in (200, 201):
        raise InatApiError(f"iNaturalist API error ({resp.status_code}): {resp.text[:200]}")
    data = resp.json()
    # Rails API returns a list with one observation
    if isinstance(data, list):
        return data[0]
    return data


def upload_photo(token, observation_id, photo_path):
    """Attach a photo file to an existing observation."""
    with open(photo_path, "rb") as f:
        resp = requests.post(
            f"{INAT_RAILS}/observation_photos.json",
            files={"file": f},
            data={"observation_photo[observation_id]": observation_id},
            headers=_headers(token),
        )
    if resp.status_code == 401:
        raise InatAuthError("iNaturalist token is invalid or expired.")
    if resp.status_code not in (200, 201):
        raise InatApiError(f"Photo upload failed ({resp.status_code}): {resp.text[:200]}")
    return resp.json()


def submit_observation(token, photo_path, taxon_name=None, observed_on=None,
                       latitude=None, longitude=None,
                       description=None, geoprivacy="open"):
    """Create observation + upload photo. Returns (observation_id, observation_url)."""
    obs = create_observation(
        token, taxon_name=taxon_name, observed_on=observed_on,
        latitude=latitude, longitude=longitude,
        description=description, geoprivacy=geoprivacy,
    )
    obs_id = obs["id"]
    obs_url = obs.get("uri", f"{INAT_RAILS}/observations/{obs_id}")
    upload_photo(token, obs_id, photo_path)
    return obs_id, obs_url
