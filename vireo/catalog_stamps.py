"""Version stamps that application code writes into the catalog.

The writers (the pipeline's eye-keypoint stage, the taxonomy import) stamp
these values on catalog rows; the data layer compares stored stamps against
them to tell current rows from outdated ones. They live here, with no
imports, so the repositories can read them without importing the pipeline
or the taxonomy loader.
"""

# Bump this string when the eye-keypoint routing or weights change in a
# way that invalidates previously persisted (eye_x, eye_y, eye_conf,
# eye_tenengrad) values. The pipeline page reads photos.eye_kp_fingerprint
# and treats != current as "Outdated".
EYE_KP_FINGERPRINT_VERSION = "v1"

# Bump when the taxonomy import changes how common names identify a taxon;
# ``db_meta.common_name_identity_version`` records the version the catalog's
# taxa index was built under.
COMMON_NAME_IDENTITY_VERSION = 1
