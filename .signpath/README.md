# SignPath release configuration

Windows public-beta tags require the SignPath Foundation open-source project
to be approved and the SignPath GitHub App to have repository access.

Configure these repository values:

- Secret `SIGNPATH_API_TOKEN`
- Variable `SIGNPATH_ORGANIZATION_ID`
- Variable `SIGNPATH_PROJECT_SLUG`
- Variable `SIGNPATH_POLICY_SLUG`
- Variable `SIGNPATH_ARTIFACT_CONFIGURATION_SLUG`
- Variable `WINDOWS_SIGNING_PUBLISHER` containing the expected certificate
  subject text

The artifact configuration must sign flat portable executable inputs. The
workflow submits two requests: first `Vireo.exe` and `vireo-server.exe`, then
the NSIS and MSI installers produced from those signed binaries. It regenerates
the adjacent Tauri updater `.sig` files after installer signing because
Authenticode changes installer bytes.
The signing policy must accept tagged builds and explicitly requested signed
release-candidate builds from GitHub-hosted runners on protected repository
branches. Restrict manual workflow dispatch to trusted maintainers in GitHub
and SignPath. The workflow rejects a signed Windows build when configuration is
absent, a signature is invalid, the publisher differs, or the timestamp is
missing.

Unsigned artifacts remain available only from manually dispatched internal
builds and must not be described as supported beta releases.

To certify a build before publishing, manually run **Build & Release** from the
protected `main` branch, leave `tag_name` blank, and enable
`sign_windows_candidate`. This uses the repository version, signs and
smoke-tests the Windows application and installers, and retains the signed
candidate artifact for 30 days. It does not create a tag or GitHub release and
does not update the website.
