# SignPath release configuration

Signing Windows releases requires the SignPath Foundation open-source project
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
and SignPath. When signing is configured, the workflow rejects signatures that
are invalid, have a different publisher, or lack a timestamp.

If any signing configuration is missing, the workflow falls back to unsigned
artifacts, including for tagged releases and requested signed candidates.
Unsigned installers can show an "unknown publisher" warning on Windows.

To certify a build before publishing, manually run **Build & Release** from the
protected `main` branch, leave `tag_name` blank, and enable
`sign_windows_candidate` after configuring all signing values above. This uses
the repository version, signs and smoke-tests the Windows application and installers, and retains the signed
candidate artifact for 30 days. It does not create a tag or GitHub release and
does not update the website.
