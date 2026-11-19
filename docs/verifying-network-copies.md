# Verifying photo copies on network storage

When you send locally staged originals to network-attached storage (NAS),
Vireo verifies the copies before updating the catalog and removing originals.
For a mounted share, it can calculate destination checksums on the server
over Secure Shell (SSH). Photo contents are still read on both machines,
but only checksums and file information need to cross the network during
this check. Disk speed and connection setup still affect verification time.

This optimization is available on macOS and Linux. Windows keeps full byte
comparison: its file creation timestamps cannot reliably reveal an in-place
rewrite that preserves a file's size and modification time.

This happens automatically when exactly one saved target under **Settings →
Remote targets** covers the destination's mounted path. The target needs a
working SSH connection without a password prompt, the correct server-side
folder, and rsync on the server. Vireo also needs a GNU rsync executable and
an OpenSSH client locally. The selected copy destination stays authoritative;
the SSH connection is used only to verify that destination.

Before trusting the connection, Vireo writes a small temporary marker into
the destination and reads it through SSH to prove that both paths reach the
same folder. It checks mounted files for missing copies, size differences,
and unsafe links, then compares contents in batches of 128 files. Jobs shows
**Verifying copy on NAS**, with counts advancing after each verified batch.
Vireo rechecks the mapping and mounted files before accepting the result.
Destination identity, size, modification time, and change time must also
match their values before the checksum pass; a changed copy blocks the move.
Temporary verification markers are removed when the check ends.

Before sending a pending mounted transfer, open its details and select
**Check NAS verification**. This checks the saved route and local tools.
The job repeats this check before copying and proves the live destination
identity during verification.

If NAS verification is unavailable, Jobs shows the reason alongside the
local verification count. Vireo compares every byte, which may reread the
destination over the network. The reason remains in the verification step
after the job finishes.

A checksum mismatch stops the move and preserves the originals. Pausing
invalidates earlier verification, so resuming must complete a fresh check
before originals can be removed. Canceling also preserves the originals.
