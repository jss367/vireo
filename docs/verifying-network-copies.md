# Verifying photo copies on network storage

When you send locally staged originals to network-attached storage (NAS),
Vireo verifies the copies before updating the catalog and removing originals.
For a mounted share, it can calculate destination checksums on the server
over Secure Shell (SSH). Photo contents are still read on both machines,
but only checksums and file information need to cross the network during
this check. Disk speed and connection setup still affect verification time.

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

If SSH, rsync, or the path proof is unavailable, Vireo reads and compares
every byte through the mounted share. Jobs shows **Verifying copy locally
(NAS verification unavailable)** when an attempted remote check falls back;
the log contains the connection or verification error when available.
Destinations with no eligible saved SSH target keep their existing local
verification behavior.

A checksum mismatch stops the move and preserves the originals. Pausing
invalidates earlier verification, so resuming must complete a fresh check
before originals can be removed. Cancelling also preserves the originals.
